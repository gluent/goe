import pytest

from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_metadata_functions import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    OFFLOAD_TYPE_FULL,
    OFFLOAD_TYPE_INCREMENTAL,
)
from goe.offload.offload_source_data import (
    INCREMENTAL_OFFLOAD_DEFAULT_PARTITION_EXCEPTION_TEXT,
    IPA_OFFLOAD_DEFAULT_PARTITION_EXCEPTION_TEXT,
    NO_MATCHING_PARTITION_EXCEPTION_TEXT,
)
from goe.offload.oracle import oracle_column
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    backend_column_exists,
    backend_table_count,
    check_metadata,
    synthetic_part_col_name,
)
from tests.integration.scenarios import scenario_constants
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_table,
    partition_columns_if_supported,
)
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.testlib.test_framework import test_constants
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)


LPA_NUM_PART_KEY_TABLE = "STORY_LPA_NUM_KEY"
LPA_VC2_PART_KEY_TABLE = "STORY_LPA_VC2_KEY"
LPA_CHR_PART_KEY_TABLE = "STORY_LPA_CHR_KEY"
LPA_DT_PART_KEY_TABLE = "STORY_LPA_DT_KEY"
LPA_TS_PART_KEY_TABLE = "STORY_LPA_TS_KEY"
LPA_NUM_PART_FUNC_TABLE = "STORY_LPA_NUM_PF"
LPA_PART1_KEY1, LPA_PART1_KEY2 = "123", "456"
LPA_PART2_KEY1 = "789"
LPA_DT_PART1_KEY1, LPA_DT_PART1_KEY2 = "2012-01-01", "2012-02-01"
LPA_DT_PART2_KEY1 = "2012-03-01"

LPA_FACT_TABLE = "STORY_LPA_FACT"
LPA_FULL_FACT_TABLE = "STORY_LPA_FULL_DEF_FACT"

LPA_UNICODE_FACT_TABLE = "STORY_LPA_UNI_FACT"
LPA_UNICODE_PART1_KEY1, LPA_UNICODE_PART1_KEY2 = "\u00f6", "o"
LPA_UNICODE_PART2_KEY1 = "\u30ad"


@pytest.fixture
def config():
    return cached_current_options()


@pytest.fixture
def schema():
    return cached_default_test_user()


@pytest.fixture
def data_db(schema, config):
    data_db = data_db_name(schema, config)
    data_db = convert_backend_identifier_case(config, data_db)
    return data_db


def gen_list_multi_part_value_create_ddl(
    schema, table_name, part_key_type, part_key_chars
):
    """Create a LIST partitioned table with multiple values per partition.
    Can create using NUMBER, DATE, TIMESTAMP, VARCHAR2 and NVARCHAR2.
    Purpose is to prove that complex values options.equal_to_values works.
    """
    part_key_type_mappings = {
        "NUMBER": "NUMBER(3)",
        "VARCHAR2": "VARCHAR2(3)",
        "CHAR": "CHAR(3)",
        "NVARCHAR2": "NVARCHAR2(3)",
        "NCHAR": "NCHAR(3)",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
    }
    assert part_key_type in part_key_type_mappings, (
        "Unsupported part_key_type: %s" % part_key_type
    )
    assert part_key_chars and type(part_key_chars) is list and len(part_key_chars) == 3
    part_key_type_defaults = {
        "NUMBER": "-1",
        "VARCHAR2": "X",
        "CHAR": "X",
        "NVARCHAR2": "X",
        "NCHAR": "X",
        "DATE": "1970-01-01",
        "TIMESTAMP": "1970-01-01",
    }
    qt = "" if part_key_type == "NUMBER" else "'"
    if part_key_type == "DATE":
        literal_fn, fn_mask = "TO_DATE(", ",'YYYY-MM-DD')"
    elif part_key_type == "TIMESTAMP":
        literal_fn, fn_mask = "TO_TIMESTAMP(", ",'YYYY-MM-DD')"
    else:
        literal_fn, fn_mask = "", ""
    params = {
        "schema": schema,
        "table": table_name,
        "part_key_type": part_key_type_mappings[part_key_type],
        "ch1": part_key_chars[0],
        "ch2": part_key_chars[1],
        "ch3": part_key_chars[2],
        "def_ch": part_key_type_defaults[part_key_type],
        "qt": qt,
        "literal_fn": literal_fn,
        "fn_mask": fn_mask,
    }
    return [
        """DROP TABLE %(schema)s.%(table)s""" % params,
        """CREATE TABLE %(schema)s.%(table)s (id NUMBER(8),data NVARCHAR2(30),cat %(part_key_type)s)
               STORAGE (INITIAL 64K NEXT 64K)
               PARTITION BY LIST (cat)
               ( PARTITION p_1 VALUES (%(literal_fn)s%(qt)s%(ch1)s%(qt)s%(fn_mask)s,%(literal_fn)s%(qt)s%(ch2)s%(qt)s%(fn_mask)s) STORAGE (INITIAL 16k)
               , PARTITION p_2 VALUES (%(literal_fn)s%(qt)s%(ch3)s%(qt)s%(fn_mask)s) STORAGE (INITIAL 16k)
               , PARTITION p_def VALUES (DEFAULT) STORAGE (INITIAL 16k))"""
        % params,
        """INSERT INTO %(schema)s.%(table)s (id, data, cat)
               SELECT ROWNUM,DBMS_RANDOM.STRING('u', 15)
               ,      CASE MOD(ROWNUM,3)
                      WHEN 0 THEN %(literal_fn)s%(qt)s%(ch1)s%(qt)s%(fn_mask)s
                      WHEN 1 THEN %(literal_fn)s%(qt)s%(ch2)s%(qt)s%(fn_mask)s
                      WHEN 2 THEN %(literal_fn)s%(qt)s%(ch3)s%(qt)s%(fn_mask)s
                      ELSE %(literal_fn)s%(qt)s%(def_ch)s%(qt)s%(fn_mask)s
                      END AS cat
               FROM   dual
               CONNECT BY ROWNUM <= 1000"""
        % params,
    ]


def offload_lpa_fact_assertion(
    schema,
    data_db,
    table_name,
    config,
    backend_api,
    messages,
    repo_client,
    hwm_literals,
    offload_pattern=scenario_constants.OFFLOAD_PATTERN_90_10,
    incremental_key=None,
    incremental_range="PARTITION",
    incremental_predicate_type=None,
    incremental_predicate_value=None,
    backend_table_count_check=None,
    partition_functions=None,
    synthetic_partition_column_name=None,
):
    assert schema
    assert data_db
    assert table_name
    assert hwm_literals is None or type(hwm_literals) in (list, tuple)
    messages.log("offload_lpa_fact_assertions: %s" % offload_pattern, detail=VVERBOSE)
    backend_table = convert_backend_identifier_case(config, table_name)

    if offload_pattern == scenario_constants.OFFLOAD_PATTERN_90_10:
        offload_type = OFFLOAD_TYPE_INCREMENTAL
        assert hwm_literals, "hwm_literals required for %s" % offload_pattern
        hwm_in_hybrid_view = True
    elif offload_pattern == scenario_constants.OFFLOAD_PATTERN_100_0:
        offload_type = OFFLOAD_TYPE_FULL
        incremental_key = None
        incremental_predicate_type = None
        hwm_in_hybrid_view = False
    elif offload_pattern == scenario_constants.OFFLOAD_PATTERN_100_10:
        assert hwm_literals, "hwm_literals required for %s" % offload_pattern
        offload_type = OFFLOAD_TYPE_FULL
        hwm_in_hybrid_view = True

    messages.log("offload_type: %s" % offload_type, detail=VVERBOSE)
    messages.log("hwm_in_hybrid_view: %s" % hwm_in_hybrid_view, detail=VVERBOSE)

    if hwm_in_hybrid_view:

        def check_fn(mt):
            if all(_ in mt.incremental_high_value for _ in hwm_literals):
                return True
            else:
                messages.log(
                    "Metadata check_fn False for: %s"
                    % str(
                        [_ for _ in hwm_literals if _ not in mt.incremental_high_value]
                    )
                )
                return False

    else:
        check_fn = lambda mt: bool(
            not mt.incremental_key and not mt.incremental_high_value
        )

    if not check_metadata(
        schema,
        table_name,
        messages,
        repo_client,
        hadoop_owner=data_db,
        hadoop_table=backend_table,
        incremental_key=incremental_key,
        offload_type=offload_type,
        incremental_range=incremental_range,
        incremental_predicate_type=incremental_predicate_type,
        incremental_predicate_value=incremental_predicate_value,
        offload_partition_functions=partition_functions,
        check_fn=check_fn,
    ):
        return False

    if backend_table_count_check is not None:
        if (
            backend_table_count(config, backend_api, messages, data_db, backend_table)
            != backend_table_count_check
        ):
            messages.log(f"Backend count != {backend_table_count_check}")
            return False

    if (
        synthetic_partition_column_name
        and backend_api.synthetic_partitioning_supported()
    ):
        if not backend_column_exists(
            config,
            backend_api,
            messages,
            data_db,
            backend_table,
            synthetic_partition_column_name,
        ):
            messages.log(
                f"Backend column {synthetic_partition_column_name} does not exist"
            )
            return False

    return True


def test_offload_lpa_num(config, schema, data_db):
    id = "test_offload_lpa_num"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_list_multi_part_value_create_ddl(
            schema,
            LPA_NUM_PART_KEY_TABLE,
            oracle_column.ORACLE_TYPE_NUMBER,
            [LPA_PART1_KEY1, LPA_PART1_KEY2, LPA_PART2_KEY1],
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_NUM_PART_KEY_TABLE
            ),
        ],
    )

    # Offload a partition with multiple NUMBER partition keys.
    options = {
        "owner_table": schema + "." + LPA_NUM_PART_KEY_TABLE,
        "equal_to_values": ["%s,%s" % (LPA_PART1_KEY1, LPA_PART1_KEY2)],
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 1000,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_NUM_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_PART1_KEY1, LPA_PART1_KEY2],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload next partition.
    options = {
        "owner_table": schema + "." + LPA_NUM_PART_KEY_TABLE,
        "equal_to_values": [LPA_PART2_KEY1],
        "verify_row_count": "aggregate",
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_NUM_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_PART1_KEY1, LPA_PART1_KEY2, LPA_PART2_KEY1],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_lpa_vc2(config, schema, data_db):
    id = "test_offload_lpa_vc2"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_list_multi_part_value_create_ddl(
            schema,
            LPA_VC2_PART_KEY_TABLE,
            oracle_column.ORACLE_TYPE_VARCHAR2,
            [LPA_PART1_KEY1, LPA_PART1_KEY2, LPA_PART2_KEY1],
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_VC2_PART_KEY_TABLE
            ),
        ],
    )

    # Offload a partition with multiple VC2 partition keys.
    options = {
        "owner_table": schema + "." + LPA_VC2_PART_KEY_TABLE,
        "equal_to_values": ["%s,%s" % (LPA_PART1_KEY1, LPA_PART1_KEY2)],
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_VC2_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_PART1_KEY1, LPA_PART1_KEY2],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload next LPA partition.
    options = {
        "owner_table": schema + "." + LPA_VC2_PART_KEY_TABLE,
        "equal_to_values": [LPA_PART2_KEY1],
        "verify_row_count": "aggregate",
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_VC2_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_PART1_KEY1, LPA_PART1_KEY2, LPA_PART2_KEY1],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_lpa_char(config, schema, data_db):
    id = "test_offload_lpa_char"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_list_multi_part_value_create_ddl(
            schema,
            LPA_CHR_PART_KEY_TABLE,
            oracle_column.ORACLE_TYPE_CHAR,
            [LPA_PART1_KEY1, LPA_PART1_KEY2, LPA_PART2_KEY1],
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_CHR_PART_KEY_TABLE
            ),
        ],
    )

    # Offload a partition with multiple CHAR partition keys.
    options = {
        "owner_table": schema + "." + LPA_CHR_PART_KEY_TABLE,
        "equal_to_values": ["%s,%s" % (LPA_PART1_KEY1, LPA_PART1_KEY2)],
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_CHR_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_PART1_KEY1, LPA_PART1_KEY2],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload next LPA partition.
    options = {
        "owner_table": schema + "." + LPA_CHR_PART_KEY_TABLE,
        "equal_to_values": [LPA_PART2_KEY1],
        "verify_row_count": "aggregate",
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_CHR_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_PART1_KEY1, LPA_PART1_KEY2, LPA_PART2_KEY1],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_lpa_date(config, schema, data_db):
    id = "test_offload_lpa_date"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_list_multi_part_value_create_ddl(
            schema,
            LPA_DT_PART_KEY_TABLE,
            oracle_column.ORACLE_TYPE_DATE,
            [LPA_DT_PART1_KEY1, LPA_DT_PART1_KEY2, LPA_DT_PART2_KEY1],
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_DT_PART_KEY_TABLE
            ),
        ],
    )

    # Offload a partition with multiple DATE partition keys.
    options = {
        "owner_table": schema + "." + LPA_DT_PART_KEY_TABLE,
        "equal_to_values": ["%s,%s" % (LPA_DT_PART1_KEY1, LPA_DT_PART1_KEY2)],
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_DT_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_DT_PART1_KEY1, LPA_DT_PART1_KEY2],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload next LPA partition.
    options = {
        "owner_table": schema + "." + LPA_DT_PART_KEY_TABLE,
        "equal_to_values": [LPA_DT_PART2_KEY1],
        "verify_row_count": "aggregate",
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_DT_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_DT_PART1_KEY1, LPA_DT_PART1_KEY2, LPA_DT_PART2_KEY1],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_lpa_ts(config, schema, data_db):
    id = "test_offload_lpa_ts"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_list_multi_part_value_create_ddl(
            schema,
            LPA_TS_PART_KEY_TABLE,
            oracle_column.ORACLE_TYPE_TIMESTAMP,
            [LPA_DT_PART1_KEY1, LPA_DT_PART1_KEY2, LPA_DT_PART2_KEY1],
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_TS_PART_KEY_TABLE
            ),
        ],
    )

    # Offload a partition with multiple CHAR partition keys.
    options = {
        "owner_table": schema + "." + LPA_TS_PART_KEY_TABLE,
        "equal_to_values": ["%s,%s" % (LPA_DT_PART1_KEY1, LPA_DT_PART1_KEY2)],
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_TS_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_DT_PART1_KEY1, LPA_DT_PART1_KEY2],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload next LPA partition.
    options = {
        "owner_table": schema + "." + LPA_TS_PART_KEY_TABLE,
        "equal_to_values": [LPA_DT_PART2_KEY1],
        "verify_row_count": "aggregate",
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_TS_PART_KEY_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_DT_PART1_KEY1, LPA_DT_PART1_KEY2, LPA_DT_PART2_KEY1],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_lpa_unicode(config, schema, data_db):
    id = "test_offload_lpa_unicode"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_list_multi_part_value_create_ddl(
            schema,
            LPA_UNICODE_FACT_TABLE,
            oracle_column.ORACLE_TYPE_NVARCHAR2,
            [LPA_UNICODE_PART1_KEY1, LPA_UNICODE_PART1_KEY2, LPA_UNICODE_PART2_KEY1],
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_UNICODE_FACT_TABLE
            ),
        ],
    )

    # Offload a partition from a table that has unicode partition keys.
    # Impala/HDFS do not support unicode partition keys so we partition the backend by ID instead.
    # We can still check code and metadata honour the characters.
    options = {
        "owner_table": schema + "." + LPA_UNICODE_FACT_TABLE,
        "equal_to_values": [(LPA_UNICODE_PART1_KEY1, LPA_UNICODE_PART1_KEY2)],
        "offload_partition_columns": partition_columns_if_supported(backend_api, "id"),
        "offload_partition_granularity": "100",
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 10000,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_UNICODE_FACT_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_UNICODE_PART1_KEY1, LPA_UNICODE_PART1_KEY2],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload next LPA partition.
    options = {
        "owner_table": schema + "." + LPA_UNICODE_FACT_TABLE,
        "equal_to_values": [(LPA_UNICODE_PART2_KEY1,)],
        "verify_row_count": "aggregate",
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_UNICODE_FACT_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_UNICODE_PART1_KEY1, LPA_UNICODE_PART1_KEY2, LPA_UNICODE_PART2_KEY1],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_offload_lpa_fact(config, schema, data_db):
    id = "test_offload_lpa_fact"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.sales_based_list_fact_create_ddl(
            schema,
            LPA_FACT_TABLE,
            default_partition=True,
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, LPA_FACT_TABLE
        ),
    )

    # Offload empty LIST partition.
    options = {
        "owner_table": schema + "." + LPA_FACT_TABLE,
        "equal_to_values": [test_constants.SALES_BASED_LIST_HV_2],
        "offload_partition_lower_value": test_constants.LOWER_YRMON_NUM,
        "offload_partition_upper_value": test_constants.UPPER_YRMON_NUM,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_FACT_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [test_constants.SALES_BASED_LIST_HV_2],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
        backend_table_count_check=0,
    )

    # Offload empty LIST partition.
    options = {
        "owner_table": schema + "." + LPA_FACT_TABLE,
        "equal_to_values": [test_constants.SALES_BASED_LIST_HV_1],
        "offload_partition_lower_value": test_constants.LOWER_YRMON_NUM,
        "offload_partition_upper_value": test_constants.UPPER_YRMON_NUM,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_FACT_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [test_constants.SALES_BASED_LIST_HV_1],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload IPA an empty LIST partition and ensure, even though no data copied, metadata reflects current and past predicates.
    options = {
        "owner_table": schema + "." + LPA_FACT_TABLE,
        "equal_to_values": [test_constants.SALES_BASED_LIST_HV_2],
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_FACT_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [test_constants.SALES_BASED_LIST_HV_1, test_constants.SALES_BASED_LIST_HV_2],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Offload IPA LIST by partition name.
    options = {
        "owner_table": schema + "." + LPA_FACT_TABLE,
        "partition_names_csv": test_constants.SALES_BASED_LIST_PNAME_3
        + ","
        + test_constants.SALES_BASED_LIST_PNAME_4,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_FACT_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [
            test_constants.SALES_BASED_LIST_HV_1,
            test_constants.SALES_BASED_LIST_HV_2,
            test_constants.SALES_BASED_LIST_HV_3,
            test_constants.SALES_BASED_LIST_HV_4,
        ],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )

    # Attempt and fail to Offload IPA a default partition.
    options = {
        "owner_table": schema + "." + LPA_FACT_TABLE,
        "equal_to_values": ["DEfauLT"],
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=IPA_OFFLOAD_DEFAULT_PARTITION_EXCEPTION_TEXT,
    )

    # Offloads same partition from fact table again which should result in no action and return of False for early abort.
    options = {
        "owner_table": schema + "." + LPA_FACT_TABLE,
        "partition_names_csv": test_constants.SALES_BASED_LIST_PNAME_3,
    }
    run_offload(options, config, messages, expected_status=False)

    # Reset the predicate in a list 90/10 config with no new value, this should be rejected.
    options = {
        "owner_table": schema + "." + LPA_FACT_TABLE,
        "reset_hybrid_view": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=offload_constants.RESET_HYBRID_VIEW_EXCEPTION_TEXT,
    )

    # Reset the predicate in a list 90/10 config with an invalid list.
    options = {
        "owner_table": schema + "." + LPA_FACT_TABLE,
        "reset_hybrid_view": True,
        "partition_names_csv": "NOT_A_PARTITION",
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=NO_MATCHING_PARTITION_EXCEPTION_TEXT,
    )

    # TODO Enable tests below as part of issue-16.
    # Reset the predicate in a list 90/10 config with a new list.
    # options = {
    #    "owner_table": schema + "." + LPA_FACT_TABLE,
    #    "reset_hybrid_view": True,
    #    "partition_names_csv": test_constants.SALES_BASED_LIST_PNAME_3,
    # }
    # run_offload(options, config, messages)
    # assert offload_lpa_fact_assertion(
    #    schema,
    #    data_db,
    #    LPA_FACT_TABLE,
    #    config,
    #    backend_api,
    #    messages,
    #    repo_client,
    #    [
    #        test_constants.SALES_BASED_LIST_HV_3,
    #    ],
    #    incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    # )

    # Offload a partition after the Hybrid View reset, should move data and influence the metadata.
    # We pass HV 5 and 6 but only 6 has not been offloaded, should then have 3, 5 and 6 in view while only copying 6.
    # options = {
    #    "owner_table": schema + "." + LPA_FACT_TABLE,
    #    "reset_hybrid_view": True,
    #    "partition_names_csv": test_constants.SALES_BASED_LIST_PNAME_5
    #    + ","
    #    + test_constants.SALES_BASED_LIST_PNAME_6,
    # }
    # run_offload(options, config, messages)
    # assert offload_lpa_fact_assertion(
    #    schema,
    #    data_db,
    #    LPA_FACT_TABLE,
    #    config,
    #    backend_api,
    #    messages,
    #    repo_client,
    #    [
    #        test_constants.SALES_BASED_LIST_HV_3,
    #        test_constants.SALES_BASED_LIST_HV_5,
    #        test_constants.SALES_BASED_LIST_HV_6,
    #    ],
    #    incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    # )

    # Only way to offload the default partition is to switch to FULL.
    options = {
        "owner_table": schema + "." + LPA_FACT_TABLE,
        "offload_type": OFFLOAD_TYPE_FULL,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_FACT_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        None,
        offload_pattern=scenario_constants.OFFLOAD_PATTERN_100_0,
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
    )


def test_offload_lpa_part_fn(config, schema, data_db):
    id = "test_offload_lpa_part_fn"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)

    if not backend_api.goe_partition_functions_supported():
        messages.log(
            f"Skipping {id} due to goe_partition_functions_supported() == False"
        )
        return

    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
    repo_client = orchestration_repo_client_factory(
        config, messages, trace_action=f"repo_client({id})"
    )
    udf = data_db + "." + test_constants.PARTITION_FUNCTION_TEST_FROM_INT8
    udf_synth_name = synthetic_part_col_name("U0", "CAT")
    udf_synth_name = convert_backend_identifier_case(config, udf_synth_name)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=gen_list_multi_part_value_create_ddl(
            schema,
            LPA_NUM_PART_FUNC_TABLE,
            oracle_column.ORACLE_TYPE_NUMBER,
            [LPA_PART1_KEY1, LPA_PART1_KEY2, LPA_PART2_KEY1],
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, LPA_NUM_PART_FUNC_TABLE
            ),
        ],
    )

    # IPA 90/10 list partition with partition function.
    options = {
        "owner_table": schema + "." + LPA_NUM_PART_FUNC_TABLE,
        "equal_to_values": ["%s,%s" % (LPA_PART1_KEY1, LPA_PART1_KEY2)],
        "offload_partition_functions": udf,
        "offload_partition_lower_value": 0,
        "offload_partition_upper_value": 1000,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_NUM_PART_FUNC_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_PART1_KEY1, LPA_PART1_KEY2],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
        partition_functions=udf,
        synthetic_partition_column_name=udf_synth_name,
    )

    # LPA next partition with partition function.
    options = {
        "owner_table": schema + "." + LPA_NUM_PART_FUNC_TABLE,
        "equal_to_values": [LPA_PART2_KEY1],
        "verify_row_count": "aggregate",
    }
    run_offload(options, config, messages)
    assert offload_lpa_fact_assertion(
        schema,
        data_db,
        LPA_NUM_PART_FUNC_TABLE,
        config,
        backend_api,
        messages,
        repo_client,
        [LPA_PART1_KEY1, LPA_PART1_KEY2, LPA_PART2_KEY1],
        incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
        partition_functions=udf,
    )


# TODO Need to convert offload_list_partition_append_full_story_tests
