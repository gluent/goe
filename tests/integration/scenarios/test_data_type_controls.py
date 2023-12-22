from random import randint
from textwrap import dedent

from numpy import datetime64
import pytest

from goe.offload.backend_table import (
    CAST_VALIDATION_EXCEPTION_TEXT,
    DATA_VALIDATION_SCALE_EXCEPTION_TEXT,
)
from goe.offload.column_metadata import (
    match_table_column,
    GLUENT_TYPE_FIXED_STRING,
    GLUENT_TYPE_VARIABLE_STRING,
    GLUENT_TYPE_INTEGER_1,
    GLUENT_TYPE_INTEGER_2,
    GLUENT_TYPE_INTEGER_4,
    GLUENT_TYPE_INTEGER_8,
    GLUENT_TYPE_DECIMAL,
    GLUENT_TYPE_DOUBLE,
    GLUENT_TYPE_DATE,
)
from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_IMPALA,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
    INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
)
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
    load_db_name,
)
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_source_table import (
    DATA_SAMPLE_SIZE_AUTO,
    DATETIME_STATS_SAMPLING_OPT_ACTION_TEXT,
    COLUMNS_FAILED_SAMPLING_EXCEPTION_TEXT,
)
from goe.offload.operation.data_type_controls import (
    CONFLICTING_DATA_TYPE_OPTIONS_EXCEPTION_TEXT,
    DECIMAL_COL_TYPE_SYNTAX_TEMPLATE,
)

from tests.integration.scenarios.assertion_functions import (
    backend_column_exists,
    frontend_column_exists,
    hint_text_in_log,
)
from tests.integration.scenarios.scenario_runner import (
    ScenarioRunnerException,
    run_offload,
    run_setup,
)
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_load_table,
    drop_backend_test_table,
    no_query_import_transport_method,
)
from tests.testlib.test_framework.backend_testing_api import (
    STORY_TEST_OFFLOAD_NUMS_BARE_NUM,
    STORY_TEST_OFFLOAD_NUMS_BARE_FLT,
    STORY_TEST_OFFLOAD_NUMS_NUM_4,
    STORY_TEST_OFFLOAD_NUMS_NUM_18,
    STORY_TEST_OFFLOAD_NUMS_NUM_19,
    STORY_TEST_OFFLOAD_NUMS_NUM_3_2,
    STORY_TEST_OFFLOAD_NUMS_NUM_13_3,
    STORY_TEST_OFFLOAD_NUMS_NUM_16_1,
    STORY_TEST_OFFLOAD_NUMS_NUM_20_5,
    STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4,
    STORY_TEST_OFFLOAD_NUMS_NUM_3_5,
    STORY_TEST_OFFLOAD_NUMS_NUM_10_M5,
    STORY_TEST_OFFLOAD_NUMS_DEC_10_0,
    STORY_TEST_OFFLOAD_NUMS_DEC_13_9,
    STORY_TEST_OFFLOAD_NUMS_DEC_15_9,
    STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
    STORY_TEST_OFFLOAD_NUMS_DEC_37_3,
    STORY_TEST_OFFLOAD_NUMS_DEC_38_3,
)
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)


DATE_DIM = "STORY_DATES"
DATE_SDIM = "STORY_SDATES"
NUMS_DIM = "STORY_NUMS"
NUM_TOO_BIG_DIM = "STORY_NUM_TB"
WILDCARD_DIM = "STORY_DC_COLS"
OFFLOAD_DIM = "STORY_DC_DIM"

# TODO nj@2020-03-31 when GOE-1528 is fixed we should change bad years below from 0001 to -1000
BAD_DT = "0001-01-01"


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


@pytest.fixture
def load_db(schema, config):
    load_db = load_db_name(schema, config)
    load_db = convert_backend_identifier_case(config, load_db)
    return load_db


def log_test_marker(messages, test_id):
    messages.log(test_id, detail=VVERBOSE)


def cast_validation_exception_text(backend_api):
    """Get expected exception text when cast validation fails"""
    if backend_api:
        if backend_api.backend_type() == DBTYPE_IMPALA:
            return CAST_VALIDATION_EXCEPTION_TEXT
    return DATA_VALIDATION_SCALE_EXCEPTION_TEXT


def num_of_size(digits):
    return "".join([str(randint(1, 9)) for _ in range(digits)])


def nums_setup_frontend_ddl(
    frontend_api, backend_api, config, schema, table_name
) -> list:
    if config.db_type == DBTYPE_ORACLE:
        setup_casts = {
            STORY_TEST_OFFLOAD_NUMS_BARE_NUM: "CAST(1.101 AS NUMBER)",
            STORY_TEST_OFFLOAD_NUMS_BARE_FLT: "CAST(1 AS FLOAT)",
            STORY_TEST_OFFLOAD_NUMS_NUM_4: "CAST(1234 AS NUMBER(4))",
            STORY_TEST_OFFLOAD_NUMS_NUM_18: "CAST(1 AS NUMBER(18))",
            STORY_TEST_OFFLOAD_NUMS_NUM_19: "CAST(1 AS NUMBER(19))",
            STORY_TEST_OFFLOAD_NUMS_NUM_3_2: "CAST(1.12 AS NUMBER(3,2))",
            STORY_TEST_OFFLOAD_NUMS_NUM_13_3: "CAST(1.123 AS NUMBER(13,3))",
            STORY_TEST_OFFLOAD_NUMS_NUM_16_1: "CAST(1.1 AS NUMBER(16,1))",
            STORY_TEST_OFFLOAD_NUMS_NUM_20_5: "CAST(1.12345 AS NUMBER(20,5))",
            STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4: "CAST(1.1234 AS NUMBER(*,4))",
            STORY_TEST_OFFLOAD_NUMS_NUM_3_5: "CAST(0.001 AS NUMBER(3,5))",
            STORY_TEST_OFFLOAD_NUMS_NUM_10_M5: "CAST(1 AS NUMBER(10,-5))",
            STORY_TEST_OFFLOAD_NUMS_DEC_10_0: "CAST(1234567890 AS NUMBER)",
            STORY_TEST_OFFLOAD_NUMS_DEC_13_9: "CAST(1234.123456789 AS NUMBER)",
            STORY_TEST_OFFLOAD_NUMS_DEC_15_9: "CAST(123456.123456789 AS NUMBER)",
            STORY_TEST_OFFLOAD_NUMS_DEC_36_3: "CAST(%s.123 AS NUMBER)"
            % num_of_size(20),
            STORY_TEST_OFFLOAD_NUMS_DEC_37_3: "CAST(%s.123 AS NUMBER)"
            % num_of_size(20),
            STORY_TEST_OFFLOAD_NUMS_DEC_38_3: "CAST(%s.123 AS NUMBER)"
            % num_of_size(20),
        }
        select_template = "SELECT {} FROM dual"
    elif config.db_type == DBTYPE_TERADATA:
        setup_casts = {
            STORY_TEST_OFFLOAD_NUMS_BARE_NUM: "CAST(1.101 AS NUMBER)",
            STORY_TEST_OFFLOAD_NUMS_NUM_4: "CAST(1234 AS NUMBER(4))",
            STORY_TEST_OFFLOAD_NUMS_NUM_18: "CAST(1 AS NUMBER(18))",
            STORY_TEST_OFFLOAD_NUMS_NUM_19: "CAST(1 AS NUMBER(19))",
            STORY_TEST_OFFLOAD_NUMS_NUM_3_2: "CAST(1.12 AS NUMBER(3,2))",
            STORY_TEST_OFFLOAD_NUMS_NUM_13_3: "CAST(1.123 AS NUMBER(13,3))",
            STORY_TEST_OFFLOAD_NUMS_NUM_16_1: "CAST(1.1 AS NUMBER(16,1))",
            STORY_TEST_OFFLOAD_NUMS_NUM_20_5: "CAST(1.12345 AS NUMBER(20,5))",
            STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4: "CAST(1.1234 AS NUMBER(*,4))",
            STORY_TEST_OFFLOAD_NUMS_DEC_10_0: "CAST(1234567890 AS NUMBER)",
            STORY_TEST_OFFLOAD_NUMS_DEC_13_9: "CAST(1234.123456789 AS NUMBER)",
            STORY_TEST_OFFLOAD_NUMS_DEC_15_9: "CAST(123456.123456789 AS NUMBER)",
            STORY_TEST_OFFLOAD_NUMS_DEC_36_3: "CAST(%s.123 AS NUMBER)"
            % num_of_size(20),
            STORY_TEST_OFFLOAD_NUMS_DEC_37_3: "CAST(%s.123 AS NUMBER)"
            % num_of_size(20),
            STORY_TEST_OFFLOAD_NUMS_DEC_38_3: "CAST(%s.123 AS NUMBER)"
            % num_of_size(20),
        }
        select_template = "SELECT {}"
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    subquery = select_template.format(
        "\n,      ".join(
            "{} AS {}".format(setup_casts[_], _)
            for _ in setup_casts
            if _ in backend_api.story_test_offload_nums_expected_backend_types()
        )
    )
    return frontend_api.gen_ctas_from_subquery(
        schema, table_name, subquery, with_stats_collection=True
    )


def dates_setup_frontend_ddl(frontend_api, config, schema, table_name) -> list:
    if config.db_type == DBTYPE_ORACLE:
        subquery = dedent(
            """\
        SELECT TRUNC(SYSDATE) AS dt
        ,      CAST(TRUNC(SYSDATE) AS TIMESTAMP(0)) AS ts0
        ,      CAST(TRUNC(SYSDATE) AS TIMESTAMP(6)) AS ts6
        ,      CAST(TIMESTAMP'2001-10-31 01:00:00 -5:00' AS TIMESTAMP(0) WITH TIME ZONE) AS ts0tz
        ,      CAST(TIMESTAMP'2001-10-31 01:00:00 -5:00' AS TIMESTAMP(6) WITH TIME ZONE) AS ts6tz
        FROM   dual"""
        )
    elif config.db_type == DBTYPE_TERADATA:
        subquery = dedent(
            """\
        SELECT CURRENT_DATE AS dt
        ,      CAST(CURRENT_DATE AS TIMESTAMP(0)) AS ts0
        ,      CAST(CURRENT_DATE AS TIMESTAMP(6)) AS ts6
        ,      CAST(TRUNC(TO_TIMESTAMP_TZ('2001-10-31 01:00:00 -5:00','YYYY-MM-DD HH24:MI:SS TZH:TZM')) AS TIMESTAMP(0) WITH TIME ZONE) AS ts0tz
        ,      CAST(TO_TIMESTAMP_TZ('2001-10-31 01:00:00 -5:00','YYYY-MM-DD HH24:MI:SS TZH:TZM') AS TIMESTAMP(6) WITH TIME ZONE) AS ts6tz"""
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    return frontend_api.gen_ctas_from_subquery(
        schema, table_name, subquery, with_stats_collection=True
    )


def samp_dates_setup_frontend_ddl(frontend_api, config, schema, table_name) -> list:
    if config.db_type == DBTYPE_ORACLE:
        subquery = (
            dedent(
                """\
        SELECT DATE' %(bad_dt)s'                             AS bad_date
        ,      TIMESTAMP'%(bad_dt)s 00:00:00.000000'         AS bad_ts
        --,      TIMESTAMP'1400-01-01 01:00:00 +5:00'          AS bad_tstz
        ,      SYSDATE                                       AS good_date
        ,      CAST(SYSDATE AS TIMESTAMP(6))                 AS good_ts
        ,      TIMESTAMP'1400-01-01 01:00:00 -5:00'          AS good_tstz
        FROM   dual"""
            )
            % {"bad_dt": BAD_DT}
        )
    elif config.db_type == DBTYPE_TERADATA:
        subquery = (
            dedent(
                """\
        SELECT DATE '%(bad_dt)s'                             AS bad_date
        ,      TIMESTAMP '%(bad_dt)s 00:00:00.000000'         AS bad_ts
        --,      TIMESTAMP'1400-01-01 01:00:00 +5:00'           AS bad_tstz
        ,      CURRENT_DATE                                  AS good_date
        ,      CAST(CURRENT_DATE AS TIMESTAMP(6))            AS good_ts
        ,      TO_TIMESTAMP_TZ('1400-01-01 01:00:00 -5:00','YYYY-MM-DD HH24:MI:SS TZH:TZM') AS good_tstz"""
            )
            % {"bad_dt": BAD_DT}
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    return frontend_api.gen_ctas_from_subquery(
        schema, table_name, subquery, with_stats_collection=True
    )


def num_overflow_setup_frontend_ddl(
    frontend_api, config, schema, table_name, with_stats=True
) -> list:
    if config.db_type in DBTYPE_ORACLE:
        subquery = "SELECT 1 AS id, CAST(%s AS NUMBER(38)) AS num FROM dual" % (
            "123".ljust(38, "0")
        )
    elif config.db_type == DBTYPE_TERADATA:
        subquery = "SELECT 1 AS id, CAST(%s AS NUMBER(38)) AS num" % (
            schema,
            table_name,
            "123".ljust(38, "0"),
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    return frontend_api.gen_ctas_from_subquery(
        schema, table_name, subquery, with_stats_collection=with_stats
    )


def num_scale_overflow_setup_frontend_ddl(
    frontend_api, config, schema, table_name
) -> list:
    if config.db_type == DBTYPE_ORACLE:
        subquery = "SELECT CAST(1 AS NUMBER(4)) AS id, CAST(12.0123456789 AS NUMBER(20,10)) AS num FROM dual"
    elif config.db_type == DBTYPE_TERADATA:
        subquery = "SELECT CAST(1 AS NUMBER(4)) AS id, CAST(12.0123456789 AS NUMBER(20,10)) AS num"
    else:
        raise NotImplementedError(f"Unsupported db_type: {config.db_type}")
    return frontend_api.gen_ctas_from_subquery(
        schema, table_name, subquery, with_stats_collection=True
    )


def wildcard_setup_frontend_ddl(frontend_api, config, schema, table_name) -> list:
    subquery = dedent(
        """\
        SELECT 1                          AS table_id
        ,      2                          AS cust_id
        ,      3                          AS prod_id
        ,      2012                       AS txn_year
        ,      201209                     AS txn_month
        ,      20120931                   AS txn_day
        ,      4                          AS txn_qtr
        ,      DATE'2012-10-31'           AS txn_date
        ,      17.5                       AS txn_rate
        ,      CAST('ABC' AS VARCHAR(5)) AS txn_desc
        ,      2012                       AS sale_year
        ,      201209                     AS sale_month
        ,      20120931                   AS sale_day
        ,      DATE'2012-10-31'           AS sale_date
        ,      4                          AS sale_qtr
        ,      17.5                       AS sale_rate
        ,      123.55                     AS sale_amt
        ,      CAST('ABC' AS CHAR(3))     AS sale_desc"""
    )
    if config.db_type == DBTYPE_ORACLE:
        subquery += " FROM dual"
    return frontend_api.gen_ctas_from_subquery(
        schema, table_name, subquery, with_stats_collection=True
    )


def nums_assertion(
    config,
    frontend_api,
    backend_api,
    messages,
    data_db,
    table_name,
    detect=True,
    check_dec_10_0=True,
):
    for (
        col_name,
        expected_type,
    ) in backend_api.story_test_offload_nums_expected_backend_types(
        sampling_enabled=detect
    ).items():
        if col_name == STORY_TEST_OFFLOAD_NUMS_DEC_10_0 and not check_dec_10_0:
            # This test only makes sense when we force it to a decimal via options (nums_on test)
            continue
        if not frontend_column_exists(
            frontend_api, messages, data_db, table_name, col_name
        ):
            # Not all test columns are valid in all frontends
            continue
        if not backend_column_exists(
            config,
            backend_api,
            messages,
            data_db,
            table_name,
            col_name,
            search_type=expected_type,
        ):
            raise ScenarioRunnerException
    return True


def date_assertion(
    config,
    frontend_api,
    backend_api,
    messages,
    data_db,
    table_name,
    forced_to_date=False,
    forced_to_tstz=False,
):
    """Check outcome of DATE_DIM offloads are correct.
    Has hardcoded Hadoop data types which is necessary because we need to confirm the correct outcome, we
    can't use canonical translation because that's what we are trying to check.
    """
    if not backend_api:
        return True
    if forced_to_date:
        expected_dt_data_type = backend_api.backend_test_type_canonical_date()
        expected_ts_data_type = backend_api.backend_test_type_canonical_date()
    elif forced_to_tstz:
        expected_dt_data_type = backend_api.backend_test_type_canonical_timestamp_tz()
        expected_ts_data_type = backend_api.backend_test_type_canonical_timestamp_tz()
    else:
        if frontend_api.canonical_date_supported():
            expected_dt_data_type = backend_api.backend_test_type_canonical_date()
        else:
            expected_dt_data_type = backend_api.backend_test_type_canonical_timestamp()
        expected_ts_data_type = backend_api.backend_test_type_canonical_timestamp()
    expected_tstz_data_type = backend_api.backend_test_type_canonical_timestamp_tz()
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        table_name,
        "dt",
        search_type=expected_dt_data_type,
    ):
        raise ScenarioRunnerException(
            "Backend column does not exist: %s (%s)" % ("dt", expected_dt_data_type)
        )
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        table_name,
        "ts0",
        search_type=expected_ts_data_type,
    ):
        raise ScenarioRunnerException(
            "Backend column does not exist: %s (%s)" % ("ts0", expected_ts_data_type)
        )
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        table_name,
        "ts0tz",
        search_type=expected_tstz_data_type,
    ):
        raise ScenarioRunnerException(
            "Backend column does not exist: %s (%s)"
            % ("ts0tz", expected_tstz_data_type)
        )
    return True


def samp_date_assertion(
    config,
    backend_api,
    frontend_api,
    messages,
    data_db,
    backend_name,
    from_stats=True,
    good_as_date=False,
):
    """Check outcome of DATE_SDIM offloads are correct."""
    if frontend_api.canonical_date_supported():
        expected_good_data_type = backend_api.backend_test_type_canonical_date()
    else:
        expected_good_data_type = backend_api.backend_test_type_canonical_timestamp()
    if backend_api.min_datetime_value() > datetime64(BAD_DT):
        expected_bad_data_type = backend_api.backend_test_type_canonical_string()
    else:
        expected_bad_data_type = expected_good_data_type

    if good_as_date:
        if backend_api.canonical_date_supported():
            expected_good_data_type = backend_api.backend_test_type_canonical_date()

    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "bad_date",
        search_type=expected_bad_data_type,
    ):
        raise ScenarioRunnerException(
            "Backend column does not exist: %s (%s)"
            % ("bad_date", expected_bad_data_type)
        )
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "bad_ts",
        search_type=expected_bad_data_type,
    ):
        raise ScenarioRunnerException(
            "Backend column does not exist: %s (%s)"
            % ("bad_ts", expected_bad_data_type)
        )
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "good_date",
        search_type=expected_good_data_type,
    ):
        raise ScenarioRunnerException(
            "Backend column does not exist: %s (%s)"
            % ("good_date", expected_good_data_type)
        )
    if not backend_column_exists(
        config,
        backend_api,
        messages,
        data_db,
        backend_name,
        "good_ts",
        search_type=expected_good_data_type,
    ):
        raise ScenarioRunnerException(
            "Backend column does not exist: %s (%s)"
            % ("good_ts", expected_good_data_type)
        )
    if expected_bad_data_type != expected_good_data_type:
        text_match = messages.text_in_messages(DATETIME_STATS_SAMPLING_OPT_ACTION_TEXT)
        if text_match != from_stats:
            raise ScenarioRunnerException(
                "text_match != from_stats: %s != %s" % (text_match, from_stats)
            )
    return True


def wildcard_assertion(backend_api, data_db, backend_name):
    if not backend_api:
        return True

    def check_data_type(backend_column, expected_data_type):
        if backend_column.data_type != expected_data_type:
            raise ScenarioRunnerException(
                "Column %s offloaded to wrong backend type: %s != %s"
                % (
                    backend_column.name.upper(),
                    backend_column.data_type,
                    expected_data_type,
                )
            )
        return True

    for backend_column in backend_api.get_columns(data_db, backend_name):
        if backend_column.name.lower().endswith("_id"):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={"integer_1_columns_csv": backend_column.name}
            )[GLUENT_TYPE_INTEGER_1]
            if not check_data_type(backend_column, expected_backend_type):
                raise ScenarioRunnerException
        if backend_column.name.lower().endswith(
            "_year"
        ) or backend_column.name.lower().endswith("_qtr"):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={"integer_2_columns_csv": backend_column.name}
            )[GLUENT_TYPE_INTEGER_2]
            if not check_data_type(backend_column, expected_backend_type):
                raise ScenarioRunnerException
        if backend_column.name.lower().endswith("_month"):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={"integer_4_columns_csv": backend_column.name}
            )[GLUENT_TYPE_INTEGER_4]
            if not check_data_type(backend_column, expected_backend_type):
                raise ScenarioRunnerException
        if backend_column.name.lower().endswith("_day"):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={"integer_8_columns_csv": backend_column.name}
            )[GLUENT_TYPE_INTEGER_8]
            if not check_data_type(backend_column, expected_backend_type):
                raise ScenarioRunnerException
        if backend_column.name.lower().endswith("_date"):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={"date_columns_csv": backend_column.name}
            )[GLUENT_TYPE_DATE]
            if not check_data_type(backend_column, expected_backend_type):
                raise ScenarioRunnerException
        if backend_column.name.lower().endswith("_rate"):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={"double_columns_csv": backend_column.name}
            )[GLUENT_TYPE_DOUBLE]
            if not check_data_type(backend_column, expected_backend_type):
                raise ScenarioRunnerException
        if backend_column.name.lower().endswith("_amt"):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={"decimal_columns_csv_list": backend_column.name}
            )[GLUENT_TYPE_DECIMAL]
            if not check_data_type(backend_column, expected_backend_type):
                raise ScenarioRunnerException
        if backend_column.name.lower().endswith("_desc"):
            input_type = (
                GLUENT_TYPE_FIXED_STRING
                if backend_column.name.lower() == "sale_desc"
                else GLUENT_TYPE_VARIABLE_STRING
            )
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={"unicode_string_columns_csv": backend_column.name}
            )[input_type]
            if not check_data_type(backend_column, expected_backend_type):
                raise ScenarioRunnerException
    return True


def unicode_assertion(backend_api, data_db, backend_name, asserted_unicode_columns):
    assert isinstance(asserted_unicode_columns, dict)
    unicode_column_names = [_.upper() for _ in asserted_unicode_columns.keys()]
    backend_columns = [
        _
        for _ in backend_api.get_columns(data_db, backend_name)
        if _.name.upper() in unicode_column_names
    ]
    expected_backend_types = backend_api.expected_canonical_to_backend_type_map(
        override_used=["unicode_string_columns_csv"]
    )
    for column_name, expected_canonical_type in asserted_unicode_columns.items():
        backend_column = match_table_column(column_name, backend_columns)
        if backend_column.data_type != expected_backend_types[expected_canonical_type]:
            raise ScenarioRunnerException(
                "Column %s offloaded to wrong backend type: %s != %s"
                % (
                    backend_column.name.upper(),
                    backend_column.data_type,
                    expected_backend_types[expected_canonical_type],
                )
            )
    return True


def test_numeric_controls(config, schema, data_db):
    id = "test_numeric_controls"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    max_decimal_precision = backend_api.max_decimal_precision() if backend_api else None
    max_decimal_scale = backend_api.max_decimal_scale() if backend_api else None

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=nums_setup_frontend_ddl(
            frontend_api, backend_api, config, schema, NUMS_DIM
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, NUMS_DIM
        ),
    )

    # Offload table with assorted number columns with number detection disabled, offloads to defaults.
    options = {
        "owner_table": schema + "." + NUMS_DIM,
        "data_sample_pct": 0,
        "reset_backend_table": True,
        "decimal_padding_digits": 2,
    }
    run_offload(options, config, messages)
    nums_assertion(
        config, frontend_api, backend_api, messages, data_db, NUMS_DIM, detect=False
    )

    # Query Import Offload with assorted number columns with number detection enabled and type overrides.
    options = {
        "owner_table": schema + "." + NUMS_DIM,
        "reset_backend_table": True,
        "decimal_columns_csv_list": [
            STORY_TEST_OFFLOAD_NUMS_DEC_10_0,
            STORY_TEST_OFFLOAD_NUMS_DEC_13_9,
            STORY_TEST_OFFLOAD_NUMS_DEC_15_9,
            STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
            STORY_TEST_OFFLOAD_NUMS_DEC_37_3,
            STORY_TEST_OFFLOAD_NUMS_DEC_38_3,
        ],
        "decimal_columns_type_list": ["10,0", "13,9", "15,9", "36,3", "37,3", "38,3"],
        "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
        "decimal_padding_digits": 2,
    }
    run_offload(options, config, messages)
    nums_assertion(
        config, frontend_api, backend_api, messages, data_db, NUMS_DIM, detect=True
    )

    # Offload table with assorted number columns with number detection enabled and type overrides.
    options = {
        "owner_table": schema + "." + NUMS_DIM,
        "offload_transport_method": no_query_import_transport_method(config),
        "reset_backend_table": True,
        "decimal_columns_csv_list": [
            STORY_TEST_OFFLOAD_NUMS_DEC_10_0,
            STORY_TEST_OFFLOAD_NUMS_DEC_13_9,
            STORY_TEST_OFFLOAD_NUMS_DEC_15_9,
            STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
            STORY_TEST_OFFLOAD_NUMS_DEC_37_3,
            STORY_TEST_OFFLOAD_NUMS_DEC_38_3,
        ],
        "decimal_columns_type_list": ["10,0", "13,9", "15,9", "36,3", "37,3", "38,3"],
        "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
        "decimal_padding_digits": 2,
    }
    run_offload(options, config, messages)
    nums_assertion(
        config, frontend_api, backend_api, messages, data_db, NUMS_DIM, detect=True
    )

    # Offload table with assorted number columns with number detection for sampling.
    # Check the DEC_ columns have correct precision/scale.
    # Still use options for dec_36_3, dec_37_3, dec_38_3 just so the test passes.
    options = {
        "owner_table": schema + "." + NUMS_DIM,
        "reset_backend_table": True,
        "decimal_columns_csv_list": [
            STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
            STORY_TEST_OFFLOAD_NUMS_DEC_37_3,
            STORY_TEST_OFFLOAD_NUMS_DEC_38_3,
        ],
        "decimal_columns_type_list": ["36,3", "37,3", "38,3"],
        "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
        "decimal_padding_digits": 2,
    }
    run_offload(options, config, messages)
    nums_assertion(
        config,
        frontend_api,
        backend_api,
        messages,
        data_db,
        NUMS_DIM,
        detect=True,
        check_dec_10_0=False,
    )

    # Offload Dimension With Parallel Sampling=0.
    # Runs with --no-verify to remove risk of verification having a PARALLEL hint.
    if config.db_type == DBTYPE_ORACLE:
        options = {
            "owner_table": schema + "." + NUMS_DIM,
            "data_sample_parallelism": 0,
            "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
            "reset_backend_table": True,
            "verify_row_count": False,
        }
        log_test_marker(messages, f"{id}:samp1")
        run_offload(options, config, messages)
        assert hint_text_in_log(messages, config, 0, f"{id}:samp1")

    # Offload Dimension With Parallel Sampling=3.
    # Runs with --no-verify to remove risk of verification having a PARALLEL hint.
    if config.db_type == DBTYPE_ORACLE:
        options = {
            "owner_table": schema + "." + NUMS_DIM,
            "data_sample_parallelism": 3,
            "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
            "reset_backend_table": True,
            "verify_row_count": False,
        }
        log_test_marker(messages, f"{id}:samp2")
        run_offload(options, config, messages)
        assert hint_text_in_log(messages, config, 3, f"{id}:samp2")

    # Offload Dimension with number overflow (expect to fail).
    if config.target not in [DBTYPE_BIGQUERY]:
        options = {
            "owner_table": schema + "." + NUMS_DIM,
            "reset_backend_table": True,
            "decimal_columns_csv_list": [
                ",".join(
                    [
                        STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
                        STORY_TEST_OFFLOAD_NUMS_DEC_37_3,
                        STORY_TEST_OFFLOAD_NUMS_DEC_38_3,
                    ]
                )
            ],
            "decimal_columns_type_list": ["10,2"],
            "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=cast_validation_exception_text(backend_api),
        )

    # Offload table with bad precision (expect to fail).
    options = {
        "owner_table": schema + "." + NUMS_DIM,
        "decimal_columns_csv_list": [",".join([STORY_TEST_OFFLOAD_NUMS_DEC_36_3])],
        "decimal_columns_type_list": ["100,10"],
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=DECIMAL_COL_TYPE_SYNTAX_TEMPLATE.format(
            p=max_decimal_precision, s=max_decimal_scale
        ),
    )

    # Offload table with bad scale (expect to fail).
    options = {
        "owner_table": schema + "." + NUMS_DIM,
        "decimal_columns_csv_list": [",".join([STORY_TEST_OFFLOAD_NUMS_DEC_36_3])],
        "decimal_columns_type_list": ["10,100"],
        "reset_backend_table": True,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=DECIMAL_COL_TYPE_SYNTAX_TEMPLATE.format(
            p=max_decimal_precision, s=max_decimal_scale
        ),
    )
    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_date_controls(config, schema, data_db):
    id = "test_date_controls"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    # Setup
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=dates_setup_frontend_ddl(frontend_api, config, schema, DATE_DIM),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, DATE_DIM
        ),
    )

    # Offload dimension with dates to defaults.
    options = {
        "owner_table": schema + "." + DATE_DIM,
        "data_sample_pct": 0,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)
    date_assertion(config, frontend_api, backend_api, messages, data_db, DATE_DIM)

    if backend_api.canonical_date_supported():
        # Offload dimension with dates forced to canonical DATE.
        options = {
            "owner_table": schema + "." + DATE_DIM,
            "data_sample_pct": 0,
            "date_columns_csv": "dt,ts0,ts6",
            "reset_backend_table": True,
        }
        run_offload(options, config, messages)
        date_assertion(
            config,
            frontend_api,
            backend_api,
            messages,
            data_db,
            DATE_DIM,
            forced_to_date=True,
        )

    # Offload dimension with dates forced to canonical TIMESTAMP_TZ.
    options = {
        "owner_table": schema + "." + DATE_DIM,
        "data_sample_pct": 0,
        "timestamp_tz_columns_csv": "dt,ts0,ts6,ts0tz,ts6tz",
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)
    date_assertion(
        config,
        frontend_api,
        backend_api,
        messages,
        data_db,
        DATE_DIM,
        forced_to_tstz=True,
    )
    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_date_sampling(config, schema, data_db):
    id = "test_date_sampling"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    # Create Dimension containing dates that need sampling.
    # TODO nj@2018-06-13 cannot test bad TZ values due to GOE-1102, uncomment bad_tstz/bad_tsltz during GOE-1102
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=samp_dates_setup_frontend_ddl(
            frontend_api, config, schema, DATE_SDIM
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, DATE_SDIM
        ),
    )

    # Offload Dimension containing bad dates with stats, detection should be done from stats.
    options = {
        "owner_table": schema + "." + DATE_SDIM,
        "allow_nanosecond_timestamp_columns": True,
        "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)
    samp_date_assertion(config, backend_api, frontend_api, messages, data_db, DATE_SDIM)

    # Remove stats from DATE_SDIM.
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=[
            frontend_api.remove_table_stats_sql_text(schema.upper(), DATE_SDIM.upper())
        ],
    )

    # Offload Dimension containing bad dates without stats, detection should be done using SQL.
    options = {
        "owner_table": schema + "." + DATE_SDIM,
        "allow_nanosecond_timestamp_columns": True,
        "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)
    samp_date_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        data_db,
        DATE_SDIM,
        from_stats=False,
    )

    # We've had cases where stats appear between prior test and next one, so drop stats again here to be sure.
    # Remove stats from DATE_SDIM.
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=[
            frontend_api.remove_table_stats_sql_text(schema.upper(), DATE_SDIM.upper())
        ],
    )

    # Offload Dimension containing dates and influence canonical type using --date-columns.
    options = {
        "owner_table": schema + "." + DATE_SDIM,
        "allow_nanosecond_timestamp_columns": True,
        "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
        "date_columns_csv": "good_date,good_ts",
        "reset_backend_table": True,
    }
    run_offload(options, config, messages)
    samp_date_assertion(
        config,
        backend_api,
        frontend_api,
        messages,
        data_db,
        DATE_SDIM,
        from_stats=False,
        good_as_date=True,
    )
    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_precision_scale_overflow(config, schema, data_db):
    id = "test_precision_scale_overflow"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    if backend_api.max_decimal_integral_magnitude() < 38:
        # Create a table with data that is too big for the backend and no dbms_stats call.
        # Without optimizer stats sampling we should still catch the bad value.
        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=num_overflow_setup_frontend_ddl(
                frontend_api, config, schema, NUM_TOO_BIG_DIM, with_stats=False
            ),
            python_fns=lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, NUM_TOO_BIG_DIM
            ),
        )

        # Offload NUM_TOO_BIG_DIM with number overflow (expect to fail).
        options = {
            "owner_table": schema + "." + NUM_TOO_BIG_DIM,
            "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
            "reset_backend_table": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=COLUMNS_FAILED_SAMPLING_EXCEPTION_TEXT,
        )

        # Collect Stats On NUM_TOO_BIG_DIM
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=[
                frontend_api.collect_table_stats_sql_text(
                    schema.upper(), NUM_TOO_BIG_DIM.upper()
                )
            ],
        )

        # Offload NUM_TOO_BIG_DIM with number overflow (expect to fail).
        options = {
            "owner_table": schema + "." + NUM_TOO_BIG_DIM,
            "data_sample_pct": DATA_SAMPLE_SIZE_AUTO,
            "reset_backend_table": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=COLUMNS_FAILED_SAMPLING_EXCEPTION_TEXT,
        )

        # Offload NUM_TOO_BIG_DIM with number overflow (expect to fail).
        # Disable sampling, we will see CAST validation catch the problem data instead.
        options = {
            "owner_table": schema + "." + NUM_TOO_BIG_DIM,
            "data_sample_pct": 0,
            "reset_backend_table": True,
        }
        run_offload(
            options,
            config,
            messages,
            expected_exception_string=CAST_VALIDATION_EXCEPTION_TEXT,
        )

    # Create the table with scale that is too big for backend table
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=num_scale_overflow_setup_frontend_ddl(
            frontend_api, config, schema, NUM_TOO_BIG_DIM
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, NUM_TOO_BIG_DIM
        ),
    )

    # Offload NUM_TOO_BIG_DIM with scale overflow based on backend column spec (expect to fail).
    options = {
        "owner_table": schema + "." + NUM_TOO_BIG_DIM,
        "decimal_columns_csv_list": ["num"],
        "decimal_columns_type_list": ["20,5"],
        "reset_backend_table": True,
        "decimal_padding_digits": 0,
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=DATA_VALIDATION_SCALE_EXCEPTION_TEXT,
    )

    # Offload NUM_TOO_BIG_DIM with scale overflow based on backend column spec.
    options = {
        "owner_table": schema + "." + NUM_TOO_BIG_DIM,
        "decimal_columns_csv_list": ["num"],
        "decimal_columns_type_list": ["20,5"],
        "allow_decimal_scale_rounding": True,
        "reset_backend_table": True,
        "decimal_padding_digits": 0,
    }
    run_offload(
        options,
        config,
        messages,
    )
    # Connections are being left open, explicitly close them.
    frontend_api.close()


def test_datatype_controls_column_name_checks(config, schema, data_db, load_db):
    id = "test_datatype_controls_column_name_checks"
    messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    wildcard_dim_be, offload_dim_be = convert_backend_identifier_case(
        config, WILDCARD_DIM, OFFLOAD_DIM
    )

    # Create table with column name patterns as discussed in GOE-1670.
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=wildcard_setup_frontend_ddl(
            frontend_api, config, schema, WILDCARD_DIM
        ),
        python_fns=lambda: drop_backend_test_table(
            config, backend_api, messages, data_db, WILDCARD_DIM
        ),
    )

    # Offload with wildcards in data type controls.
    options = {
        "owner_table": schema + "." + WILDCARD_DIM,
        "reset_backend_table": True,
        "integer_1_columns_csv": "*_id",
        "integer_2_columns_csv": "*_year,*_QTR",
        "integer_4_columns_csv": "*_month",
        "integer_8_columns_csv": "*_day",
        "date_columns_csv": "*_date",
        "double_columns_csv": "*_rate",
        "decimal_columns_csv_list": ["*amt"],
        "decimal_columns_type_list": ["12,3"],
        "unicode_string_columns_csv": "*_desc",
        "decimal_padding_digits": 0,
    }
    run_offload(options, config, messages)
    wildcard_assertion(backend_api, data_db, wildcard_dim_be)

    # Offload with overlapping wildcards in data type controls (expect to fail).
    options = {
        "owner_table": schema + "." + WILDCARD_DIM,
        "reset_backend_table": True,
        "integer_1_columns_csv": "*_id",
        "integer_2_columns_csv": "*id",
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=CONFLICTING_DATA_TYPE_OPTIONS_EXCEPTION_TEXT,
    )

    # Setup OFFLOAD_DIM
    run_setup(
        frontend_api,
        backend_api,
        config,
        messages,
        frontend_sqls=frontend_api.standard_dimension_frontend_ddl(schema, OFFLOAD_DIM),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, OFFLOAD_DIM
            ),
            lambda: drop_backend_test_load_table(
                config, backend_api, messages, load_db, OFFLOAD_DIM
            ),
        ],
    )

    # Offload with number column as string (expect to fail).
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "reset_backend_table": True,
        "variable_string_columns_csv": "prod_id",
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
    )

    # Offload with number column as date (expect to fail)',
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "reset_backend_table": True,
        "date_columns_csv": "prod_id",
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
    )

    # Offload Dimension with date column as number (expect to fail)
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "reset_backend_table": True,
        "integer_8_columns_csv": "TXN_date",
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
    )

    # Offload Dimension with string column as date (expect to fail)',
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "reset_backend_table": True,
        "date_columns_csv": "TXN_DESC",
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
    )

    # Offload Dimension with string column as time zoned date (expect to fail)',
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "reset_backend_table": True,
        "timestamp_tz_columns_csv": "TXN_DESC",
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
    )

    # Offload Dimension with string column as number (expect to fail)',
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "reset_backend_table": True,
        "integer_4_columns_csv": "TXN_DESC",
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
    )

    # Offload Dimension with number column as unicode string (expect to fail)',
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "reset_backend_table": True,
        "unicode_string_columns_csv": "PROD_ID",
    }
    run_offload(
        options,
        config,
        messages,
        expected_exception_string=INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT,
    )

    # Offload Dimension with string column as unicode string.
    options = {
        "owner_table": schema + "." + OFFLOAD_DIM,
        "reset_backend_table": True,
        "unicode_string_columns_csv": "TXN_DESC",
        "skip": ["verify_exported_data"],
    }
    run_offload(
        options,
        config,
        messages,
    )
    unicode_assertion(
        backend_api, data_db, offload_dim_be, {"TXN_DESC": GLUENT_TYPE_VARIABLE_STRING}
    )
    # Connections are being left open, explicitly close them.
    frontend_api.close()
