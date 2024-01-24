import time

from test_sets.stories.story_globals import (
    OFFLOAD_PATTERN_90_10,
    STORY_SETUP_TYPE_FRONTEND,
    STORY_SETUP_TYPE_HYBRID,
    STORY_SETUP_TYPE_PYTHON,
    STORY_SETUP_TYPE_ORACLE,
    STORY_TYPE_OFFLOAD,
    STORY_TYPE_SETUP,
)
from test_sets.stories.story_setup_functions import (
    dbms_stats_gather_string,
    drop_backend_test_table,
    gen_hybrid_drop_ddl,
    gen_rdbms_dim_create_ddl,
    gen_sales_based_fact_create_ddl,
    SALES_BASED_FACT_HV_1,
    SALES_BASED_FACT_HV_2,
    SALES_BASED_FACT_HV_5,
)
from test_sets.stories.story_assertion_functions import (
    load_table_is_compressed,
    offload_dim_assertion,
    offload_fact_assertions,
    offload_lpa_fact_assertion,
    table_minus_row_count,
    text_in_messages,
    text_in_log,
)

from goe.gluent import get_log_fh
from goe.filesystem.gluent_dfs_factory import get_dfs_from_options
from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
    OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
)
from goe.offload.offload_functions import convert_backend_identifier_case
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_metadata_functions import INCREMENTAL_PREDICATE_TYPE_LIST
from goe.offload.offload_transport import (
    is_query_import_available,
    is_spark_thrift_available,
    is_spark_submit_available,
    is_sqoop_available,
    is_livy_available,
    OFFLOAD_TRANSPORT_GLUENT,
    OFFLOAD_TRANSPORT_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
    MISSING_ROWS_IMPORTED_WARNING,
    POLLING_VALIDATION_TEXT,
)
from goe.offload.offload_transport_rdbms_api import (
    OFFLOAD_TRANSPORT_SQL_STATISTICS_TITLE,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT,
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_HASHAMP,
)
from goe.util.misc_functions import human_size_to_bytes

ORACLE_DIM_TABLE = "STORY_OTRANSP_DIM"
ORACLE_FACT_TABLE = "STORY_OTRANSP_FACT"
OFFLOAD_SPLIT_FACT = "STORY_SPLIT_TYPE_FACT"
OFFLOAD_NULLS = "STORY_OTRANSP_NULLS"
LPA_LARGE_NUMS = "STORY_LPA_LG_NUMS"
RPA_LARGE_NUMS = "STORY_RPA_LG_NUMS"
LOTS_NUMS = "12345678901234567890123456789012345678"

OFFLOAD_TRANSPORT_YARN_QUEUE_NAME = "default"


def gen_offload_nulls_create_ddl(
    schema, backend_api, frontend_api, options, to_allow_query_import
):
    if not options:
        return []
    ddl = [
        "DROP TABLE %(schema)s.%(table)s" % {"schema": schema, "table": OFFLOAD_NULLS}
    ]
    if options.db_type == DBTYPE_ORACLE:
        binary_float = (
            "\n, bfval binary_float"
            if backend_api and backend_api.canonical_float_supported()
            else ""
        )
        interval_ym = (
            "" if to_allow_query_import else "\n, iyval interval year(9) to month"
        )
        ddl.append(
            """CREATE TABLE %(schema)s.%(table)s
            ( id NUMBER
            , nmval NUMBER
            , vcval VARCHAR2(10)
            , chval CHAR(10)
            , dtval DATE
            , tmval TIMESTAMP%(float)s
            , bdval BINARY_DOUBLE%(interval_ym)s
            , idval INTERVAL DAY(9) TO SECOND(9))"""
            % {
                "schema": schema,
                "table": OFFLOAD_NULLS,
                "float": binary_float,
                "interval_ym": interval_ym,
            }
        )
        binary_float = (
            "\n,      CAST(NULL as BINARY_FLOAT)"
            if backend_api and backend_api.canonical_float_supported()
            else ""
        )
        interval_ym = (
            ""
            if to_allow_query_import
            else "\n,      CAST(NULL as interval year(9) to month)"
        )

        ddl.append(
            """INSERT INTO %(schema)s.%(table)s
            SELECT 1
            ,      CAST(NULL as NUMBER)
            ,      CAST(NULL as VARCHAR2(10))
            ,      CAST(NULL as CHAR(10))
            ,      CAST(NULL as DATE)
            ,      CAST(NULL as TIMESTAMP)%(float)s
            ,      CAST(NULL as BINARY_DOUBLE)%(interval_ym)s
            ,      CAST(NULL as interval day(9) to second(9))
            FROM   dual"""
            % {
                "schema": schema,
                "table": OFFLOAD_NULLS,
                "float": binary_float,
                "interval_ym": interval_ym,
            }
        )
    elif options.db_type == DBTYPE_TERADATA:
        binary_float = (
            "\n, bfval float"
            if backend_api and backend_api.canonical_float_supported()
            else ""
        )
        # TODO add interval types
        ddl.append(
            """CREATE TABLE %(schema)s.%(table)s
            ( id NUMBER
            , nmval NUMBER
            , vcval VARCHAR(10)
            , chval CHAR(10)
            , dtval DATE
            , tmval TIMESTAMP%(float)s
            , bdval DOUBLE PRECISION)"""
            % {"schema": schema, "table": OFFLOAD_NULLS, "float": binary_float}
        )
        binary_float = (
            "\n,      CAST(NULL AS FLOAT)"
            if backend_api and backend_api.canonical_float_supported()
            else ""
        )
        ddl.append(
            """INSERT INTO %(schema)s.%(table)s
            SELECT 1
            ,      CAST(NULL as NUMBER)
            ,      CAST(NULL as VARCHAR(10))
            ,      CAST(NULL as CHAR(10))
            ,      CAST(NULL as DATE)
            ,      CAST(NULL as TIMESTAMP)%(float)s
            ,      CAST(NULL as DOUBLE PRECISION)"""
            % {"schema": schema, "table": OFFLOAD_NULLS, "float": binary_float}
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {options.db_type}")
    ddl.append(
        dbms_stats_gather_string(schema, OFFLOAD_NULLS, frontend_api=frontend_api)
    )
    return ddl


def get_max_decimal_magnitude(backend_api):
    if backend_api:
        max_decimal_magnitude = backend_api.max_decimal_integral_magnitude()
        if backend_api.backend_type() == DBTYPE_BIGQUERY:
            # Bigquery can only partition by an 8-byte integer so further reduce part_col
            max_decimal_magnitude = 18
    else:
        max_decimal_magnitude = 38
    return max_decimal_magnitude


def gen_large_num_list_part_literal(backend_api, all_nines=False):
    if all_nines:
        return "9" * get_max_decimal_magnitude(backend_api)
    else:
        return LOTS_NUMS[: get_max_decimal_magnitude(backend_api)]


def goe1938_vulnerable_test(options):
    """Negative partition keys and Impala are an issue: GOE-1938"""
    return bool(options and options.target in [DBTYPE_HIVE, DBTYPE_IMPALA])


def gen_large_num_create_ddl(
    schema, table_name, options, backend_api, frontend_api, part_type="LIST"
):
    assert part_type in ["LIST", "RANGE"]
    if not options:
        return []
    if backend_api:
        max_decimal_precision = min(backend_api.max_decimal_precision(), 38)
        max_decimal_scale = backend_api.max_decimal_scale()
    else:
        max_decimal_precision, max_decimal_scale = 38, 18
    mid_decimal_scale = min(18, max_decimal_scale)
    num_data = (
        LOTS_NUMS[: max_decimal_precision - min(max_decimal_scale, 18)]
        + "."
        + LOTS_NUMS[:mid_decimal_scale]
    )
    tiny_data = "0." + LOTS_NUMS[:max_decimal_scale]
    params = {
        "schema": schema,
        "table": table_name,
        "part_col_type": "NUMBER(%s)" % max_decimal_precision,
        "num_data_type": "NUMBER(%s,%s)" % (max_decimal_precision, mid_decimal_scale),
        "tiny_data_type": "NUMBER(%s,%s)" % (max_decimal_precision, max_decimal_scale),
        "part_lit_1": "-" + gen_large_num_list_part_literal(backend_api),
        "part_lit_2": gen_large_num_list_part_literal(backend_api),
        "part_lit_3": gen_large_num_list_part_literal(backend_api, all_nines=True),
        # For RANGE we need to INSERT smaller values
        "part_lit_mod": "- 1" if part_type == "RANGE" else "",
        "num_data_lit_1": "-" + num_data,
        "num_data_lit_2": num_data,
        "tiny_data_lit_1": "-" + tiny_data,
        "tiny_data_lit_2": tiny_data,
    }
    if goe1938_vulnerable_test(options):
        # No negative partition keys due to GOE-1938
        params.update({"part_lit_1": "1"})

    ddl = ["""DROP TABLE %(schema)s.%(table)s""" % params]
    if options.db_type == DBTYPE_ORACLE:
        create_sql = """CREATE TABLE %(schema)s.%(table)s
                ( id NUMBER(8)
                , str_data NVARCHAR2(30)
                , num_data %(num_data_type)s
                , tiny_data %(tiny_data_type)s
                , part_col %(part_col_type)s)"""
        if part_type == "LIST":
            create_sql += """
                PARTITION BY LIST (part_col)
                ( PARTITION P_1 VALUES (%(part_lit_1)s)
                , PARTITION P_2 VALUES (%(part_lit_2)s)
                , PARTITION P_3 VALUES (%(part_lit_3)s))"""
        else:
            create_sql += """
                PARTITION BY RANGE (part_col)
                ( PARTITION P_1 VALUES LESS THAN (%(part_lit_1)s)
                , PARTITION P_2 VALUES LESS THAN (%(part_lit_2)s)
                , PARTITION P_3 VALUES LESS THAN (%(part_lit_3)s))"""
        ddl.append(create_sql % params)
        ddl.append(
            """INSERT INTO %(schema)s.%(table)s
               (id, str_data, num_data, tiny_data, part_col)
               SELECT ROWNUM,DBMS_RANDOM.STRING('u', 15)
               ,      CASE MOD(ROWNUM,2)
                      WHEN 0 THEN %(num_data_lit_1)s
                      ELSE %(num_data_lit_2)s
                      END AS num_data
               ,      CASE MOD(ROWNUM,2)
                      WHEN 0 THEN %(tiny_data_lit_1)s
                      ELSE %(tiny_data_lit_2)s
                      END AS tiny_data
               ,      CASE MOD(ROWNUM,3)
                      WHEN 0 THEN %(part_lit_1)s
                      WHEN 1 THEN %(part_lit_2)s
                      ELSE %(part_lit_3)s
                      END %(part_lit_mod)s AS part_col
               FROM   dual
               CONNECT BY ROWNUM <= 100"""
            % params
        )
    elif options.db_type == DBTYPE_TERADATA:
        if part_type == "LIST":
            raise NotImplementedError(
                f"LIST (CASE_N) pending implementation: {options.db_type}"
            )
        params["step"] = "1".ljust(get_max_decimal_magnitude(backend_api), "0")
        create_sql = """CREATE TABLE %(schema)s.%(table)s
                ( id NUMBER(8)
                , str_data VARCHAR(30)
                , num_data %(num_data_type)s
                , tiny_data %(tiny_data_type)s
                , part_col %(part_col_type)s)
                PRIMARY INDEX (id)
                PARTITION BY(
                    RANGE_N(part_col BETWEEN %(part_lit_1)s AND %(part_lit_3)s EACH %(step)s)
                ) """
        ddl.append(create_sql % params)
        ddl.append(
            """INSERT INTO %(schema)s.%(table)s
               (id, str_data, num_data, tiny_data, part_col)
               SELECT id,'blah'
               ,      CASE MOD(id,2)
                      WHEN 0 THEN %(num_data_lit_1)s
                      ELSE %(num_data_lit_2)s
                      END AS num_data
               ,      CASE MOD(id,2)
                      WHEN 0 THEN %(tiny_data_lit_1)s
                      ELSE %(tiny_data_lit_2)s
                      END AS tiny_data
               ,      CASE MOD(id,3)
                      WHEN 0 THEN %(part_lit_1)s
                      WHEN 1 THEN %(part_lit_2)s
                      ELSE %(part_lit_3)s
                      END %(part_lit_mod)s AS part_col
               FROM   %(schema)s.generated_ids
               WHERE id <= 100"""
            % params
        )
    else:
        raise NotImplementedError(f"Unsupported db_type: {options.db_type}")
    ddl.append(dbms_stats_gather_string(schema, table_name, frontend_api=frontend_api))
    return ddl


def add_to_sqoop_overrides(options, additional_value):
    assert isinstance(additional_value, str)
    if options and options.sqoop_overrides:
        return options.sqoop_overrides + " " + additional_value
    else:
        return additional_value


def offload_transport_story_tests(
    schema,
    hybrid_schema,
    data_db,
    load_db,
    options,
    backend_api,
    frontend_api,
    list_only,
    repo_client,
):
    fact_offload_transport = OFFLOAD_TRANSPORT_SQOOP
    fact_offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY

    if not list_only:
        if not options:
            return []
        messages = OffloadMessages.from_options(options, get_log_fh())
        dfs = get_dfs_from_options(options, messages=messages)

        if (
            is_spark_submit_available(options, None)
            or is_spark_thrift_available(options, None)
            or is_livy_available(options, None)
        ):
            fact_offload_transport = OFFLOAD_TRANSPORT_GLUENT
            fact_offload_transport_method = None

    oracle_dim_table_be = convert_backend_identifier_case(options, ORACLE_DIM_TABLE)

    if options and options.db_type == DBTYPE_TERADATA:
        fallback_splitter = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_TERADATA_HASHAMP
    else:
        fallback_splitter = TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_EXTENT

    partition_keys_larger_than_bigint_valid = bool(
        options and options.db_type != DBTYPE_TERADATA
    )

    return [
        {
            "id": "offload_transport_split_type_setup",
            "type": STORY_TYPE_SETUP,
            "title": "Create %s" % OFFLOAD_SPLIT_FACT,
            "setup": {
                STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_fact_create_ddl(
                    frontend_api,
                    schema,
                    OFFLOAD_SPLIT_FACT,
                    subpartitions=4,
                    noseg_partition=False,
                ),
                STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(
                    options, frontend_api, hybrid_schema, OFFLOAD_SPLIT_FACT
                ),
                STORY_SETUP_TYPE_PYTHON: [
                    lambda: drop_backend_test_table(
                        options, backend_api, data_db, OFFLOAD_SPLIT_FACT
                    )
                ],
            },
        },
        {
            "id": "offload_transport_split_type_fact1",
            "type": STORY_TYPE_OFFLOAD,
            "title": "Offload split by partition",
            "narrative": "Offload two partitions with parallelism of two to test that partition splitter is used",
            "options": {
                "owner_table": schema + "." + OFFLOAD_SPLIT_FACT,
                "offload_transport_method": fact_offload_transport_method,
                "offload_transport_parallelism": 2,
                "older_than_date": SALES_BASED_FACT_HV_2,
                "reset_backend_table": True,
            },
            "config_override": {"offload_transport": fact_offload_transport},
            "assertion_pairs": offload_fact_assertions(
                options,
                backend_api,
                frontend_api,
                repo_client,
                schema,
                hybrid_schema,
                data_db,
                OFFLOAD_SPLIT_FACT,
                SALES_BASED_FACT_HV_2,
                check_aggs=False,
                offload_pattern=OFFLOAD_PATTERN_90_10,
                story_id="offload_transport_split_type_fact1",
                split_type=TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_PARTITION,
            ),
        },
        {
            "id": "offload_transport_split_type_fact2",
            "type": STORY_TYPE_OFFLOAD,
            "title": "Offload split by subpartition",
            "narrative": "Offload two partitions with parallelism of four to test that subpartition splitter is used",
            "options": {
                "owner_table": schema + "." + OFFLOAD_SPLIT_FACT,
                "offload_transport_method": fact_offload_transport_method,
                "offload_transport_parallelism": 4,
                "older_than_date": SALES_BASED_FACT_HV_2,
                "reset_backend_table": True,
            },
            "config_override": {"offload_transport": fact_offload_transport},
            "assertion_pairs": offload_fact_assertions(
                options,
                backend_api,
                frontend_api,
                repo_client,
                schema,
                hybrid_schema,
                data_db,
                OFFLOAD_SPLIT_FACT,
                SALES_BASED_FACT_HV_2,
                check_aggs=False,
                offload_pattern=OFFLOAD_PATTERN_90_10,
                story_id="offload_transport_split_type_fact2",
                split_type=TRANSPORT_ROW_SOURCE_QUERY_SPLIT_BY_SUBPARTITION,
            ),
            # TODO subpartition logic pending implementation on Teradata
            "prereq": lambda: options.db_type != DBTYPE_TERADATA,
        },
        {
            "id": "offload_transport_split_type_fact3",
            "type": STORY_TYPE_OFFLOAD,
            "title": "Offload split by extents/rowid ranges on Oracle",
            "narrative": "Offload one partition with parallelism of eight to test that extents splitter is used",
            "options": {
                "owner_table": schema + "." + OFFLOAD_SPLIT_FACT,
                "offload_transport_method": fact_offload_transport_method,
                "offload_transport_parallelism": 8,
                "older_than_date": SALES_BASED_FACT_HV_1,
                "reset_backend_table": True,
            },
            "config_override": {"offload_transport": fact_offload_transport},
            "assertion_pairs": offload_fact_assertions(
                options,
                backend_api,
                frontend_api,
                repo_client,
                schema,
                hybrid_schema,
                data_db,
                OFFLOAD_SPLIT_FACT,
                SALES_BASED_FACT_HV_1,
                check_aggs=False,
                offload_pattern=OFFLOAD_PATTERN_90_10,
                story_id="offload_transport_split_type_fact3",
                split_type=fallback_splitter,
            ),
        },
        # {
        #    "id": "offload_transport_nulls_setup1",
        #    "type": STORY_TYPE_SETUP,
        #    "title": "Create a Table With NULLs of all types",
        #    "setup": {
        #        STORY_SETUP_TYPE_FRONTEND: lambda: gen_offload_nulls_create_ddl(
        #            schema,
        #            backend_api,
        #            frontend_api,
        #            options,
        #            to_allow_query_import=True,
        #        ),
        #        STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(
        #            options, frontend_api, hybrid_schema, OFFLOAD_NULLS
        #        ),
        #        STORY_SETUP_TYPE_PYTHON: [
        #            lambda: drop_backend_test_table(
        #                options, backend_api, data_db, OFFLOAD_NULLS
        #            )
        #        ],
        #    },
        # },
        # {
        #    "id": "offload_transport_nulls_offload1",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Query Import Offload %s" % OFFLOAD_NULLS,
        #    "options": {
        #        "owner_table": schema + "." + OFFLOAD_NULLS,
        #        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
        #        "allow_floating_point_conversions": True,
        #        "reset_backend_table": True,
        #    },
        #    "assertion_pairs": [
        #        (
        #            lambda test: (
        #                table_minus_row_count(
        #                    test,
        #                    test.name,
        #                    schema + "." + OFFLOAD_NULLS,
        #                    hybrid_schema + "." + OFFLOAD_NULLS,
        #                )
        #                if frontend_api.hybrid_schema_supported()
        #                else 0
        #            ),
        #            lambda test: 0,
        #        )
        #    ],
        #    "prereq": lambda: is_query_import_available(None, options),
        # },
        # {
        #    "id": "offload_transport_nulls_setup2",
        #    "type": STORY_TYPE_SETUP,
        #    "title": "Create a Table With NULLs of all types",
        #    "setup": {
        #        STORY_SETUP_TYPE_FRONTEND: lambda: gen_offload_nulls_create_ddl(
        #            schema,
        #            backend_api,
        #            frontend_api,
        #            options,
        #            to_allow_query_import=False,
        #        ),
        #        STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(
        #            options, frontend_api, hybrid_schema, OFFLOAD_NULLS
        #        ),
        #        STORY_SETUP_TYPE_PYTHON: [
        #            lambda: drop_backend_test_table(
        #                options, backend_api, data_db, OFFLOAD_NULLS
        #            )
        #        ],
        #    },
        # },
        # {
        #    "id": "offload_transport_nulls_offload2",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Query Import Offload %s" % OFFLOAD_NULLS,
        #    "options": {
        #        "owner_table": schema + "." + OFFLOAD_NULLS,
        #        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SQOOP,
        #        "allow_floating_point_conversions": True,
        #        "reset_backend_table": True,
        #    },
        #    "assertion_pairs": [
        #        (
        #            lambda test: (
        #                table_minus_row_count(
        #                    test,
        #                    test.name,
        #                    schema + "." + OFFLOAD_NULLS,
        #                    hybrid_schema + "." + OFFLOAD_NULLS,
        #                )
        #                if frontend_api.hybrid_schema_supported()
        #                else 0
        #            ),
        #            lambda test: 0,
        #        )
        #    ],
        #    "prereq": lambda: is_sqoop_available(None, options),
        # },
        # {
        #    "id": "offload_transport_nulls_offload3",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Query Import Offload %s" % OFFLOAD_NULLS,
        #    "options": {
        #        "owner_table": schema + "." + OFFLOAD_NULLS,
        #        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
        #        "allow_floating_point_conversions": True,
        #        "reset_backend_table": True,
        #    },
        #    "assertion_pairs": [
        #        (
        #            lambda test: (
        #                table_minus_row_count(
        #                    test,
        #                    test.name,
        #                    schema + "." + OFFLOAD_NULLS,
        #                    hybrid_schema + "." + OFFLOAD_NULLS,
        #                )
        #                if frontend_api.hybrid_schema_supported()
        #                else 0
        #            ),
        #            lambda test: 0,
        #        )
        #    ],
        #    "prereq": lambda: is_spark_submit_available(options, None),
        # },
        # {
        #    "id": "offload_transport_large_nums_lpa_setup",
        #    "type": STORY_TYPE_SETUP,
        #    "title": "Create %s" % LPA_LARGE_NUMS,
        #    "narrative": "Create a LIST table with extreme numeric partition values",
        #    "setup": {
        #        STORY_SETUP_TYPE_FRONTEND: lambda: gen_large_num_create_ddl(
        #            schema, LPA_LARGE_NUMS, options, backend_api, frontend_api
        #        ),
        #        STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(
        #            options, frontend_api, hybrid_schema, LPA_LARGE_NUMS
        #        ),
        #        STORY_SETUP_TYPE_PYTHON: [
        #            lambda: drop_backend_test_table(
        #                options, backend_api, data_db, LPA_LARGE_NUMS
        #            )
        #        ],
        #    },
        #    "prereq": lambda: frontend_api.gluent_lpa_supported()
        #    and partition_keys_larger_than_bigint_valid,
        # },
        # {
        #    "id": "offload_transport_large_nums_lpa_offload1",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Offload 1st Partition from %s" % LPA_LARGE_NUMS,
        #    "options": {
        #        "owner_table": "%s.%s" % (schema, LPA_LARGE_NUMS),
        #        "reset_backend_table": True,
        #        "partition_names_csv": "P_1",
        #        "offload_partition_granularity": "1".ljust(
        #            get_max_decimal_magnitude(backend_api), "0"
        #        ),
        #        "offload_partition_lower_value": "-"
        #        + "9".ljust(get_max_decimal_magnitude(backend_api), "9"),
        #        "offload_partition_upper_value": "9".ljust(
        #            get_max_decimal_magnitude(backend_api), "9"
        #        ),
        #    },
        #    "assertion_pairs": [
        #        (
        #            lambda test: offload_lpa_fact_assertion(
        #                test,
        #                schema,
        #                hybrid_schema,
        #                data_db,
        #                LPA_LARGE_NUMS,
        #                options,
        #                backend_api,
        #                frontend_api,
        #                repo_client,
        #                [
        #                    "1"
        #                    if goe1938_vulnerable_test(options)
        #                    else "-" + gen_large_num_list_part_literal(backend_api)
        #                ],
        #                check_rowcount=True,
        #                check_data=True,
        #                incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
        #            ),
        #            lambda test: True,
        #        )
        #    ],
        #    "prereq": lambda: frontend_api.gluent_lpa_supported()
        #    and partition_keys_larger_than_bigint_valid,
        # },
        # {
        #    "id": "offload_transport_large_nums_lpa_offload2",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Offload 2nd Partition from %s" % LPA_LARGE_NUMS,
        #    "options": {
        #        "owner_table": "%s.%s" % (schema, LPA_LARGE_NUMS),
        #        "partition_names_csv": "P_2",
        #    },
        #    "assertion_pairs": [
        #        (
        #            lambda test: offload_lpa_fact_assertion(
        #                test,
        #                schema,
        #                hybrid_schema,
        #                data_db,
        #                LPA_LARGE_NUMS,
        #                options,
        #                backend_api,
        #                frontend_api,
        #                repo_client,
        #                [
        #                    "1"
        #                    if goe1938_vulnerable_test(options)
        #                    else "-" + gen_large_num_list_part_literal(backend_api),
        #                    gen_large_num_list_part_literal(backend_api),
        #                ],
        #                check_rowcount=True,
        #                check_data=True,
        #                incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
        #            ),
        #            lambda test: True,
        #        )
        #    ],
        #    "prereq": lambda: frontend_api.gluent_lpa_supported()
        #    and partition_keys_larger_than_bigint_valid,
        # },
        # {
        #    "id": "offload_transport_large_nums_lpa_offload3",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Offload 2nd Partition from %s" % LPA_LARGE_NUMS,
        #    "options": {
        #        "owner_table": "%s.%s" % (schema, LPA_LARGE_NUMS),
        #        "partition_names_csv": "P_3",
        #    },
        #    "assertion_pairs": [
        #        (
        #            lambda test: offload_lpa_fact_assertion(
        #                test,
        #                schema,
        #                hybrid_schema,
        #                data_db,
        #                LPA_LARGE_NUMS,
        #                options,
        #                backend_api,
        #                frontend_api,
        #                repo_client,
        #                [
        #                    "1"
        #                    if goe1938_vulnerable_test(options)
        #                    else "-" + gen_large_num_list_part_literal(backend_api),
        #                    gen_large_num_list_part_literal(backend_api),
        #                    gen_large_num_list_part_literal(
        #                        backend_api, all_nines=True
        #                    ),
        #                ],
        #                check_rowcount=True,
        #                check_data=True,
        #                incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
        #            ),
        #            lambda test: True,
        #        )
        #    ],
        #    "prereq": lambda: frontend_api.gluent_lpa_supported()
        #    and partition_keys_larger_than_bigint_valid,
        # },
        # {
        #    "id": "offload_transport_large_nums_range_setup",
        #    "type": STORY_TYPE_SETUP,
        #    "title": "Create %s" % RPA_LARGE_NUMS,
        #    "narrative": "Create a RANGE table with extreme numeric partition values",
        #    "setup": {
        #        STORY_SETUP_TYPE_ORACLE: gen_large_num_create_ddl(
        #            schema,
        #            RPA_LARGE_NUMS,
        #            options,
        #            backend_api,
        #            frontend_api,
        #            part_type="RANGE",
        #        ),
        #        STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(
        #            options, frontend_api, hybrid_schema, RPA_LARGE_NUMS
        #        ),
        #        STORY_SETUP_TYPE_PYTHON: [
        #            lambda: drop_backend_test_table(
        #                options, backend_api, data_db, RPA_LARGE_NUMS
        #            )
        #        ],
        #    },
        #    "prereq": partition_keys_larger_than_bigint_valid,
        # },
        # {
        #    "id": "offload_transport_large_nums_range_offload1",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Offload 1st Partition from %s" % RPA_LARGE_NUMS,
        #    "options": {
        #        "owner_table": "%s.%s" % (schema, RPA_LARGE_NUMS),
        #        "reset_backend_table": True,
        #        "partition_names_csv": "P_1",
        #        "offload_partition_granularity": "1".ljust(
        #            get_max_decimal_magnitude(backend_api), "0"
        #        ),
        #        "offload_partition_lower_value": "-"
        #        + "9".ljust(get_max_decimal_magnitude(backend_api), "9"),
        #        "offload_partition_upper_value": "9".ljust(
        #            get_max_decimal_magnitude(backend_api), "9"
        #        ),
        #    },
        #    "assertion_pairs": offload_fact_assertions(
        #        options,
        #        backend_api,
        #        frontend_api,
        #        repo_client,
        #        schema,
        #        hybrid_schema,
        #        data_db,
        #        RPA_LARGE_NUMS,
        #        "1"
        #        if goe1938_vulnerable_test(options)
        #        else ("-" + gen_large_num_list_part_literal(backend_api)),
        #        offload_pattern=OFFLOAD_PATTERN_90_10,
        #        check_aggs=False,
        #        incremental_key="part_col",
        #    ),
        #    "prereq": partition_keys_larger_than_bigint_valid,
        # },
        # {
        #    "id": "offload_transport_large_nums_range_offload2",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Offload 2nd Partition from %s" % RPA_LARGE_NUMS,
        #    "options": {
        #        "owner_table": "%s.%s" % (schema, RPA_LARGE_NUMS),
        #        "partition_names_csv": "P_2",
        #    },
        #    "assertion_pairs": offload_fact_assertions(
        #        options,
        #        backend_api,
        #        frontend_api,
        #        repo_client,
        #        schema,
        #        hybrid_schema,
        #        data_db,
        #        RPA_LARGE_NUMS,
        #        gen_large_num_list_part_literal(backend_api),
        #        offload_pattern=OFFLOAD_PATTERN_90_10,
        #        check_aggs=False,
        #        incremental_key="part_col",
        #    ),
        #    "prereq": partition_keys_larger_than_bigint_valid,
        # },
        # {
        #    "id": "offload_transport_large_nums_range_offload3",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Offload 3rd Partition from %s" % RPA_LARGE_NUMS,
        #    "options": {
        #        "owner_table": "%s.%s" % (schema, RPA_LARGE_NUMS),
        #        "partition_names_csv": "P_3",
        #    },
        #    "assertion_pairs": offload_fact_assertions(
        #        options,
        #        backend_api,
        #        frontend_api,
        #        repo_client,
        #        schema,
        #        hybrid_schema,
        #        data_db,
        #        RPA_LARGE_NUMS,
        #        gen_large_num_list_part_literal(backend_api, all_nines=True),
        #        offload_pattern=OFFLOAD_PATTERN_90_10,
        #        check_aggs=False,
        #        incremental_key="part_col",
        #    ),
        #    "prereq": partition_keys_larger_than_bigint_valid,
        # },
        # {
        #    "id": "offload_transport_use_polling_validation_spark_submit_on",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Offload with Spark submit transport method and use polling validation for transported rows",
        #    "options": {
        #        "owner_table": schema + "." + ORACLE_DIM_TABLE,
        #        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
        #        "offload_transport_validation_polling_interval": 1,
        #        "reset_backend_table": True,
        #    },
        #    "assertion_pairs": [
        #        (
        #            lambda test: text_in_log(
        #                POLLING_VALIDATION_TEXT % "OffloadTransportSqlStatsThread",
        #                "(%s)"
        #                % "offload_transport_use_polling_validation_spark_submit_on",
        #            ),
        #            lambda test: True,
        #        )
        #    ],
        #    # TODO Offload transport SQL stats polling is not applicable on Teradata
        #    "prereq": lambda: is_spark_submit_available(options, None)
        #    and options.db_type != DBTYPE_TERADATA,
        # },
        # {
        #    "id": "offload_transport_use_polling_validation_spark_submit_off",
        #    "type": STORY_TYPE_OFFLOAD,
        #    "title": "Offload with Spark submit transport method with disabled SQL stats validation",
        #    "narrative": "Even with SQL stats validation disabled we should be able to get the count from Spark logs",
        #    "options": {
        #        "owner_table": schema + "." + ORACLE_DIM_TABLE,
        #        "offload_transport_method": OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
        #        "offload_transport_validation_polling_interval": OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
        #        "reset_backend_table": True,
        #    },
        #    "prereq": lambda: is_spark_submit_available(options, None),
        #    "assertion_pairs": [
        #        (
        #            lambda test: text_in_log(
        #                OFFLOAD_TRANSPORT_SQL_STATISTICS_TITLE,
        #                "(%s)"
        #                % "offload_transport_use_polling_validation_spark_submit_off",
        #            ),
        #            lambda test: False,
        #        ),
        #        (
        #            lambda test: text_in_log(
        #                MISSING_ROWS_IMPORTED_WARNING,
        #                "(%s)"
        #                % "offload_transport_use_polling_validation_spark_submit_off",
        #            ),
        #            lambda test: False,
        #        ),
        #    ],
        # },
    ]
