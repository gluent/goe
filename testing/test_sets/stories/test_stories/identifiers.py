""" Tests relating to frontend/backend system identifiers such as:
        Incompatible table names
        Incompatible table or columns lengths
        Backend table name case (upper/lower/camel)
"""

from gluent import (
    ADJUSTED_BACKEND_IDENTIFIER_MESSAGE_TEXT,
    LEGACY_MAX_HYBRID_IDENTIFIER_LENGTH,
    log,
)
from gluentlib.offload.offload_constants import DBTYPE_IMPALA, DBTYPE_ORACLE, DBTYPE_TERADATA
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_metadata_functions import INCREMENTAL_PREDICATE_TYPE_LIST
from gluentlib.offload.offload_transport import (
    OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
    is_query_import_available,
    is_spark_gcloud_dataproc_available,
    is_spark_submit_available,
    is_spark_thrift_available,
    is_sqoop_available,
    is_sqoop_by_query_available,
)
from gluentlib.offload.oracle.oracle_column import ORACLE_TYPE_DATE
from gluentlib.offload.predicate_offload import GenericPredicate

from test_sets.stories.story_assertion_functions import (
    backend_column_exists,
    offload_dim_assertion,
    offload_fact_assertions,
    offload_lpa_fact_assertion,
    text_in_log,
)
from test_sets.stories.story_globals import (
    STORY_SETUP_TYPE_FRONTEND,
    STORY_SETUP_TYPE_HYBRID,
    STORY_SETUP_TYPE_ORACLE,
    STORY_SETUP_TYPE_PYTHON,
    STORY_TYPE_AGG_VALIDATE,
    STORY_TYPE_OFFLOAD,
    STORY_TYPE_SCHEMA_SYNC,
    STORY_TYPE_SETUP,
)
from test_sets.stories.story_setup_functions import (
    LOWER_YRMON_NUM,
    SALES_BASED_FACT_HV_1,
    SALES_BASED_FACT_HV_2,
    SALES_BASED_FACT_HV_3,
    SALES_BASED_LIST_HV_1,
    SALES_BASED_LIST_HV_3,
    UPPER_YRMON_NUM,
    dbms_stats_gather_string,
    drop_backend_test_table,
    gen_hybrid_drop_ddl,
    gen_max_length_identifier,
    gen_rdbms_dim_create_ddl,
    gen_sales_based_fact_create_ddl,
    gen_sales_based_list_create_ddl,
)

RPA_TABLE = 'STORY_LONG_RPA'
LPA_TABLE = 'STORY_LONG_LPA'
PBO_TABLE = 'STORY_LONG_PBO'
SS_TABLE = 'STORY_LONG_SS'
CASE_DIM = 'STORY_CASE_DIM'
KEYWORD_COL_TABLE = 'STORY_KEYWORD_COLS'
BAD_CHAR_COL_TABLE = 'STORY_BAD_COLS'
BAD_CHAR_PART_COL_TABLE = 'STORY_BAD_PART_COL'


def gen_offload_story_bad_name_test(story_id, title, schema, table_name, transport_method, check_fn, extra_opts=None):
    offload_options = {
        'owner_table': schema + '.' + table_name,
        'offload_transport_method': transport_method,
        'reset_backend_table': True,
        'offload_partition_lower_value': 2000,
        'offload_partition_upper_value': 2100
    }
    if extra_opts:
        offload_options.update(extra_opts or {})
    return {
        'id': story_id,
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Offload (%s) Of Table With Dodgy Column Names' % title,
        'narrative': 'Offload (%s) of table with dodgy column names. No assertions, just checking it runs to completion'
                     % title,
        'options': offload_options,
        'prereq': lambda: check_fn()}


def backend_case_offload_assertion(search_token: str, test_id: str) -> bool:
    search_string = f"{ADJUSTED_BACKEND_IDENTIFIER_MESSAGE_TEXT}: {search_token}"
    result = text_in_log(search_string, f'({test_id})')
    if not result:
        log(f'Search string "{search_string}" not found in log (beyond token: "{test_id}")')
    return result


def gen_keyword_col_table_ddl(schema, table_name, options, frontend_api) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        subquery = """SELECT TRUNC(SYSDATE) AS "DATE"
        ,      CAST('ABC' AS VARCHAR2(5))   AS "SELECT"
        FROM   dual"""
    elif options.db_type == DBTYPE_TERADATA:
        subquery = """SELECT CURRENT_DATE AS "DATE"
        ,      CAST('ABC' AS VARCHAR(5))  AS "SELECT" """
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery, with_stats_collection=True)


def gen_bad_char_col_table_ddl(schema, table_name, options, frontend_api) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        subquery = """SELECT CAST(1 AS NUMBER(1))       AS "ID"
        ,      CAST(123 AS NUMBER(5))       AS "COL SPACE"
        ,      CAST(123 AS BINARY_DOUBLE)   AS "COL-HYPHEN"
        ,      CAST(123 AS NUMBER(5))       AS "COL#HASH"
        FROM   dual"""
    elif options.db_type == DBTYPE_TERADATA:
        subquery = """SELECT CAST(1 AS NUMBER(1))        AS "ID"
        ,      CAST(123 AS NUMBER(5))        AS "COL SPACE"
        ,      CAST(123 AS DOUBLE PRECISION) AS "COL-HYPHEN"
        ,      CAST(123 AS NUMBER(5))        AS "COL#HASH" """
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery, with_stats_collection=True)


def gen_bad_char_part_col_table_ddl(schema, table_name, options, frontend_api) -> list:
    if not options:
        return []
    params = {'schema': schema,
              'table': table_name}
    if options.db_type == DBTYPE_ORACLE:
        return [
            'DROP TABLE %(schema)s.%(table)s' % params,
            """CREATE TABLE %(schema)s.%(table)s PARTITION BY RANGE ("PCOL 1","PCOL 2")
               (PARTITION %(table)s_P1 VALUES LESS THAN (2012,01)
               ,PARTITION %(table)s_P2 VALUES LESS THAN (2013,01)
               ,PARTITION %(table)s_P3 VALUES LESS THAN (2014,01))
               AS
               SELECT CAST(ROWNUM AS NUMBER(4))               AS id
               ,      CAST(2011 + MOD(ROWNUM,3) AS NUMBER(4)) AS "PCOL 1"
               ,      CAST(1 + MOD(ROWNUM,12) AS NUMBER(2))   AS "PCOL 2"
               ,      'hello world'                           AS data
               FROM   dual
               CONNECT BY ROWNUM <= 100
               """ % params,
            dbms_stats_gather_string(schema, table_name, frontend_api=frontend_api)
        ]
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')


def identifier_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api, repo_client):
    """ Tests relating to table/column name identifiers, both frontend and backend. Examples:
            - Frontend tables/columns with names longer than backend max
            - Backend tables/columns with names longer than frontend max
            - Backend columns with reserved words as names
            - Upper/lower/camel case table and column names
    """
    # Variables for long table name tests
    max_hybrid_length = frontend_api.max_table_name_length() if frontend_api else LEGACY_MAX_HYBRID_IDENTIFIER_LENGTH
    frontend_long_table_name = gen_max_length_identifier('story_long_fe_name', max_hybrid_length)
    frontend_long_table_name_be = convert_backend_identifier_case(options, frontend_long_table_name)
    # Variables for long column name tests
    rpa_table = gen_max_length_identifier(RPA_TABLE, max_hybrid_length)
    rpa_table_be = convert_backend_identifier_case(options, rpa_table)
    lpa_table = gen_max_length_identifier(LPA_TABLE, max_hybrid_length)
    lpa_table_be = convert_backend_identifier_case(options, lpa_table)
    pbo_table = gen_max_length_identifier(PBO_TABLE, max_hybrid_length)
    pbo_table_be = convert_backend_identifier_case(options, pbo_table)
    ss_table = gen_max_length_identifier(SS_TABLE, max_hybrid_length)
    ss_table_be = convert_backend_identifier_case(options, ss_table)
    frontend_long_col_name = gen_max_length_identifier('this_column_name_is_long', max_hybrid_length)
    # Limit partition columns to 100 characters in length to simplify synthetic part col processing.
    # Because we derive the source column name from the GL_PART name we cannot hash it without more metadata.
    # It's very unlikely a customer will partition a table by a column with a name > 100 characters.
    max_pcol_length = min(max_hybrid_length, 100)
    long_pcol_name = gen_max_length_identifier('this_part_column_name_is_long', max_pcol_length)

    return [
        #
        # BAD COLUMN NAME TESTS
        #
        {'id': 'identifiers_keyword_col_name_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % KEYWORD_COL_TABLE,
         'narrative': 'Used to confirm that a table with a column name of a reserved word can still be offloaded',
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_keyword_col_table_ddl(schema, KEYWORD_COL_TABLE,
                                                                        options, frontend_api),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, KEYWORD_COL_TABLE)],
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, KEYWORD_COL_TABLE)}},
        gen_offload_story_bad_name_test('identifiers_keyword_col_name_query_import', 'Query Import', schema,
                                        KEYWORD_COL_TABLE, OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
                                        lambda: is_query_import_available(None, options)),
        gen_offload_story_bad_name_test('identifiers_keyword_col_name_sqoop1', 'Sqoop By Query', schema,
                                        KEYWORD_COL_TABLE, OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
                                        lambda: is_sqoop_by_query_available(options)),
        gen_offload_story_bad_name_test('identifiers_keyword_col_name_sqoop2', 'Sqoop', schema,
                                        KEYWORD_COL_TABLE, OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
                                        lambda: is_sqoop_available(None, options)),
        gen_offload_story_bad_name_test('identifiers_keyword_col_name_spark_submit', 'Spark Submit', schema,
                                        KEYWORD_COL_TABLE, OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
                                        lambda: is_spark_submit_available(options, None)),
        gen_offload_story_bad_name_test('identifiers_keyword_col_name_spark_thrift', 'Spark Thrift', schema,
                                        KEYWORD_COL_TABLE, OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
                                        lambda: is_spark_thrift_available(options, None)),
        # AVRO and PARQUET do not support non-standard characters in their column names.
        # In the future we should decouple staging column names from actual names to work around this restriction.
        {'id': 'identifiers_bad_char_col_name_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % BAD_CHAR_COL_TABLE,
         'narrative': 'Used to confirm that a table with a column name containing space or hyphen can be offloaded',
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_bad_char_col_table_ddl(schema, BAD_CHAR_COL_TABLE,
                                                                         options, frontend_api),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, BAD_CHAR_COL_TABLE)],
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, BAD_CHAR_COL_TABLE)},
         'prereq': lambda: bool(options and options.target != DBTYPE_IMPALA)},
        gen_offload_story_bad_name_test('identifiers_bad_char_col_name_query_import', 'Query Import', schema,
                                        BAD_CHAR_COL_TABLE, OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
                                        lambda: (is_query_import_available(None, options)
                                                 and bool(options and options.target != DBTYPE_IMPALA))),
        gen_offload_story_bad_name_test('identifiers_bad_char_col_name_spark_submit', 'Spark Submit', schema,
                                        BAD_CHAR_COL_TABLE, OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
                                        lambda: (is_spark_submit_available(options, None)
                                                 and bool(options and options.target != DBTYPE_IMPALA))),
        {'id': 'identifiers_bad_char_part_col_name_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % BAD_CHAR_PART_COL_TABLE,
         'narrative': 'Used to confirm that a table with a partition column name containing space can be offloaded',
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_bad_char_part_col_table_ddl(schema, BAD_CHAR_PART_COL_TABLE,
                                                                              options, frontend_api),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, BAD_CHAR_PART_COL_TABLE)],
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema,
                                                                BAD_CHAR_PART_COL_TABLE)},
         'prereq': lambda: bool(options and options.target != DBTYPE_IMPALA)},
        gen_offload_story_bad_name_test('identifiers_bad_char_part_col_name_spark_submit', 'Spark Submit', schema,
                                        BAD_CHAR_PART_COL_TABLE, OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
                                        lambda: (is_spark_submit_available(options, None)
                                                 and bool(options and options.target != DBTYPE_IMPALA)),
                                        extra_opts={'offload_partition_columns': 'pcol 1',
                                                    'offload_partition_granularity': "1"}),
        gen_offload_story_bad_name_test('identifiers_bad_char_part_col_name_spark_dataproc', 'Dataproc', schema,
                                        BAD_CHAR_PART_COL_TABLE, OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
                                        lambda: (is_spark_gcloud_dataproc_available(options, None)
                                                 and bool(options and options.target != DBTYPE_IMPALA)),
                                        extra_opts={'offload_partition_columns': 'pcol 1',
                                                    'offload_partition_granularity': "1"}),
        #
        # ASSORTED CASE BACKEND TABLE NAME TESTS
        #
        {'id': 'identifiers_backend_table_case_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % CASE_DIM,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, CASE_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, CASE_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, CASE_DIM)]}},
        {'id': 'identifiers_backend_table_case_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Lower Case Backend Table',
         'narrative': """Offload the dimension checking log that we attempted to create in correct case.
                         Can't actually run it because it would apply to db name too.""",
         'options': {'owner_table': schema + '.' + CASE_DIM,
                     'reset_backend_table': True},
         'config_override': {'execute': False,
                             'backend_identifier_case': 'LOWER'},
         'assertion_pairs': [(lambda test: backend_case_offload_assertion(f'{data_db}.{CASE_DIM}'.lower(),
                                                                          'identifiers_backend_table_case_offload1'),
                              lambda test: True)],
         'prereq': lambda: backend_api.case_sensitive_identifiers()},
        {'id': 'identifiers_backend_table_case_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Upper Case Backend Table',
         'narrative': """Offload the dimension checking log that we attempted to create in correct case.
                         Can't actually run it because it would apply to db name too.""",
         'options': {'owner_table': schema + '.' + CASE_DIM,
                     'reset_backend_table': True},
         'config_override': {'execute': False,
                             'backend_identifier_case': 'UPPER'},
         'assertion_pairs': [(lambda test: backend_case_offload_assertion(f'{data_db}.{CASE_DIM}'.upper(),
                                                                          'identifiers_backend_table_case_offload2'),
                              lambda test: True)],
         'prereq': lambda: backend_api.case_sensitive_identifiers()},
        {'id': 'identifiers_backend_table_case_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Mixed Case Backend Table',
         'narrative': """Offload the dimension checking log that we attempted to create in correct case.
                         Can't actually run it because it would apply to db name too.""",
         'options': {'owner_table': schema.upper() + '.' + CASE_DIM.capitalize(),
                     'reset_backend_table': True},
         'config_override': {'execute': False,
                             'backend_identifier_case': 'NO_MODIFY'},
         'assertion_pairs': [(lambda test: backend_case_offload_assertion(f'{data_db.upper()}.{CASE_DIM.capitalize()}',
                                                                          'identifiers_backend_table_case_offload3'),
                              lambda test: True)],
         'prereq': lambda: backend_api.case_sensitive_identifiers()},
        #
        # LONG TABLE NAME TESTS
        #
        {'id': 'identifiers_frontend_long_table_name_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Frontend Table With Really Long Name',
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_rdbms_dim_create_ddl(frontend_api, schema, frontend_long_table_name),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema,
                                                                frontend_long_table_name),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, frontend_long_table_name_be)]}},
        {'id': 'identifiers_frontend_long_table_name_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Table With Really Long Name',
         'narrative': 'Check that extended names (e.g. external tables, agg views and rewrite rules) have no issues',
         'options': {'owner_table': schema + '.' + frontend_long_table_name,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema,
                 hybrid_schema, data_db, frontend_long_table_name, check_aggs=True),
              lambda test: True)]},
        {'id': 'identifiers_frontend_long_table_name_validate',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate Over Long Table Name',
         'options': {'owner_table': schema + '.' + frontend_long_table_name}},
        #
        # EXTENDED LONG TABLE AND COLUMN NAME TESTS
        # Going into more detail, e.g. incremental offloads, predicate offloads etc
        #
        {'id': 'identifiers_frontend_long_rpa_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Frontend Fact With Really Long Column Name',
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       gen_sales_based_fact_create_ddl(frontend_api, schema, rpa_table,
                                                       time_id_column_name=long_pcol_name,
                                                       extra_col_tuples=[(123, frontend_long_col_name)],
                                                       simple_partition_names=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, rpa_table),
                   STORY_SETUP_TYPE_PYTHON: [
                       lambda: drop_backend_test_table(options, backend_api, data_db, rpa_table_be)
                   ]}},
        {'id': 'identifiers_frontend_long_rpa_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st Partition',
         'narrative': """Partition, sort by and use data type control options with long column names.""",
         'options': {'owner_table': schema + '.' + rpa_table,
                     'older_than_date': SALES_BASED_FACT_HV_1,
                     'reset_backend_table': True,
                     'date_columns_csv': long_pcol_name,
                     'sort_columns_csv': frontend_long_col_name,
                     'integer_8_columns_csv': frontend_long_col_name},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, rpa_table, SALES_BASED_FACT_HV_1,
                                                    incremental_key=long_pcol_name,
                                                    incremental_key_type=ORACLE_TYPE_DATE)},
        {'id': 'identifiers_frontend_long_rpa_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 2nd Partition',
         'options': {'owner_table': schema + '.' + rpa_table,
                     'older_than_date': SALES_BASED_FACT_HV_2},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, rpa_table, SALES_BASED_FACT_HV_2,
                                                    incremental_key=long_pcol_name,
                                                    incremental_key_type=ORACLE_TYPE_DATE)},
        {'id': 'identifiers_frontend_long_rpa_validate',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate Over Long Table Name',
         'options': {'owner_table': schema + '.' + rpa_table}},
        {'id': 'identifiers_frontend_long_lpa_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Frontend Fact With Really Long Column Name',
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       gen_sales_based_list_create_ddl(frontend_api, schema, lpa_table,
                                                       yrmon_column_name=long_pcol_name,
                                                       extra_col_tuples=[(123, frontend_long_col_name)]),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, lpa_table),
                   STORY_SETUP_TYPE_PYTHON: [
                       lambda: drop_backend_test_table(options, backend_api, data_db, lpa_table_be)
                   ]},
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'identifiers_frontend_long_lpa_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st LIST Partition',
         'options': {'owner_table': '%s.%s' % (schema, lpa_table),
                     'equal_to_values': [SALES_BASED_LIST_HV_1],
                     'offload_partition_lower_value': LOWER_YRMON_NUM,
                     'offload_partition_upper_value': UPPER_YRMON_NUM,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(test, schema, hybrid_schema, data_db, lpa_table,
                                                      options, backend_api, frontend_api, repo_client,
                                                      [SALES_BASED_LIST_HV_1], check_rowcount=True,
                                                      incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST),
              lambda test: True)],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'identifiers_frontend_long_lpa_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 2nd LIST Partition',
         'options': {'owner_table': '%s.%s' % (schema, lpa_table),
                     'equal_to_values': [SALES_BASED_LIST_HV_3]},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(test, schema, hybrid_schema, data_db, lpa_table,
                                                      options, backend_api, frontend_api, repo_client,
                                                      [SALES_BASED_LIST_HV_1, SALES_BASED_LIST_HV_3],
                                                      check_rowcount=True,
                                                      incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST),
              lambda test: True)],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'identifiers_frontend_long_pbo_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % pbo_table,
         'narrative': 'Create dimension for PBO testing, gen_rdbms_dim_create_ddl table is based on PRODUCTS',
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_rdbms_dim_create_ddl(frontend_api, schema, pbo_table,
                                                                       pk_col='prod_id',
                                                                       extra_col_tuples=[(123, frontend_long_col_name)]),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, pbo_table),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, pbo_table_be)]}},
        {'id': 'identifiers_frontend_long_pbo_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'PBO Offload %s' % pbo_table,
         'narrative': 'Offload 1st string predicate of dimension',
         'options': {'owner_table': schema + '.' + pbo_table,
                     'offload_predicate': GenericPredicate('column(%s) = numeric(123)' % frontend_long_col_name),
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, pbo_table),
              lambda test: True),
         ]},
        {'id': 'identifiers_frontend_long_rpa_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 3rd Partition',
         'options': {'owner_table': schema + '.' + rpa_table,
                     'older_than_date': SALES_BASED_FACT_HV_3},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, rpa_table, SALES_BASED_FACT_HV_3,
                                                    incremental_key=long_pcol_name,
                                                    incremental_key_type=ORACLE_TYPE_DATE)},
        {'id': 'identifiers_frontend_long_schema_sync_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % ss_table,
         'narrative': 'Create dimension for Schema Sync testing',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, ss_table, pk_col='prod_id'),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, ss_table),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, ss_table_be)]},
         'prereq': lambda: backend_api.schema_evolution_supported() and frontend_api.gluent_schema_sync_supported()},
        {'id': 'identifiers_frontend_long_schema_sync_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Pre Schema Sync Offload %s' % ss_table,
         'options': {'owner_table': schema + '.' + ss_table,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, ss_table),
              lambda test: True)],
         'prereq': lambda: backend_api.schema_evolution_supported() and frontend_api.gluent_schema_sync_supported()},
        {'id': 'identifiers_frontend_long_schema_sync_add_cols',
         'type': STORY_TYPE_SETUP,
         'title': 'Add Long Column to %s' % ss_table,
         'setup': {STORY_SETUP_TYPE_ORACLE: ['ALTER TABLE %(schema)s.%(table)s ADD ( %(col)s NUMBER(1) )'
                                             % {'schema': schema, 'table': ss_table, 'col': frontend_long_col_name}]},
         'prereq': lambda: backend_api.schema_evolution_supported() and frontend_api.gluent_schema_sync_supported()},
        {'id': 'identifiers_frontend_long_schema_sync_run',
         'type': STORY_TYPE_SCHEMA_SYNC,
         'title': 'Run Schema Sync Adding Long Column to Long Table',
         'options': {'include': schema + '.' + ss_table,
                     'command_file': None},
         'assertion_pairs': [
             (lambda test: backend_column_exists(backend_api, data_db, ss_table_be, frontend_long_col_name),
              lambda test: True)],
         'prereq': lambda: backend_api.schema_evolution_supported() and frontend_api.gluent_schema_sync_supported()},
    ]
