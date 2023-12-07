""" Tests relating to synthetic bucketing and/or hash bucket column.
"""
from test_sets.stories.story_globals import STORY_SETUP_TYPE_FRONTEND, STORY_SETUP_TYPE_HYBRID, \
    STORY_SETUP_TYPE_PYTHON, STORY_SETUP_TYPE_ORACLE, STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import drop_backend_test_table, drop_backend_test_load_table,\
    gen_hybrid_drop_ddl, gen_rdbms_dim_create_ddl, dbms_stats_gather_string
from test_sets.stories.story_assertion_functions import offload_dim_assertion

from gluentlib.offload.offload_constants import DBTYPE_ORACLE, DBTYPE_SYNAPSE, DBTYPE_TERADATA
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from testlib.test_framework.test_functions import log


BUCKET_DIM = 'STORY_BKT_DIM'
DATATYPES_DIM = 'STORY_BKT_DATATYPES'


def synapse_distribution_assertion(backend_api, data_db, backend_name, expected_distribution):
    if not backend_api:
        return True
    table_distribution = backend_api.table_distribution(data_db, backend_name)
    if table_distribution != expected_distribution:
        log(f'table_distribution({data_db}, {backend_name}) {table_distribution} != {expected_distribution}')
        return False
    return True


def gen_bucket_types_table_ddl(schema, table_name, options, frontend_api) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        subquery = """SELECT TRUNC(SYSDATE) AS date_dim
        ,      CAST(TRUNC(SYSDATE)          AS TIMESTAMP(0)) AS timestamp_dim
        ,      CAST(1.101 AS NUMBER)        AS number_dim
        ,      CAST(100 AS NUMBER(10))      AS whole_num_dim
        ,      CAST('ABC' AS VARCHAR2(5))   AS varchar_dim
        ,      CAST('ABC' AS CHAR(3))       AS char_dim
        ,      CAST('ABC' AS NVARCHAR2(5))  AS nvarchar2_dim
        ,      CAST('ABC' AS NCHAR(3))      AS nchar_dim
        FROM   dual"""
    elif options.db_type == DBTYPE_TERADATA:
        subquery = """SELECT CURRENT_DATE    AS date_dim
        ,      CAST(TRUNC(CURRENT_TIMESTAMP) AS TIMESTAMP(0)) AS timestamp_dim
        ,      CAST(1.101 AS NUMBER)         AS number_dim
        ,      CAST(100 AS NUMBER(10))       AS whole_num_dim
        ,      CAST('ABC' AS VARCHAR(5))     AS varchar_dim
        ,      CAST('ABC' AS CHAR(3))        AS char_dim"""
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery, with_stats_collection=True)


def offload_hash_bucket_story_tests(schema, hybrid_schema, data_db, load_db, options,
                                    backend_api, frontend_api, repo_client):
    """ Tests relating to synthetic bucketing and/or hash bucket column.
    """
    if backend_api and not backend_api.synthetic_bucketing_supported() and not backend_api.bucket_hash_column_supported():
        if not backend_api.synthetic_bucketing_supported():
            log('Skipping offload_hash_bucket_story_tests() because synthetic_bucketing_supported() == False')
            return []
        if not backend_api.bucket_hash_column_supported():
            log('Skipping offload_hash_bucket_story_tests() because bucket_hash_column_supported() == False')
            return []

    bucket_dim_be = convert_backend_identifier_case(options, BUCKET_DIM)
    datatypes_dim_be = convert_backend_identifier_case(options, DATATYPES_DIM)
    return [
        {'id': 'offload_hash_bucket_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % BUCKET_DIM,
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_rdbms_dim_create_ddl(frontend_api, schema, BUCKET_DIM,
                                                                       pk_col='PROD_ID'),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, BUCKET_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, BUCKET_DIM),
                                             lambda: drop_backend_test_load_table(options, backend_api,
                                                                                  load_db, BUCKET_DIM)]}},
        {'id': 'offload_hash_bucket_auto_buckets1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with Auto Buckets',
         'narrative': 'Offload the dimension checking that --num-buckets is reduced to 1',
         'options': {'owner_table': schema + '.' + BUCKET_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'prod_id',
                     'num_buckets': 'auto',
                     'num_location_files': 8},
         'config_override': {'num_buckets_max': 4,
                             'num_buckets_threshold': '10g'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, BUCKET_DIM, bucket_check=1, check_aggs=True,
                 bucket_col='PROD_ID', bucket_method='GLUENT_BUCKET', num_loc_files=8),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported()},
        {'id': 'offload_hash_bucket_auto_buckets2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with Auto Buckets',
         'narrative': 'Offload the dimension checking that --num-buckets is NOT reduced',
         'options': {'owner_table': schema + '.' + BUCKET_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'prod_id',
                     'num_buckets': 'auto',
                     'num_location_files': 16},
         'config_override': {'num_buckets_max': 4,
                             'num_buckets_threshold': '0.1k'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, BUCKET_DIM, bucket_check=4, check_aggs=True, bucket_col='PROD_ID',
                 bucket_method='GLUENT_BUCKET', num_loc_files=16),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported()},
        # Synapse tests ensuring this comment from design note remains true:
        #   Sep 7, 2021 Tables below default bucket threshold size will default to ROUND_ROBIN, else HASH.
        #   HASH keys will be chosen from hash bucket key option or automatically in order of PK, stats and fallback .
        {'id': 'offload_hash_bucket_synapse_distribution1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension Expecting ROUND_ROBIN Distribution',
         'narrative': 'Offload the dimension without --bucket-hash-column and high threshold, expect ROUND_ROBIN',
         'options': {'owner_table': schema + '.' + BUCKET_DIM,
                     'reset_backend_table': True},
         'config_override': {'num_buckets_threshold': '10g'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, BUCKET_DIM, check_aggs=True, bucket_col='NULL'),
              lambda test: True),
             (lambda test: synapse_distribution_assertion(backend_api, data_db, bucket_dim_be, 'ROUND_ROBIN'),
              lambda test: True)
         ],
         'prereq': lambda: options.target == DBTYPE_SYNAPSE},
        {'id': 'offload_hash_bucket_synapse_distribution2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension Forcing HASH Distribution',
         'narrative': 'Offload the dimension with --bucket-hash-column and high threshold, expect HASH',
         'options': {'owner_table': schema + '.' + BUCKET_DIM,
                     'bucket_hash_col': 'prod_id',
                     'reset_backend_table': True},
         'config_override': {'num_buckets_threshold': '10g'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, BUCKET_DIM, check_aggs=True, bucket_col='PROD_ID'),
              lambda test: True),
             (lambda test: synapse_distribution_assertion(backend_api, data_db, bucket_dim_be, 'HASH'),
              lambda test: True)
         ],
         'prereq': lambda: options.target == DBTYPE_SYNAPSE},
        {'id': 'offload_hash_bucket_synapse_distribution3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension Expecting HASH Distribution',
         'narrative': 'Offload the dimension without --bucket-hash-column and low threshold, expect HASH',
         'options': {'owner_table': schema + '.' + BUCKET_DIM,
                     'reset_backend_table': True},
         'config_override': {'num_buckets_threshold': '0.1k'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, BUCKET_DIM, check_aggs=True, bucket_col='PROD_ID'),
              lambda test: True),
             (lambda test: synapse_distribution_assertion(backend_api, data_db, bucket_dim_be, 'HASH'),
              lambda test: True)
         ],
         'prereq': lambda: options.target == DBTYPE_SYNAPSE},
        # Add tests for hash bucket offload w different datatypes as bucket
        # NUMBER, DATE, TIMESTAMP(0), VARCHAR2, CHAR, NVARCHAR2, NCHAR
        {'id': 'offload_hash_bucket_types_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create table for testing various hash bucket datatypes',
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_bucket_types_table_ddl(schema, DATATYPES_DIM, options, frontend_api),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DATATYPES_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, datatypes_dim_be)]},
         'prereq': lambda: backend_api.synthetic_bucketing_supported()
         },
        {'id': 'offload_hash_bucket_types_date',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with Date type bucket',
         'narrative': 'Offload the table with the bucket_hash_col set to a Date type dimension',
         'options': {'owner_table': schema + '.' + DATATYPES_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'date_dim',
                     'num_buckets': 'auto'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, DATATYPES_DIM, bucket_col='date_dim', bucket_method='NULL'),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported()
         },
        {'id': 'offload_hash_bucket_types_timestamp',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with Timestamp type bucket',
         'narrative': 'Offload the table with the bucket_hash_col set to a Timestamp type dimension',
         'options': {'owner_table': schema + '.' + DATATYPES_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'timestamp_dim',
                     'num_buckets': 'auto'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, DATATYPES_DIM, bucket_col='timestamp_dim', bucket_method='NULL'),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported()
         },
        {'id': 'offload_hash_bucket_types_number',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with Number type bucket',
         'narrative': 'Offload the table with the bucket_hash_col set to a Number type dimension',
         'options': {'owner_table': schema + '.' + DATATYPES_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'number_dim',
                     'num_buckets': 'auto'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, DATATYPES_DIM, bucket_col='number_dim', bucket_method='NULL'),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported()
         },
        {'id': 'offload_hash_bucket_types_whole_number',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with Number type bucket (whole number)',
         'narrative': 'Offload the table with the bucket_hash_col set to a Number type dimension (whole number)',
         'options': {'owner_table': schema + '.' + DATATYPES_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'whole_num_dim',
                     'num_buckets': 'auto'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, DATATYPES_DIM, bucket_col='whole_num_dim', bucket_method='GLUENT_BUCKET'),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported()
         },
        {'id': 'offload_hash_bucket_types_varchar',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with VARCHAR type bucket',
         'narrative': 'Offload the table with the bucket_hash_col set to a VARCHAR type dimension',
         'options': {'owner_table': schema + '.' + DATATYPES_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'varchar_dim',
                     'num_buckets': 'auto'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, DATATYPES_DIM, bucket_col='varchar_dim', bucket_method='NULL'),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported()
         },
        {'id': 'offload_hash_bucket_types_char',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with char type bucket',
         'narrative': 'Offload the table with the bucket_hash_col set to a char type dimension',
         'options': {'owner_table': schema + '.' + DATATYPES_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'char_dim',
                     'num_buckets': 'auto'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, DATATYPES_DIM, bucket_col='char_dim', bucket_method='NULL'),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported()
         },
        {'id': 'offload_hash_bucket_types_nvarchar2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with nVarChar2 type bucket',
         'narrative': 'Offload the table with the bucket_hash_col set to an nVarChar2 type dimension',
         'options': {'owner_table': schema + '.' + DATATYPES_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'nvarchar2_dim',
                     'num_buckets': 'auto'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, DATATYPES_DIM, bucket_col='nvarchar2_dim', bucket_method='NULL'),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported() and options.db_type == DBTYPE_ORACLE
         },
        {'id': 'offload_hash_bucket_types_nchar',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with nchar type bucket',
         'narrative': 'Offload the table with the bucket_hash_col set to an nchar type dimension',
         'options': {'owner_table': schema + '.' + DATATYPES_DIM,
                     'reset_backend_table': True,
                     'bucket_hash_col': 'nchar_dim',
                     'num_buckets': 'auto'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, DATATYPES_DIM, bucket_col='nchar_dim', bucket_method='NULL'),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported() and options.db_type == DBTYPE_ORACLE
         },
    ]
