from test_sets.stories.story_globals import STORY_TYPE_OFFLOAD, STORY_TYPE_PRESENT, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import dbms_stats_gather_string, drop_backend_test_table, \
    gen_hybrid_drop_ddl, post_setup_pause, setup_backend_only
from test_sets.stories.story_assertion_functions import check_metadata, desc_columns, table_row_count

from gluentlib.offload.offload_constants import DBTYPE_BIGQUERY
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_transport import is_spark_thrift_available, is_spark_submit_available, \
    is_sqoop_available, is_livy_available, is_query_import_available, \
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY, OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT, \
    OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT, OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT, OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY
from gluentlib.util.misc_functions import add_suffix_in_same_case
from testlib.test_framework.test_functions import log

OFFLOAD_STORY_DIM_TRANSFORMATIONS = {"CUST_MARITAL_STATUS": "null",
                                     "CUST_MAIN_PHONE_NUMBER": "regexp_replace('[0-9]','X')",
                                     "CUST_STREET_ADDRESS" :"regexp_replace('^.*$','NOT-HERE')"}
OFFLOAD_STORY_FACT_TRANSFORMATIONS = {"AMOUNT_SOLD": "null",
                                      "CUST_MAIN_PHONE_NUMBER": "regexp_replace('[0-9]','X')",
                                      "CUST_STREET_ADDRESS" :"regexp_replace('^.*$','NOT-HERE')"}

BACKEND_DIM = 'story_hadoop_xform'
RDBMS_DIM = 'story_dim_xform'
RDBMS_FACT = 'story_fact_xform'


def transformation_story_present_run_transformation_list(backend_api):
    if backend_api and backend_api.backend_type() == DBTYPE_BIGQUERY:
        # No translate() equivalient on BigQuery
        return {"CUST_MARITAL_STATUS": "null",
                "CUST_MAIN_PHONE_NUMBER": "regexp_replace('[0-9]','X')",
                "CUST_CITY_ID": "suppress"}
    else:
        return {"CUST_MARITAL_STATUS": "null",
                "CUST_MAIN_PHONE_NUMBER": "regexp_replace('[0-9]','X')",
                'cust_city': "translate('A','a')",
                "CUST_CITY_ID": "suppress"}


def offload_dim_transform_assertions(schema, hybrid_schema, test_cursor):
    return [(lambda test: table_row_count(test_cursor, schema + '.' + RDBMS_DIM),
             lambda test: table_row_count(test_cursor, hybrid_schema + '.' + RDBMS_DIM)),
            (lambda test: table_row_count(test_cursor, hybrid_schema + '.' + RDBMS_DIM,
                                          where_clause='WHERE CUST_MARITAL_STATUS IS NOT NULL'),
             lambda test: 0),
            (lambda test: table_row_count(test_cursor, hybrid_schema + '.' + RDBMS_DIM,
                                          where_clause='WHERE CUST_STREET_ADDRESS <> \'NOT-HERE\''),
             lambda test: 0),
            (lambda test: table_row_count(test_cursor, hybrid_schema + '.' + RDBMS_DIM,
                                          where_clause='WHERE REGEXP_LIKE(CUST_MAIN_PHONE_NUMBER,\'[0-9]\')'),
             lambda test: 0)]


def offload_fact_transform_assertions(schema, hybrid_schema, test_cursor):
    return [(lambda test: table_row_count(test_cursor, schema + '.' + RDBMS_FACT),
             lambda test: table_row_count(test_cursor, hybrid_schema + '.' + RDBMS_FACT)),
            (lambda test: table_row_count(test_cursor, '%s.%s_ext' % (hybrid_schema, RDBMS_FACT),
                                          where_clause='WHERE AMOUNT_SOLD IS NOT NULL'),
             lambda test: 0),
            (lambda test: table_row_count(test_cursor, '%s.%s_ext' % (hybrid_schema, RDBMS_FACT),
                                          where_clause='WHERE CUST_STREET_ADDRESS <> \'NOT-HERE\''),
             lambda test: 0),
            (lambda test: table_row_count(test_cursor, '%s.%s_ext' % (hybrid_schema, RDBMS_FACT),
                                          where_clause='WHERE REGEXP_LIKE(CUST_MAIN_PHONE_NUMBER,\'[0-9]\')'),
             lambda test: 0)]


def transformations_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                test_cursor, list_only, repo_client):
    if not list_only and not options:
        return []
    if backend_api and not backend_api.gluent_column_transformations_supported():
        log('Skipping transformations_story_tests() because gluent_column_transformations_supported() == False')
        return []

    backend_dim_be = convert_backend_identifier_case(options, BACKEND_DIM)
    backend_dim_conv = add_suffix_in_same_case(backend_dim_be, '_conv')
    return [
        {'id': 'transformation_story_present_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Backend Dimension for Transformation Testing',
         'setup': setup_backend_only(backend_api, frontend_api, hybrid_schema, data_db, backend_dim_be,
                                     options, repo_client)},
        {'id': 'transformation_story_present_run',
         'type': STORY_TYPE_PRESENT,
         'title': 'Present and Transform Backend Only Table',
         'narrative': 'Use present transform column to mask, nullify and suppress columns/data',
         'options': {'owner_table': data_db + '.' + backend_dim_be,
                     'target_owner_name': schema + '.' + BACKEND_DIM,
                     'column_transformation_list': transformation_story_present_run_transformation_list(backend_api),
                     'force': True},
         'assertion_pairs': [
             (lambda test: table_row_count(test_cursor, hybrid_schema + '.' + BACKEND_DIM,
                                           where_clause='WHERE CUST_MARITAL_STATUS IS NOT NULL'),
              lambda test: 0),
             (lambda test: table_row_count(test_cursor, hybrid_schema + '.' + BACKEND_DIM,
                                           where_clause='WHERE REGEXP_LIKE(CUST_MAIN_PHONE_NUMBER,\'[0-9]\')'),
              lambda test: 0),
             (lambda test: len([x for x in desc_columns(test_cursor, hybrid_schema + '.' + BACKEND_DIM)
                                if x[0] == 'CUST_CITY_ID']),
              lambda test: 0),
             (lambda test: check_metadata(hybrid_schema, BACKEND_DIM, repo_client, hybrid_owner=hybrid_schema,
                                          hybrid_view=BACKEND_DIM, hadoop_owner=data_db, hadoop_table=backend_dim_conv),
              lambda test: True)],
         'prereq': lambda: post_setup_pause()},
        {'id': 'transformation_story_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Dimension for Query Transformation Testing',
         'setup': {'oracle': ['DROP TABLE %(schema)s.%(table)s' % {'schema': schema, 'table': RDBMS_DIM},
                              'CREATE TABLE %(schema)s.%(table)s AS SELECT * FROM %(schema)s.customers'
                              % {'schema': schema, 'table': RDBMS_DIM},
                              dbms_stats_gather_string(schema, RDBMS_DIM)],
                   'hybrid': gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, RDBMS_DIM),
                   'python': [lambda: drop_backend_test_table(options, backend_api, data_db, RDBMS_DIM)]}},
        {'id': 'transformation_story_dim_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload and Transform a Dimension',
         'narrative': 'Use offload transformation to mask columns in a small dimension, forcing Query Import',
         'options': {'owner_table': schema + '.' + RDBMS_DIM,
                     'offload_transport_method': OFFLOAD_TRANSPORT_METHOD_QUERY_IMPORT,
                     'reset_backend_table': True,
                     'column_transformation_list': OFFLOAD_STORY_DIM_TRANSFORMATIONS},
         'assertion_pairs': offload_dim_transform_assertions(schema, hybrid_schema, test_cursor),
         'prereq': lambda: is_query_import_available(None, options)},
        {'id': 'transformation_story_dim_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload and Transform a Dimension with Sqoop',
         'narrative': 'Use offload transformation to mask columns in a small dimension, forcing Sqoop',
         'options': {'owner_table': schema + '.' + RDBMS_DIM,
                     'offload_transport_method': OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
                     'reset_backend_table': True,
                     'column_transformation_list': OFFLOAD_STORY_DIM_TRANSFORMATIONS},
         'assertion_pairs': offload_dim_transform_assertions(schema, hybrid_schema, test_cursor),
         'prereq': lambda: is_sqoop_available(None, options)},
        {'id': 'transformation_story_dim_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload and Transform a Dimension with Spark Thrift',
         'narrative': 'Use offload transformation to mask columns in a small dimension, forcing Spark Thrift',
         'options': {'owner_table': schema + '.' + RDBMS_DIM,
                     'offload_transport_method': OFFLOAD_TRANSPORT_METHOD_SPARK_THRIFT,
                     'reset_backend_table': True,
                     'column_transformation_list': OFFLOAD_STORY_DIM_TRANSFORMATIONS},
         'prereq': lambda: is_spark_thrift_available(options, None),
         'assertion_pairs': offload_dim_transform_assertions(schema, hybrid_schema, test_cursor)},
        {'id': 'transformation_story_dim_offload4',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload and Transform a Dimension with Spark Submit',
         'narrative': 'Use offload transformation to mask columns in a small dimension, forcing Spark Submit',
         'options': {'owner_table': schema + '.' + RDBMS_DIM,
                     'offload_transport_method': OFFLOAD_TRANSPORT_METHOD_SPARK_SUBMIT,
                     'reset_backend_table': True,
                     'column_transformation_list': OFFLOAD_STORY_DIM_TRANSFORMATIONS},
         'prereq': lambda: is_spark_submit_available(options, None),
         'assertion_pairs': offload_dim_transform_assertions(schema, hybrid_schema, test_cursor)},
        {'id': 'transformation_story_dim_offload5',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload and Transform a Dimension with Spark Livy',
         'narrative': 'Use offload transformation to mask columns in a small dimension, forcing Spark Livy',
         'options': {'owner_table': schema + '.' + RDBMS_DIM,
                     'offload_transport_method': OFFLOAD_TRANSPORT_METHOD_SPARK_LIVY,
                     'reset_backend_table': True,
                     'column_transformation_list': OFFLOAD_STORY_DIM_TRANSFORMATIONS},
         'prereq': lambda: is_livy_available(options, None),
         'assertion_pairs': offload_dim_transform_assertions(schema, hybrid_schema, test_cursor)},
        {'id': 'transformation_story_fact_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Fact for Query Transformation Testing',
         'setup': {'oracle': ['DROP TABLE %(schema)s.%(table)s' % {'schema': schema, 'table': RDBMS_FACT},
                              """CREATE TABLE %(schema)s.%(table)s PARTITION BY RANGE (time_id)
                                  (PARTITION p_20120201 VALUES LESS THAN (TO_DATE('2012-02-01','YYYY-MM-DD'))
                                  ,PARTITION p_20120202 VALUES LESS THAN (TO_DATE('2012-02-02','YYYY-MM-DD'))
                                  ,PARTITION p_20120203 VALUES LESS THAN (TO_DATE('2012-02-03','YYYY-MM-DD'))
                                  ,PARTITION p_20120204 VALUES LESS THAN (TO_DATE('2012-02-04','YYYY-MM-DD'))
                                  ,PARTITION p_20120205 VALUES LESS THAN (TO_DATE('2012-02-05','YYYY-MM-DD'))
                                  ,PARTITION p_20120206 VALUES LESS THAN (TO_DATE('2012-02-06','YYYY-MM-DD')))
                                  AS
                                  SELECT s.*, c.cust_main_phone_number, c.cust_street_address FROM %(schema)s.sales s, %(schema)s.customers c
                                  WHERE c.cust_id = s.cust_id
                                  AND time_id BETWEEN TO_DATE('2012-01-31','YYYY-MM-DD') AND TO_DATE('2012-02-05 23:59','YYYY-MM-DD HH24:MI')""" % {'schema': schema, 'table': RDBMS_FACT},
                              # NULLABLE AMOUNT_SOLD is workaround for GOE-1311
                              "ALTER TABLE %(schema)s.%(table)s MODIFY (AMOUNT_SOLD NULL)"
                              % {'schema': schema, 'table': RDBMS_FACT}],
                   'hybrid': gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, RDBMS_FACT),
                   'python': [lambda: drop_backend_test_table(options, backend_api, data_db, RDBMS_FACT)]}},
        {'id': 'transformation_story_fact_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload and Transform an Oracle Fact (Extent Driven)',
         'narrative': 'Mask columns in a partitioned fact with parallelism > number of partitions to test extent splitter',
         'options': {'owner_table': schema + '.' + RDBMS_FACT,
                     'reset_backend_table': True,
                     'column_transformation_list': OFFLOAD_STORY_FACT_TRANSFORMATIONS,
                     'offload_transport_parallelism': 8},
         'assertion_pairs': offload_fact_transform_assertions(schema, hybrid_schema, test_cursor)},
        {'id': 'transformation_story_fact_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload and Transform an Oracle Fact (Partition Driven)',
         'narrative': 'Mask columns in a partitioned fact with parallelism < number of partitions to test partition splitter',
         'options': {'owner_table': schema + '.' + RDBMS_FACT,
                     'reset_backend_table': True,
                     'column_transformation_list': OFFLOAD_STORY_FACT_TRANSFORMATIONS,
                     'offload_transport_parallelism': 2},
         'assertion_pairs': offload_fact_transform_assertions(schema, hybrid_schema, test_cursor)}
    ]
