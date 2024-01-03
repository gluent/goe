import os
from textwrap import dedent
from distutils.version import StrictVersion

from test_sets.stories.offload_fs_location import base_table_fs_scheme_is_correct
from test_sets.stories.story_globals import STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP, \
    STORY_SETUP_TYPE_FRONTEND, STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_ORACLE, STORY_SETUP_TYPE_PYTHON
from test_sets.stories.story_setup_functions import dbms_stats_gather_string, drop_backend_test_table,\
    gen_hybrid_drop_ddl, gen_rdbms_dim_create_ddl, gen_sales_based_fact_create_ddl,\
    gen_sales_based_multi_pcol_fact_create_ddl, no_query_import_transport_method, \
    SALES_BASED_FACT_HV_1, SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3, SALES_BASED_FACT_HV_5
from test_sets.stories.story_assertion_functions import backend_table_exists, backend_column_exists, check_metadata, \
    get_date_offload_granularity, get_max_bucket_id, get_query_engine, messages_step_executions, \
    offload_dim_assertion, offload_fact_assertions, \
    synthetic_part_col_name, table_minus_row_count, text_in_log

from goe.gluent import num_location_files_enabled
from goe.config import orchestration_defaults
from goe.filesystem.gluent_dfs import OFFLOAD_FS_SCHEME_HDFS
from goe.offload.backend_table import DATA_VALIDATION_NOT_NULL_EXCEPTION_TEXT, \
    OFFLOAD_CHUNK_COLUMN_MESSAGE_PATTERN
from goe.offload.offload_constants import DBTYPE_IMPALA, DBTYPE_ORACLE, DBTYPE_TERADATA, \
    NOT_NULL_PROPAGATION_AUTO, NOT_NULL_PROPAGATION_NONE, OFFLOAD_BUCKET_NAME, \
    HADOOP_BASED_BACKEND_DISTRIBUTIONS, PART_COL_GRANULARITY_YEAR
from goe.offload.offload_functions import convert_backend_identifier_case, load_db_name
from goe.offload.offload_metadata_functions import OFFLOAD_TYPE_FULL
from goe.offload.operation.not_null_columns import UNKNOWN_NOT_NULL_COLUMN_EXCEPTION_TEXT
from goe.offload.operation.partition_controls import OFFLOAD_CHUNK_COLUMN_PART_COL_EXCEPTION_TEXT,\
    PARTITION_FUNCTIONS_NOT_SUPPORTED_EXCEPTION_TEXT, TOO_MANY_BACKEND_PART_COLS_EXCEPTION_TEXT
from goe.orchestration import command_steps
from goe.orchestration.command_steps import step_title
from goe.util.misc_functions import add_suffix_in_same_case
from testlib.test_framework.test_functions import log


OFFLOAD_DIM = 'STORY_ESO_DIM'
OFFLOAD_NEW_NAME_DIM1 = 'STORY_DIM_NEW_NAME'
OFFLOAD_NEW_NAME_DIM2 = 'STORY_DIM_NEW_NAME2'
OFFLOAD_NEW_NAME_FACT1 = 'STORY_FACT_NEW_NAME'
OFFLOAD_NEW_NAME_FACT2 = 'STORY_FACT_NEW_NAME2'
OFFLOAD_MCOL_MAXVAL_TABLE = 'STORY_MCOL_MAXVAL'
OFFLOAD_NAN_DIM = 'STORY_NAN_DIM'
OFFLOAD_NANO_FACT = 'STORY_NANO_FACT'
OFFLOAD_MICRO_FACT = 'STORY_MICRO_FACT'
HASH_FACT = 'STORY_HASH'
OLD_FORMAT_FACT = 'STORY_OLD_HYBVW'
ERR_FACT = 'STORY_ERR_FACT'
NOSEG_FACT = 'STORY_NOSEG_FACT'
NOT_NULL_DIM = 'STORY_NOT_NULL'
XMLTYPE_TABLE = 'STORY_XMLTYPE'


def offload_sql_supports_microsecond_predicates(backend_api):
    """ The nanosecond predicates in agg validate are failing with LLAP, see GOE-1182 for details
        Microsecond preds don't work either, hence the name of this fn
    """
    return bool(backend_api and backend_api.sql_microsecond_predicate_supported())


def connector_sql_supports_nanoseconds():
    """ For Spark: "Timestamps are now stored at a precision of 1us, rather than 1ns"
        Doc link: https://spark.apache.org/docs/2.4.0/sql-migration-guide-upgrade.html
    """
    return bool(os.environ.get('CONNECTOR_SQL_ENGINE', '').upper() not in ('SPARK', 'BIGQUERY'))


def gen_fractional_second_partition_table_ddl(options, frontend_api, schema, table_name, scale):
    assert scale in (6, 9)
    fractional_9s = '9'.ljust(scale, '9')
    fractional_0s = '0'.ljust(scale-1, '0')
    if not options:
        return []
    ddls = [f'DROP TABLE {schema}.{table_name}']
    if options.db_type == DBTYPE_ORACLE:
        ddls.extend([
           """CREATE TABLE %(schema)s.%(table)s
              (id INTEGER, dt DATE, ts TIMESTAMP(%(scale)s), cat INTEGER) PARTITION BY RANGE (ts)
              (PARTITION %(table)s_1_998 VALUES LESS THAN (TIMESTAMP' 2030-01-01 23:59:59.%(fractional_9s)s')
              ,PARTITION %(table)s_1_999 VALUES LESS THAN (TIMESTAMP' 2030-01-02 00:00:00.%(fractional_0s)s0')
              ,PARTITION %(table)s_2_000 VALUES LESS THAN (TIMESTAMP' 2030-01-02 00:00:00.%(fractional_0s)s1')
              ,PARTITION %(table)s_2_002 VALUES LESS THAN (TIMESTAMP' 2030-01-02 00:00:00.%(fractional_0s)s3')
              ,PARTITION %(table)s_2_999 VALUES LESS THAN (TIMESTAMP' 2030-01-03 00:00:00.%(fractional_0s)s1'))""" \
              % {'schema': schema, 'table': table_name, 'scale': scale,
                 'fractional_9s': fractional_9s, 'fractional_0s': fractional_0s},
           """INSERT INTO %(schema)s.%(table)s (id, cat, dt, ts)
              SELECT ROWNUM,1,TO_DATE('2001-01-01','YYYY-MM-DD'),TIMESTAMP' 2030-01-02 00:00:00.%(fractional_0s)s2'-(NUMTODSINTERVAL(ROWNUM, 'SECOND')/1e%(scale)s)
              FROM dual CONNECT BY ROWNUM <= 4""" % {'schema': schema, 'table': table_name, 'scale': scale, 'fractional_0s': fractional_0s}
        ])
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    ddls.append(dbms_stats_gather_string(schema, table_name, frontend_api))
    return ddls


def gen_nan_table_ddl(schema, table_name):
    ddl = ['DROP TABLE %(schema)s.%(table)s' % {'schema': schema, 'table': table_name},
           """CREATE TABLE %(schema)s.%(table)s
              (id INTEGER, bf BINARY_FLOAT, bd BINARY_DOUBLE)""" \
              % {'schema': schema, 'table': table_name}]
    for id, val in enumerate(['nan', 'inf', '-inf', '123.456']):
        ddl.append(
            """INSERT INTO %(schema)s.%(table)s (id, bf, bd) VALUES (%(id)d, '%(val)s', '%(val)s')""" % {
                'schema': schema,
                'table': table_name,
                'id': id, 'val': val})
    ddl.append(dbms_stats_gather_string(schema, table_name))
    return ddl


def gen_not_null_table_ddl(options, frontend_api, schema, table_name):
    if not options:
        return []
    ddls = [f'DROP TABLE {schema}.{table_name}']
    if options.db_type == DBTYPE_ORACLE:
        ddls.extend([
            f"""CREATE TABLE {schema}.{table_name}
            ( id INTEGER, join_id INTEGER
            , dt DATE, dt_nn DATE NOT NULL
            , ts TIMESTAMP(0), ts_nn TIMESTAMP(0) NOT NULL
            , num NUMBER(5), num_nn NUMBER(5) NOT NULL
            , vc VARCHAR2(10), vc_nn VARCHAR2(10) NOT NULL
            , with_nulls VARCHAR2(10)
            )""",
            f"""INSERT INTO {schema}.{table_name}
            SELECT ROWNUM,1
            ,      SYSDATE,SYSDATE
            ,      SYSDATE,SYSDATE
            ,      ROWNUM*2,ROWNUM*2
            ,      'NOT NULL','NOT NULL'
            ,      DECODE(MOD(ROWNUM,2),0,'NOT NULL',NULL)
            FROM dual CONNECT BY ROWNUM <= 3"""
        ])
    elif options.db_type == DBTYPE_TERADATA:
        ddls.extend([
            f"""CREATE TABLE {schema}.{table_name}
            ( id INTEGER, join_id INTEGER
            , dt DATE, dt_nn DATE NOT NULL
            , ts TIMESTAMP(0), ts_nn TIMESTAMP(0) NOT NULL
            , num NUMBER(5), num_nn NUMBER(5) NOT NULL
            , vc VARCHAR(10), vc_nn VARCHAR(10) NOT NULL
            , with_nulls VARCHAR(10)
            )""",
            f"""INSERT INTO {schema}.{table_name}
            SELECT id,1
            ,      CURRENT_DATE,CURRENT_DATE
            ,      TRUNC(CURRENT_TIMESTAMP),TRUNC(CURRENT_TIMESTAMP)
            ,      id*2,id*2
            ,      'NOT NULL','NOT NULL'
            ,      DECODE(MOD(id,2),0,'NOT NULL',NULL)
            FROM {schema}.generated_ids WHERE id <= 100"""
        ])
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    ddls.append(dbms_stats_gather_string(schema, table_name, frontend_api))
    return ddls


def offload_new_name_tmp_owner(schema, backend_api):
    """ If the backend can create dbs then use a temp name, otherwise just pass through
    """
    if backend_api and backend_api.create_database_supported():
        return add_suffix_in_same_case(schema, '_story_tmp')
    else:
        return schema


def offload_new_name_using_temp_db(db_name, backend_api):
    return bool(offload_new_name_tmp_owner(db_name, backend_api).lower() != db_name.lower())


def offload_new_name_drop_db(db_name, backend_api, options):
    """ Only drop the db if we're creating a temporary one
    """
    if offload_new_name_using_temp_db(db_name, backend_api):
        cmds = []
        tmp_db_owner = offload_new_name_tmp_owner(db_name, backend_api)
        tmp_load_owner = load_db_name(tmp_db_owner, options)
        cmds.append(backend_api.drop_database(tmp_db_owner, cascade=True))
        cmds.append(backend_api.drop_database(tmp_load_owner, cascade=True))
        return cmds


def no_nan_assertions(options, schema, hybrid_schema, data_db, frontend_api, backend_api):
    if not options:
        return []
    # All of NaN, inf and -inf should be NULL so COUNT expected to be 3 in backend and 0 in frontend
    asrt = [
        (lambda test: frontend_api.get_table_row_count(schema, OFFLOAD_NAN_DIM, "bf IS NULL"),
         lambda test: 0)
    ]
    if frontend_api.hybrid_schema_supported():
        asrt.append(
            (lambda test: frontend_api.get_table_row_count(hybrid_schema, OFFLOAD_NAN_DIM, "bf IS NULL"),
             lambda test: 3)
        )
    else:
        be_table_name = convert_backend_identifier_case(options, OFFLOAD_NAN_DIM)
        asrt.append(
            (lambda test: backend_api.get_table_row_count(data_db, be_table_name, "bf IS NULL"),
             lambda test: 3)
        )
    return asrt


def offload_story_new_name_dim_init_assertions(hybrid_schema, data_db, table_name, be_table, new_table,
                                               options, backend_api, frontend_api, repo_client) -> list:
    if not options:
        return []
    asrt = [(lambda test: check_metadata(hybrid_schema, table_name, repo_client, hadoop_owner=data_db,
                                         hadoop_table=new_table),
             lambda test: True),
            (lambda test: backend_table_exists(backend_api, data_db, new_table),
             lambda test: True),
            (lambda test: backend_table_exists(backend_api, data_db, be_table),
             lambda test: False)]
    if frontend_api.hybrid_schema_supported():
        agg_name = add_suffix_in_same_case(table_name, '_agg')
        asrt.append((lambda test: check_metadata(hybrid_schema, agg_name, repo_client,
                                                 hadoop_owner=data_db, hadoop_table=new_table),
                     lambda test: True))
    return asrt


def offload_fact_new_name_partition_options(backend_api):
    if backend_api and not backend_api.partition_by_column_supported():
        return {}
    else:
        return {'offload_partition_columns': 'time_id',
                'offload_partition_granularity': get_date_offload_granularity(backend_api, PART_COL_GRANULARITY_YEAR)}


def offload_fact_new_name_init_assertion(hybrid_schema, data_db, new_db, orig_table, new_table, options, backend_api,
                                         frontend_api, repo_client):
    if not options:
        return True
    if not check_metadata(hybrid_schema, orig_table, repo_client, hadoop_owner=new_db,
                          hadoop_table=new_table.upper()):
        log('check_metadata(%s, %s): False' % (hybrid_schema, orig_table))
        return False
    if frontend_api.hybrid_schema_supported():
        agg_name = add_suffix_in_same_case(orig_table, '_agg')
        if not check_metadata(hybrid_schema, agg_name, repo_client, hadoop_owner=new_db,
                              hadoop_table=new_table.upper()):
            log('check_metadata(%s, %s): False' % (hybrid_schema, agg_name))
            return False
    if not backend_table_exists(backend_api, new_db, new_table):
        log('backend_table_exists(%s, %s): False when should be True' % (new_db, new_table))
        return False
    if backend_table_exists(backend_api, data_db, orig_table):
        log('backend_table_exists(%s, %s): True when should be False' % (data_db, orig_table))
        return False
    return True


def offload_not_null_assertion(data_db, table_name, options, backend_api, not_null_col_list=None):
    if not backend_api:
        return True
    if not_null_col_list is None:
        not_null_col_list = ['DT_NN', 'TS_NN', 'NUM_NN', 'VC_NN']
    be_table_name = convert_backend_identifier_case(options, table_name)
    for backend_column in backend_api.get_columns(data_db, be_table_name):
        if backend_column.name.upper() == 'ID':
            continue
        expected_nullable = bool(not (backend_api.not_null_column_supported()
                                      and backend_column.name.upper() in not_null_col_list))
        column_nullable = backend_column.nullable
        if column_nullable is None:
            column_nullable = True
        if column_nullable != expected_nullable:
            log(f'{backend_column.name}.nullable: {column_nullable} when should be {expected_nullable}')
            return False
    return True


def offload_story_mcol_maxvalue_partition_options(backend_api):
    if not backend_api or not backend_api.partition_by_column_supported():
        return {}
    elif backend_api.max_partition_columns() == 1:
        return {'offload_partition_columns': 'time_id'}
    else:
        return {'offload_partition_granularity': '1,1,Y'}


def offload_story_hash_offload_assertion(test, schema, hybrid_schema, data_db, table_name, backend_name,
                                         frontend_api, backend_api, options, repo_client):
    if not options:
        return True
    if (frontend_api.hybrid_schema_supported()
        and table_minus_row_count(test, test.name, schema + '.' + table_name,
                                  hybrid_schema + '.' + table_name, None) != 0):
        return False
    if backend_api.partition_by_column_supported():
        # possible_part_cols covers outcome being synthetic or native
        possible_part_cols = [
            synthetic_part_col_name(1000, 'id', orchestration_defaults.synthetic_partition_digits_default()).lower(),
            'id']
        for part_col in (backend_api.get_partition_columns(data_db, backend_name) or []):
            if part_col.name.lower() in possible_part_cols:
                log('Partition key should not exist: %s' % part_col.name)
                return False
        if backend_column_exists(backend_api, data_db, backend_name, 'gl_part_000000000001000_id'):
            return False
    if not check_metadata(hybrid_schema, table_name, repo_client, hybrid_owner=hybrid_schema, hybrid_view=table_name,
                          hadoop_owner=data_db, hadoop_table=backend_name, offload_type=OFFLOAD_TYPE_FULL):
        return False
    if messages_step_executions(test.offload_messages, step_title(command_steps.STEP_FINAL_LOAD)) != 2:
        log('Execution count of step "%s" != 2' % step_title(command_steps.STEP_FINAL_LOAD))
        return False
    return True


def old_dep_hyb_view_ddl_v3_0(schema, hybrid_schema, table_name, time_id_hv):
    """ In v3.0 we didn't use tags to surround HWM conditions """
    return dedent("""
            CREATE OR REPLACE VIEW "%(hybrid_schema)s"."%(table_name)s_CNT_AGG" ("TIME_ID", "COUNT_STAR", "MIN_TIME_ID", "MAX_TIME_ID", "COUNT_TIME_ID") AS
            SELECT /* IMPORTANT: do not modify this view or query underlying tables directly */
                   /*+
                       QB_NAME("GLUENT_HYBRID_RDBMS")
                       OPT_PARAM('query_rewrite_integrity','trusted')
                       REWRITE
                   */
                   "TIME_ID",
                   COUNT(*),
                   MIN("TIME_ID"),
                   MAX("TIME_ID"),
                   COUNT("TIME_ID")
            FROM   "%(schema)s"."%(table_name)s"
            WHERE  "TIME_ID" >= TO_DATE(' %(time_id_hv)s 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
            GROUP BY TIME_ID
            UNION ALL
            SELECT
                   /*+
                       QB_NAME("GLUENT_HYBRID_OFFLOADED")
                       OPT_PARAM('query_rewrite_integrity','trusted')
                       REWRITE
                   */
                   "%(table_name)s_CNT_AGG_EXT"."TIME_ID",
                   "%(table_name)s_CNT_AGG_EXT"."COUNT_STAR",
                   "%(table_name)s_CNT_AGG_EXT"."MIN_TIME_ID",
                   "%(table_name)s_CNT_AGG_EXT"."MAX_TIME_ID",
                   "%(table_name)s_CNT_AGG_EXT"."COUNT_TIME_ID"
            FROM   "%(hybrid_schema)s"."%(table_name)s_CNT_AGG_EXT"
            WHERE  "TIME_ID" < TO_DATE(' %(time_id_hv)s 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')""")\
            % {'schema': schema.upper(), 'hybrid_schema': hybrid_schema.upper(),
               'table_name': table_name.upper(), 'time_id_hv': time_id_hv}


def old_dep_hyb_view_ddl_v2_8(schema, hybrid_schema, table_name, time_id_hv):
    """ In v2.8 we didn't quote column names """
    return dedent("""
            CREATE OR REPLACE VIEW "%(hybrid_schema)s"."%(table_name)s_CNT_AGG" (TIME_ID, COUNT_STAR, MIN_TIME_ID, MAX_TIME_ID, COUNT_TIME_ID) AS
            SELECT /* IMPORTANT: do not modify this view or query underlying tables directly */
                   /*+
                       QB_NAME("GLUENT_HYBRID_RDBMS")
                       OPT_PARAM('query_rewrite_integrity','trusted')
                       REWRITE
                   */
                   TIME_ID,
                   COUNT(*),
                   MIN(TIME_ID),
                   MAX(TIME_ID),
                   COUNT(TIME_ID)
            FROM   %(schema)s.%(table_name)s
            WHERE  TIME_ID >= TO_DATE(' %(time_id_hv)s 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')
            GROUP BY TIME_ID
            UNION ALL
            SELECT
                   /*+
                       QB_NAME("GLUENT_HYBRID_OFFLOADED")
                       OPT_PARAM('query_rewrite_integrity','trusted')
                       REWRITE
                   */
                   "%(table_name)s_CNT_AGG_EXT"."TIME_ID",
                   "%(table_name)s_CNT_AGG_EXT"."COUNT_STAR",
                   "%(table_name)s_CNT_AGG_EXT"."MIN_TIME_ID",
                   "%(table_name)s_CNT_AGG_EXT"."MAX_TIME_ID",
                   "%(table_name)s_CNT_AGG_EXT"."COUNT_TIME_ID"
            FROM   "%(hybrid_schema)s"."%(table_name)s_CNT_AGG_EXT"
            WHERE  TIME_ID < TO_DATE(' %(time_id_hv)s 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')""") \
           % {'schema': schema.upper(), 'hybrid_schema': hybrid_schema.upper(),
              'table_name': table_name.upper(), 'time_id_hv': time_id_hv}


def offload_story_force_hdfs_prereq(options, backend_api):
    if not backend_api.filesystem_scheme_hdfs_supported():
        return False
    if backend_api.backend_type() == DBTYPE_IMPALA and \
        StrictVersion(backend_api.target_version()) >= StrictVersion('3.3.0') and \
        hasattr(options, 'hiveserver2_http_transport') and options.hiveserver2_http_transport == True:
            return False
    return True


def hint_text_in_log(options, parallelism, search_from_text):
    if not options:
        return True
    if options.db_type == DBTYPE_ORACLE:
        hint = 'NO_PARALLEL' if parallelism in (0, 1) else 'PARALLEL({})'.format(str(parallelism))
        return text_in_log(hint, search_from_text)
    else:
        return False


def offload_esoteric_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api, repo_client):
    option_target = options.target if options else ''
    offload_new_name_dim1_be = convert_backend_identifier_case(options, OFFLOAD_NEW_NAME_DIM1)
    offload_new_name_dim2_be = convert_backend_identifier_case(options, OFFLOAD_NEW_NAME_DIM2)
    offload_new_name_fact1_be = convert_backend_identifier_case(options, OFFLOAD_NEW_NAME_FACT1)
    offload_new_name_fact2_be = convert_backend_identifier_case(options, OFFLOAD_NEW_NAME_FACT2)
    hash_fact_be = convert_backend_identifier_case(options, HASH_FACT)
    xmltype_table_be = convert_backend_identifier_case(options, XMLTYPE_TABLE)
    if frontend_api:
        frontend_datetime = frontend_api.test_type_canonical_timestamp()
    else:
        frontend_datetime = None

    return [
        {'id': 'offload_story_dim_num_loc_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_DIM,
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_rdbms_dim_create_ddl(frontend_api, schema, OFFLOAD_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_DIM)]}},
        {'id': 'offload_story_dim_num_loc_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with Auto Buckets Capped by Location Files',
         'narrative': 'Offload the dimension checking that --num-buckets is capped at --num-location-files',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'num_buckets': 'auto',
                     'num_location_files': 4},
         'config_override': {'num_buckets_max': 16,
                             'num_buckets_threshold': '0.1k'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, OFFLOAD_DIM,
                                                 check_aggs=True, num_loc_files=4,
                                                 bucket_check=(4 if num_location_files_enabled(option_target) else 16)),
              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported()},
        {'id': 'offload_story_dim_num_loc_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with Default Loc Files',
         'narrative': 'Offload the dimension checking that --num-location-files reverts to the default, Hive only',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'num_buckets': 1},
         'config_override': {'num_buckets_max': 16,
                             'num_buckets_threshold': '0.1k'},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, OFFLOAD_DIM,
                                                 bucket_check=1, check_aggs=True,
                                                 num_loc_files=int(orchestration_defaults.num_location_files_default())),
              lambda test: True)],
         'prereq': lambda: num_location_files_enabled(option_target)},
        #{'id': 'offload_story_new_name_dim_setup',
         #'type': STORY_TYPE_SETUP,
         #'title': 'Create %s' % OFFLOAD_NEW_NAME_DIM1,
         #'setup': {STORY_SETUP_TYPE_FRONTEND: gen_rdbms_dim_create_ddl(frontend_api, schema, OFFLOAD_NEW_NAME_DIM1),
                   #STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_NEW_NAME_DIM1),
                   #STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             #data_db, OFFLOAD_NEW_NAME_DIM1),
                                             #lambda: drop_backend_test_table(options, backend_api,
                                                                             #data_db, OFFLOAD_NEW_NAME_DIM2)]}},
        #{'id': 'offload_story_new_name_dim_offload1',
         #'type': STORY_TYPE_OFFLOAD,
         #'title': 'Offload Dim to New Table Name',
         #'narrative': 'Used to confirm that we can offload to a backend table with a different table name.',
         #'options': {'owner_table': schema + '.' + OFFLOAD_NEW_NAME_DIM1,
                     #'target_owner_name': schema + '.' + OFFLOAD_NEW_NAME_DIM2,
                     #'reset_backend_table': True},
         #'assertion_pairs': [
             #(lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 #schema, hybrid_schema, data_db, OFFLOAD_NEW_NAME_DIM1,
                                                 #check_aggs=True, hadoop_table=offload_new_name_dim2_be),
              #lambda test: True)
         #] + offload_story_new_name_dim_init_assertions(hybrid_schema, data_db, OFFLOAD_NEW_NAME_DIM1,
                                                        #offload_new_name_dim1_be, offload_new_name_dim2_be,
                                                        #options, backend_api, frontend_api, repo_client)},
        # Uncomment this test after completing GOE-1461
        #{'id': 'offload_story_new_name_dim_offload2',
         #'type': STORY_TYPE_OFFLOAD,
         #'title': 'Attempt Re-Offload Renamed Dim',
         #'narrative': 'Used to confirm that attempted re-offload exits early and doesn\'t lose sight of previous --target-name',
         #'options': {'owner_table': schema + '.' + OFFLOAD_NEW_NAME_DIM1},
         #'assertion_pairs': [(lambda test: backend_table_exists(backend_api, data_db, offload_new_name_dim1_be), lambda test: False)],
         #'expected_status': False},
        # Already copied across to identifiers scenario:
        #{'id': 'offload_story_new_name_fact_setup1',
         #'type': STORY_TYPE_SETUP,
         #'title': 'Create %s' % OFFLOAD_NEW_NAME_FACT1,
         #'narrative': 'Used to confirm we can offload to a table with a different name in a different database.',
         #'setup': {STORY_SETUP_TYPE_FRONTEND: gen_sales_based_fact_create_ddl(frontend_api, schema,
                                                                              #OFFLOAD_NEW_NAME_FACT1),
                   #STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema,
                                                                #OFFLOAD_NEW_NAME_FACT1),
                   #STORY_SETUP_TYPE_PYTHON:
                       #[lambda: drop_backend_test_table(options, backend_api, data_db, OFFLOAD_NEW_NAME_FACT1),
                        #lambda: drop_backend_test_table(options, backend_api,
                                                        #offload_new_name_tmp_owner(data_db, backend_api),
                                                        #OFFLOAD_NEW_NAME_FACT2),
                        #lambda: offload_new_name_drop_db(data_db, backend_api, options)]}},
        #{'id': 'offload_story_new_name_fact_invalidate_metadata',
         #'type': STORY_TYPE_SETUP,
         #'title': 'Invalidate metadata (Impala only)',
         #'narrative': 'Used to workaround GOE-1253.',
         #'setup': {STORY_SETUP_TYPE_PYTHON: [lambda: backend_api.execute_ddl('INVALIDATE METADATA')]},
         #'prereq': lambda: (get_query_engine() == DBTYPE_IMPALA and offload_new_name_using_temp_db(schema, backend_api))},
        #{'id': 'offload_story_new_name_fact_offload1',
         #'type': STORY_TYPE_OFFLOAD,
         #'title': 'Offload a Fact to New DB and Table Name',
         #'narrative': 'Offloads from a fact table but to a table with a different name in a different database.',
         #'options': dict(list({'owner_table': schema + '.' + OFFLOAD_NEW_NAME_FACT1,
                          #'target_owner_name': offload_new_name_tmp_owner(schema, backend_api) + '.' + OFFLOAD_NEW_NAME_FACT2,
                          #'reset_backend_table': True,
                          #'create_backend_db': True if offload_new_name_using_temp_db(schema, backend_api) else False,
                          #'older_than_date': SALES_BASED_FACT_HV_2}.items()) +
                         #list(offload_fact_new_name_partition_options(backend_api).items())),
         #'assertion_pairs': offload_fact_assertions(
             #options, backend_api, frontend_api, repo_client, schema, hybrid_schema, data_db,
             #OFFLOAD_NEW_NAME_FACT1, SALES_BASED_FACT_HV_2, check_aggs=True,
             #hadoop_schema=offload_new_name_tmp_owner(schema, backend_api),
             #hadoop_table=offload_new_name_fact2_be
         #) + [(lambda test: offload_fact_new_name_init_assertion(hybrid_schema, data_db,
                                                                 #offload_new_name_tmp_owner(data_db, backend_api),
                                                                 #offload_new_name_fact1_be, offload_new_name_fact2_be,
                                                                 #options, backend_api, frontend_api, repo_client),
               #lambda test: True)]},
        # Uncomment this test after completing GOE-1461
        #{'id': 'offload_story_new_name_fact_offload2',
         #'type': STORY_TYPE_OFFLOAD,
         #'title': 'Offload IPA to Renamed Backend Table Name',
         #'narrative': 'Offloads more partitions from a fact table but to a backend table with a different name and in a different database.',
         #'options': {'owner_table': schema + '.' + OFFLOAD_NEW_NAME_FACT1,
                     #'older_than_date': SALES_BASED_FACT_HV_3},
         #'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, schema, hybrid_schema, data_db, OFFLOAD_NEW_NAME_FACT1,
                                                    #SALES_BASED_FACT_HV_3, check_aggs=True,
                                                    #hadoop_schema=offload_new_name_tmp_owner(schema, backend_api), hadoop_table=offload_new_name_fact2_be) + \
                            #[(offload_fact_new_name_init_assertion(hybrid_schema, data_db, offload_new_name_tmp_owner(data_db, backend_api),
                                                                         #offload_new_name_fact1_be, offload_new_name_fact2_be, options, backend_api, frontend_api, repo_client),
                             #lambda test: True)]},
        #{'id': 'offload_story_new_name_fact_setup2',
         #'type': STORY_TYPE_SETUP,
         #'title': 'Drop Test DB %s' % offload_new_name_tmp_owner(data_db, backend_api),
         #'narrative': 'Clean up temporary database used for new name testing',
         #'setup': {STORY_SETUP_TYPE_PYTHON: [lambda: offload_new_name_drop_db(data_db, backend_api, options)]},
         #'prereq': lambda: offload_new_name_using_temp_db(data_db, backend_api)},
        {'id': 'offload_story_no_nan_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Offload a Dimension Containing Nan and Inf Values',
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_nan_table_ddl(schema, OFFLOAD_NAN_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_NAN_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_NAN_DIM)]},
         'prereq': lambda: frontend_api.nan_supported() and not backend_api.nan_supported()},
        {'id': 'offload_story_no_nan_offload_fail',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Fail to Offload table Containing Nan and Inf Values (expect to fail)',
         'narrative': "Fails to offload Nan and Inf values to a backend system that doesn't support it",
         'options': {'owner_table': schema + '.' + OFFLOAD_NAN_DIM,
                     'reset_backend_table': True,
                     'allow_floating_point_conversions': False},
         'expected_status': False,
         'prereq': lambda: frontend_api.nan_supported() and not backend_api.nan_supported()},
        {'id': 'offload_story_no_nan_offload_query_import',
         'type': STORY_TYPE_OFFLOAD,
         'title': "Offload Nan and Inf Values When the Backend Doesn't Support It (Query Import)",
         'options': {'owner_table': schema + '.' + OFFLOAD_NAN_DIM,
                     'reset_backend_table': True,
                     'allow_floating_point_conversions': True},
         'assertion_pairs': no_nan_assertions(options, schema, hybrid_schema, data_db, frontend_api, backend_api),
         'prereq': lambda: frontend_api.nan_supported() and not backend_api.nan_supported()},
        {'id': 'offload_story_no_nan_offload_no_query_import',
         'type': STORY_TYPE_OFFLOAD,
         'title': "Offload Nan and Inf Values When the Backend Doesn't Support It (No Query Import)",
         'options': {'owner_table': schema + '.' + OFFLOAD_NAN_DIM,
                     'reset_backend_table': True,
                     'offload_transport_method': no_query_import_transport_method(options),
                     'allow_floating_point_conversions': True},
         'assertion_pairs': no_nan_assertions(options, schema, hybrid_schema, data_db, frontend_api, backend_api),
         'prereq': lambda: frontend_api.nan_supported() and not backend_api.nan_supported()},
        {'id': 'offload_story_micro_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Offload a Microsecond Partitioned Fact',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_fractional_second_partition_table_ddl(options, frontend_api,
                                                                                              schema, OFFLOAD_MICRO_FACT, 6),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_MICRO_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_MICRO_FACT)]},
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_story_micro_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload a Microsecond Partitioned Fact',
         'narrative': 'Offloads first partition from a microsecond partitioned fact table',
         'options': {'owner_table': schema + '.' + OFFLOAD_MICRO_FACT,
                     'reset_backend_table': True,
                     'less_than_value': '2030-01-02'},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_MICRO_FACT, '2030-01-02',
                                                    incremental_key='TS',
                                                    incremental_key_type=frontend_datetime,
                                                    check_rowcount=True),
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_story_micro_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload a Microsecond Partitioned Fact',
         'narrative': 'Offloads first partition from a microsecond partitioned fact table',
         'options': {'owner_table': schema + '.' + OFFLOAD_MICRO_FACT,
                     'less_than_value': '2030-01-03',
                     'verify_row_count': 'aggregate' if offload_sql_supports_microsecond_predicates(backend_api) else 'minus'},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_MICRO_FACT,
                                                    '2030-01-02 00:00:00.000003000',
                                                    incremental_key='TS',
                                                    incremental_key_type=frontend_datetime,
                                                    check_rowcount=True),
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_story_micro_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'No-op Offload of Microsecond Partitioned Fact',
         'options': {'owner_table': schema + '.' + OFFLOAD_MICRO_FACT,
                     'less_than_value': '2030-01-03'},
         'expected_status': False},
        {'id': 'offload_story_nano_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Offload a Nanosecond Partitioned Fact',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_fractional_second_partition_table_ddl(options, frontend_api,
                                                                                              schema, OFFLOAD_NANO_FACT, 9),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_NANO_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_NANO_FACT)]},
         'prereq': lambda: backend_api.nanoseconds_supported()},
        ,
        {'id': 'offload_story_hash_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create a Hash Partitioned Table',
         'narrative': """This table is sized carefully in order to test chunking.
                         8 partitions * 64K = 512K for chunk size 300K. Also 1000 rows matters for chunking test""",
         'setup': {STORY_SETUP_TYPE_ORACLE:
                       ['DROP TABLE %(schema)s.%(table)s' % {'schema': schema, 'table': HASH_FACT},
                        """CREATE TABLE %(schema)s.%(table)s
                           (id NUMBER(8), data VARCHAR2(30))
                           PARTITION BY HASH (id) PARTITIONS 8 STORAGE (INITIAL 64k)"""
                        % {'schema': schema, 'table': HASH_FACT},
                        """INSERT INTO %(schema)s.%(table)s (id, data)
                           SELECT ROWNUM, DBMS_RANDOM.STRING('u',30) FROM dual CONNECT BY ROWNUM <= 1e3""" \
                        % {'schema': schema, 'table': HASH_FACT},
                        dbms_stats_gather_string(schema, HASH_FACT, frontend_api=frontend_api)],
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, HASH_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, HASH_FACT)]},
         'prereq': lambda: options.db_type == DBTYPE_ORACLE},
        {'id': 'offload_story_hash_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload a Hash Partitioned Table',
         'narrative': """Offload hash partitioned table. Check the partition column is not used for backend part col.
                         Chunk by size results in 2 chunks, verified by checking STEP_FINAL_LOAD count""",
         'options': {'owner_table': schema + '.' + HASH_FACT,
                     'max_offload_chunk_size': 300 * 1024,
                     'reset_backend_table': True},
         'assertion_pairs': [(lambda test: offload_story_hash_offload_assertion(test, schema, hybrid_schema, data_db,
                                                                                HASH_FACT, hash_fact_be,
                                                                                frontend_api, backend_api, options,
                                                                                repo_client),
                              lambda test: True)],
         'prereq': lambda: options.db_type == DBTYPE_ORACLE},
        {'id': 'offload_story_hash_chunk_col1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Table Chunking By offload_bucket_id',
         'narrative': 'Tests hidden Offload Chunk Column option to ensure it continues to work.',
         'options': {'owner_table': schema + '.' + HASH_FACT,
                     'offload_chunk_column': OFFLOAD_BUCKET_NAME,
                     'num_buckets': 2,
                     'reset_backend_table': True},
         'config_override': {'num_buckets_max': 2},
         'assertion_pairs': [(lambda test: table_minus_row_count(test, test.name, schema + '.' + HASH_FACT,
                                                                 hybrid_schema + '.' + HASH_FACT, None),
                              lambda test: 0),
                             (lambda test: get_max_bucket_id(backend_api, data_db, hash_fact_be), lambda test: 1),
                             (lambda test: text_in_log(OFFLOAD_CHUNK_COLUMN_MESSAGE_PATTERN % '2', '(%s)'
                                                       % ('offload_story_hash_chunk_col1')),
                              lambda test: True)],
         'prereq': lambda: backend_api.synthetic_bucketing_supported() and options.db_type == DBTYPE_ORACLE},
        {'id': 'offload_story_hash_chunk_col2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Table Chunking By Non-existent Partition Column',
         'options': {'owner_table': schema + '.' + HASH_FACT,
                     'offload_chunk_column': 'data',
                     'reset_backend_table': True},
         'prereq': lambda: backend_api.partition_by_column_supported() and options.db_type == DBTYPE_ORACLE,
         'expected_exception_string': OFFLOAD_CHUNK_COLUMN_PART_COL_EXCEPTION_TEXT},
        {'id': 'offload_story_hash_chunk_col3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Table Chunking By Synthetic Partition Column',
         'options': {'owner_table': schema + '.' + HASH_FACT,
                     'offload_chunk_column': 'id',
                     'offload_partition_columns': 'id',
                     'offload_partition_granularity': '10000',
                     'offload_partition_lower_value': 0,
                     'offload_partition_upper_value': 10001,
                     'reset_backend_table': True},
         'assertion_pairs': [(lambda test: table_minus_row_count(test, test.name, schema + '.' + HASH_FACT,
                                                                 hybrid_schema + '.' + HASH_FACT, None),
                              lambda test: 0),
                             (lambda test: text_in_log(OFFLOAD_CHUNK_COLUMN_MESSAGE_PATTERN % '1', '(%s)'
                                                       % ('offload_story_hash_chunk_col3')),
                              lambda test: True)],
         # This only makes sense on Hadoop where we don't have native RANGE partitioning
         'prereq': lambda: (backend_api.synthetic_partitioning_supported()
                            and options.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS
                            and options.db_type == DBTYPE_ORACLE)},
        # Don't need this any more
        #{'id': 'offload_story_error_recovery_setup',
        # 'type': STORY_TYPE_SETUP,
        # 'title': 'Create story_err_fact',
        # 'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, ERR_FACT),
        #           STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, ERR_FACT),
        #           STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
        #                                                                     data_db, ERR_FACT)]}},
        #{'id': 'offload_story_error_recovery_offload1',
        # 'type': STORY_TYPE_OFFLOAD,
        # 'title': 'Initial Offload of Fact',
        # 'narrative': 'Offloads first partition of fact for error recovery test',
        # 'options': {'owner_table': schema + '.' + ERR_FACT,
        #             'reset_backend_table': True,
        #             'older_than_date': SALES_BASED_FACT_HV_2},
        # 'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
        #                                            schema, hybrid_schema, data_db, ERR_FACT, SALES_BASED_FACT_HV_2,
        #                                            check_aggs=False)},
        #{'id': 'offload_story_error_recovery_drop_hybrid',
        # 'type': STORY_TYPE_SETUP,
        # 'title': 'Drop Hybrid Metadata For story_err_fact',
        # 'setup': {STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, ERR_FACT)}},
        #{'id': 'offload_story_error_recovery_offload2',
        # 'type': STORY_TYPE_OFFLOAD,
        # 'title': 'Incremental Offload of Fact ',
        # 'narrative': 'Next offload should succeed and create missing hybrid view',
        # 'options': {'owner_table': schema + '.' + ERR_FACT,
        #             'older_than_date': SALES_BASED_FACT_HV_3,
        #             'force': True},
        # 'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
        #                                            schema, hybrid_schema, data_db, ERR_FACT, SALES_BASED_FACT_HV_3,
        #                                            check_aggs=False)},
        {'id': 'offload_story_noseg_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % NOSEG_FACT,
         'narrative': 'Create a partitioned table with one populated partition and a series of empty ones',
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, NOSEG_FACT,
                                                               extra_pred="AND time_id = TO_DATE('2012-01-01','YYYY-MM-DD')"),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, NOSEG_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, NOSEG_FACT)]},
         # On Teradata, as far as GOE is concerned, empty partitions do not exist, therefore test not required.
         'prereq': lambda: bool(options and options.db_type != DBTYPE_TERADATA)},
        {'id': 'offload_story_noseg_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Initial Offload of Fact',
         'narrative': 'Offloads first partitions of fact, one is populated and a one empty',
         'options': {'owner_table': schema + '.' + NOSEG_FACT,
                     'max_offload_chunk_count': 1,
                     'reset_backend_table': True,
                     'older_than_date': SALES_BASED_FACT_HV_2},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, NOSEG_FACT, SALES_BASED_FACT_HV_2,
                                                    check_aggs=False),
         # On Teradata, as far as GOE is concerned, empty partitions do not exist, therefore test not required.
         'prereq': lambda: bool(options and options.db_type != DBTYPE_TERADATA)},
        {'id': 'offload_story_noseg_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Empty Partitions',
         'narrative': 'Attempts to offload empty partitions',
         'options': {'owner_table': schema + '.' + NOSEG_FACT,
                     'max_offload_chunk_count': 1,
                     'older_than_date': '2012-06-01'},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, NOSEG_FACT,
                                                    SALES_BASED_FACT_HV_5, check_aggs=False),
         # On Teradata, as far as GOE is concerned, empty partitions do not exist, therefore test not required.
         'prereq': lambda: bool(options and options.db_type != DBTYPE_TERADATA)},
        # Don't need this any more
        #{'id': 'offload_story_old_dep_hyb_view_setup1',
        # 'type': STORY_TYPE_SETUP,
        # 'title': 'Create %s' % OLD_FORMAT_FACT,
        # 'setup': {STORY_SETUP_TYPE_FRONTEND: gen_sales_based_fact_create_ddl(frontend_api, schema, OLD_FORMAT_FACT),
        #           STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OLD_FORMAT_FACT),
        #           STORY_SETUP_TYPE_PYTHON:
        #               [lambda: drop_backend_test_table(options, backend_api, data_db, OLD_FORMAT_FACT)]},
        # 'prereq': lambda: frontend_api.hybrid_schema_supported()},
        #{'id': 'offload_story_old_dep_hyb_view_offload1',
        # 'type': STORY_TYPE_OFFLOAD,
        # 'title': 'Offload 1st Partition From %s' % OLD_FORMAT_FACT,
        # 'options': {'owner_table': schema + '.' + OLD_FORMAT_FACT,
        #             'older_than_date': SALES_BASED_FACT_HV_1,
        #             'reset_backend_table': True},
        # 'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
        #                                            hybrid_schema, data_db, OLD_FORMAT_FACT, SALES_BASED_FACT_HV_1),
        # 'prereq': lambda: frontend_api.hybrid_schema_supported()},
        #{'id': 'offload_story_old_dep_hyb_view_setup2',
        # 'type': STORY_TYPE_SETUP,
        # 'title': 'Revert AAPD Views To v3.0',
        # 'narrative': 'Overwrite the CNT_AGG AAPD view with DDL from GOE v3.0. HWM predicates are a different format',
        # 'setup': {STORY_SETUP_TYPE_HYBRID:
        #               [old_dep_hyb_view_ddl_v3_0(schema, hybrid_schema, OLD_FORMAT_FACT, SALES_BASED_FACT_HV_1)]},
        # 'prereq': lambda: frontend_api.hybrid_schema_supported()},
        #{'id': 'offload_story_old_dep_hyb_view_offload2',
        # 'type': STORY_TYPE_OFFLOAD,
        # 'title': 'Offload 2nd Partition From %s' % OLD_FORMAT_FACT,
        # 'narrative': """IPA Offload with cnt agg AAPD view in old format. Dependent hybrid view code will still find
        #                 and replace the HWM.""",
        # 'options': {'owner_table': schema + '.' + OLD_FORMAT_FACT,
        #             'older_than_date': SALES_BASED_FACT_HV_2},
        # 'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
        #                                            hybrid_schema, data_db, OLD_FORMAT_FACT, SALES_BASED_FACT_HV_2),
        # 'prereq': lambda: frontend_api.hybrid_schema_supported()},
        #{'id': 'offload_story_old_dep_hyb_view_setup3',
        # 'type': STORY_TYPE_SETUP,
        # 'title': 'Revert AAPD Views To v2.8',
        # 'narrative': 'Overwrite the CNT_AGG AAPD view with DDL from GOE v2.8. Many columns are not double quoted',
        # 'setup': {STORY_SETUP_TYPE_HYBRID:
        #               [old_dep_hyb_view_ddl_v2_8(schema, hybrid_schema, OLD_FORMAT_FACT, SALES_BASED_FACT_HV_2)]},
        # 'prereq': lambda: frontend_api.hybrid_schema_supported()},
        #{'id': 'offload_story_old_dep_hyb_view_offload3',
        # 'type': STORY_TYPE_OFFLOAD,
        # 'title': 'Offload 2nd Partition From %s' % OLD_FORMAT_FACT,
        # 'narrative': """IPA Offload with cnt agg AAPD view in old format. Dependent hybrid view code will still find
        #                and replace the HWM.""",
        # 'options': {'owner_table': schema + '.' + OLD_FORMAT_FACT,
        #             'older_than_date': SALES_BASED_FACT_HV_3},
        # 'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
        #                                            hybrid_schema, data_db, OLD_FORMAT_FACT, SALES_BASED_FACT_HV_3),
        # 'prereq': lambda: frontend_api.hybrid_schema_supported()},
        {'id': 'offload_story_mcol_maxvalue_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_MCOL_MAXVAL_TABLE,
         'narrative': 'Create a multi-column RANGE partitioned table with MAXVALUE partitions',
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_sales_based_multi_pcol_fact_create_ddl(schema,
                                                                                       OFFLOAD_MCOL_MAXVAL_TABLE,
                                                                                       maxval_partition=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_MCOL_MAXVAL_TABLE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_MCOL_MAXVAL_TABLE)]},
         'prereq': lambda: frontend_api.gluent_multi_column_incremental_key_supported()},
        {'id': 'offload_story_mcol_maxvalue_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Initial Offload of Fact',
         'narrative': 'Offloads partitions of fact with MAXVALUE',
         'options': {'owner_table': schema + '.' + OFFLOAD_MCOL_MAXVAL_TABLE,
                     'reset_backend_table': True,
                     'less_than_value': '2012,12,MAXVALUE'},
         'expected_exception_string': TOO_MANY_BACKEND_PART_COLS_EXCEPTION_TEXT,
         'prereq': lambda: (backend_api.partition_by_column_supported()
                            and frontend_api.gluent_multi_column_incremental_key_supported()
                            and backend_api.max_partition_columns() < 3)},
        {'id': 'offload_story_mcol_maxvalue_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Initial Offload of Fact',
         'narrative': 'Offloads partitions of fact with MAXVALUE',
         'options': dict(list({'owner_table': schema + '.' + OFFLOAD_MCOL_MAXVAL_TABLE,
                          'bucket_hash_col': 'cust_id',
                          'reset_backend_table': True,
                          'less_than_value': '2012,12,MaxValue'}.items()) + \
                         list(offload_story_mcol_maxvalue_partition_options(backend_api).items())),
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_MCOL_MAXVAL_TABLE,
                                                    '2012, 12, MAXVALUE',
                                                    incremental_key='TIME_YEAR, TIME_MONTH, TIME_ID',
                                                    check_hwm_in_hybrid_view=False),
         'prereq': lambda: frontend_api.gluent_multi_column_incremental_key_supported()},
        {'id': 'offload_story_force_hdfs_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_DIM,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, OFFLOAD_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_DIM)]}},
        {'id': 'offload_story_force_hdfs',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension Forcing Location as HDFS',
         'narrative': 'Offload the dimension forcing to HDFS which only really matters for S3 (and the like) testing.',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True},
         'config_override': {'offload_fs_scheme': OFFLOAD_FS_SCHEME_HDFS,
                             'offload_fs_container': options.offload_fs_container if options else None,
                             'offload_fs_prefix': options.offload_fs_prefix if options else None},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, OFFLOAD_DIM),
              lambda test: True),
             (lambda test: base_table_fs_scheme_is_correct(data_db, OFFLOAD_DIM, options, backend_api,
                                                           option_fs_scheme_override=OFFLOAD_FS_SCHEME_HDFS),
              lambda test: True)],
         'prereq': lambda: offload_story_force_hdfs_prereq(options, backend_api)},
        #{'id': 'offload_story_dim_no_part_funcs',
        # 'type': STORY_TYPE_OFFLOAD,
        # 'title': 'Offload With Partition Functions When Not Supported',
        # 'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
        #             'reset_backend_table': True,
        #             'offload_partition_functions': 'anything'},
        # 'expected_exception_string': PARTITION_FUNCTIONS_NOT_SUPPORTED_EXCEPTION_TEXT,
        # 'prereq': lambda: not backend_api.gluent_partition_functions_supported()},
        {'id': 'offload_story_dim_verify_parallel1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Verification Parallelism=3',
         'narrative': 'Disables data sampling to minimize risk of other hints being matched',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'verify_parallelism': 3,
                     'data_sample_pct': 0,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: hint_text_in_log(options, 3, '(offload_story_dim_verify_parallel1)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ],
         'prereq': lambda: options.db_type == DBTYPE_ORACLE},
        {'id': 'offload_story_dim_verify_parallel2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Verification Parallelism=4',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'verify_parallelism': 4,
                     'data_sample_pct': 0,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: hint_text_in_log(options, 4, '(offload_story_dim_verify_parallel2)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ]},
        {'id': 'offload_story_dim_verify_parallel3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Verification Parallelism=1',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'verify_parallelism': 1,
                     'data_sample_pct': 0,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: hint_text_in_log(options, 1, '(offload_story_dim_verify_parallel3)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ]},
        {'id': 'offload_story_dim_verify_parallel4',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Agg Validate Parallelism=0',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'verify_row_count': 'aggregate',
                     'verify_parallelism': 0,
                     'data_sample_pct': 0,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: hint_text_in_log(options, 0, '(offload_story_dim_verify_parallel4)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ]},
        {'id': 'offload_story_dim_verify_parallel5',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Agg Validate Parallelism=1',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'verify_row_count': 'aggregate',
                     'verify_parallelism': 1,
                     'data_sample_pct': 0,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: hint_text_in_log(options, 1, '(offload_story_dim_verify_parallel5)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ]},
        {'id': 'offload_story_dim_verify_parallel6',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Agg Validate Parallelism=3',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'verify_row_count': 'aggregate',
                     'verify_parallelism': 3,
                     'data_sample_pct': 0,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: hint_text_in_log(options, 3, '(offload_story_dim_verify_parallel6)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ]},
        {'id': 'offload_story_not_null_setup',
         'type': STORY_TYPE_SETUP,
         'title': f'Create {NOT_NULL_DIM}',
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_not_null_table_ddl(options, frontend_api,
                                                                             schema, NOT_NULL_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, NOT_NULL_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, NOT_NULL_DIM)]}},
        {'id': 'offload_story_not_null_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With NOT NULL Columns',
         'narrative': 'Ensure NOT NULL is propagated to backend.',
         'options': {'owner_table': f'{schema}.{NOT_NULL_DIM}',
                     'reset_backend_table': True},
         'config_override': {'not_null_propagation': NOT_NULL_PROPAGATION_AUTO},
         'assertion_pairs': [
             (lambda test: offload_not_null_assertion(data_db, NOT_NULL_DIM, options, backend_api),
              lambda test: True),
         ]},
        {'id': 'offload_story_not_null_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With NOT NULL Columns',
         'narrative': 'Ensure NOT NULL is propagated to backend.',
         'options': {'owner_table': f'{schema}.{NOT_NULL_DIM}',
                     'reset_backend_table': True},
         'config_override': {'not_null_propagation': NOT_NULL_PROPAGATION_NONE},
         'assertion_pairs': [
             (lambda test: offload_not_null_assertion(data_db, NOT_NULL_DIM, options, backend_api,
                                                      not_null_col_list=[]),
              lambda test: True),
         ]},
        {'id': 'offload_story_not_null_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With --not-null-columns',
         'narrative': 'Ensure NOT NULL is propagated to backend.',
         'options': {'owner_table': f'{schema}.{NOT_NULL_DIM}',
                     'not_null_columns_csv': 'DT*',
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_not_null_assertion(data_db, NOT_NULL_DIM, options, backend_api,
                                                      not_null_col_list=['DT', 'DT_NN']),
              lambda test: True),
         ]},
        {'id': 'offload_story_not_null_offload4',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With --not-null-columns and --integer-8-columns',
         'narrative': """Ensure data type controls do not interfere with NOT NULL propagation.
                         Only NUM should be NOT NULL, not NUM_NN""",
         'options': {'owner_table': f'{schema}.{NOT_NULL_DIM}',
                     'integer_8_columns_csv': 'NUM*',
                     'not_null_columns_csv': 'NUM',
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_not_null_assertion(data_db, NOT_NULL_DIM, options, backend_api,
                                                      not_null_col_list=['NUM']),
              lambda test: True),
         ]},
        {'id': 'offload_story_not_null_offload_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid --not-null-columns',
         'options': {'owner_table': f'{schema}.{NOT_NULL_DIM}',
                     'not_null_columns_csv': 'not-a-column',
                     'reset_backend_table': True},
         'expected_exception_string': UNKNOWN_NOT_NULL_COLUMN_EXCEPTION_TEXT,
         'prereq': lambda: backend_api.not_null_column_supported()},
        {'id': 'offload_story_not_null_offload_fail2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload NOT NULL With NULLs in Column',
         'options': {'owner_table': f'{schema}.{NOT_NULL_DIM}',
                     'not_null_columns_csv': 'With_Nulls',
                     'reset_backend_table': True},
         'expected_exception_string': DATA_VALIDATION_NOT_NULL_EXCEPTION_TEXT,
         'prereq': lambda: backend_api.not_null_column_supported()},
        {'id': 'offload_story_xmltype_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create a table with XMLTYPE',
         'setup': {STORY_SETUP_TYPE_ORACLE:
                       ['DROP TABLE %(schema)s.%(table)s' % {'schema': schema, 'table': XMLTYPE_TABLE},
                        """CREATE TABLE %(schema)s.%(table)s
                           (id NUMBER(8), data XMLTYPE)"""
                        % {'schema': schema, 'table': XMLTYPE_TABLE},
                        """INSERT INTO %(schema)s.%(table)s (id, data)
                           SELECT ROWNUM, SYS_XMLGEN(table_name) FROM all_tables WHERE ROWNUM <= 10""" \
                        % {'schema': schema, 'table': XMLTYPE_TABLE},
                        dbms_stats_gather_string(schema, XMLTYPE_TABLE, frontend_api=frontend_api)],
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, XMLTYPE_TABLE)]},
         'prereq': lambda: options.db_type == DBTYPE_ORACLE},
        {'id': 'offload_story_xmltype_offload1',
         'title': "Offload XMLTYPE (Query Import), expect to fail",
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload XMLTYPE Table',
         'options': {'owner_table': schema + '.' + XMLTYPE_TABLE,
                     'reset_backend_table': True},
         'prereq': lambda: options.db_type == DBTYPE_ORACLE},
        {'id': 'offload_story_xmltype_offload2',
         'title': "Offload XMLTYPE (no Query Import), expect to fail",
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload XMLTYPE Table',
         'options': {'owner_table': schema + '.' + XMLTYPE_TABLE,
                     'offload_transport_method': no_query_import_transport_method(options, no_table_centric_sqoop=True),
                     'reset_backend_table': True},
         'prereq': lambda: options.db_type == DBTYPE_ORACLE},
    ]
