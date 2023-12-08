""" Tests these Schema Sync scenarios:
            schema_sync_00: Dimension (no Oracle stats). Offload (no backend stats), add cols, schema sync (no-gather-stats), add cols, schema sync (gather stats and sample)
            schema_sync_01: Fact (partitioned). Offload, add cols, schema sync, offload next partition.
                            Repeat for a number of data type sets ensuring good type coverage.
            schema_sync_09: Dimension with 995 columns. Offload. add cols. schema sync.
"""
from goe.offload.offload_constants import (
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_SYNAPSE,
    OFFLOAD_STATS_METHOD_NONE,
)
from goe.offload.offload_functions import convert_backend_identifier_case
from goe.orchestration import command_steps
from goe.orchestration.command_steps import step_title

from test_sets.stories.story_assertion_functions import (
    backend_column_exists,
    get_max_bucket_id,
    messages_step_executions,
    minus_column_spec_count,
    offload_fact_assertions,
    rdbms_count_minus_backend_count,
    text_in_log,
    text_in_messages,
)
from test_sets.stories.story_globals import (
    STORY_TYPE_OFFLOAD,
    STORY_SETUP_TYPE_ORACLE,
    STORY_TYPE_SCHEMA_SYNC,
    STORY_TYPE_SETUP,
)
from test_sets.stories.story_setup_functions import (
    SALES_BASED_FACT_HV_1,
    SALES_BASED_FACT_HV_2,
    SALES_BASED_FACT_HV_3,
    SALES_BASED_FACT_HV_4,
    STORY_SETUP_TYPE_HYBRID,
    STORY_SETUP_TYPE_PYTHON,
    dbms_stats_delete_string,
    drop_backend_test_table,
    gen_hybrid_drop_ddl,
    gen_incr_update_part_ddl,
    gen_rdbms_dim_create_ddl,
    gen_sales_based_fact_create_ddl,
    gen_variable_columns_dim_ddl,
    get_backend_drop_incr_fns,
)
from testlib.test_framework.test_functions import log

ESTIMATE_STATS_PERCENTAGE = float(5)
TABLE_00 = 'schema_sync_00'
TABLE_01 = 'schema_sync_01'
TABLE_09 = 'schema_sync_09'

NUM_BUCKETS = {TABLE_01: 1}


def schema_sync_init_offload_dim_opts(schema, table, num_buckets=None, num_location_files=None):
    opts = {'owner_table'         : schema + '.' + table,
            'reset_backend_table' : True,
            'offload_stats_method': OFFLOAD_STATS_METHOD_NONE}
    if num_buckets:
        opts['num_buckets'] = num_buckets
    if num_location_files:
        opts['num_location_files'] = num_location_files
    return opts


def setup_dim_opts(options, backend_api, frontend_api, schema, hybrid_schema, data_db, table_name,
                   drop_incr=False, rowdependencies=False):
    backend_name = convert_backend_identifier_case(options, table_name)
    return {'oracle': gen_incr_update_part_ddl(schema, table_name, rowdependencies=rowdependencies),
            'hybrid': gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, table_name, drop_incr=drop_incr),
            'python': get_backend_drop_incr_fns(options, backend_api, data_db, backend_name)}


def schema_sync_add_cols_setup(schema, table, col1_type, col2_type, col3_type, col4_type, col5_type, col_offset=0):
    col_names = [f'newcol{_+col_offset+1}' for _ in range(5)]
    return 'ALTER TABLE %s.%s ADD (%s %s, %s %s, %s %s, %s %s, %s %s)' \
           % (schema, table, col_names[0], col1_type, col_names[1], col2_type, col_names[2], col3_type,
              col_names[3], col4_type, col_names[4], col5_type)


def schema_sync_run_2_assertion(test, options, backend_api, test_cursor, schema, hybrid_schema, data_db):
    if not options:
        return True

    for check_table in [TABLE_01]:
        minus_count = minus_column_spec_count(test, test.name, schema, check_table, hybrid_schema, check_table)
        if minus_count != 0:
            log('minus_column_spec_count(%s, %s) != 0: %s' % (check_table, check_table, minus_count))
            return False
        ext_minus_count = minus_column_spec_count(test, test.name, schema, check_table,
                                                  hybrid_schema, check_table + '_ext')
        if ext_minus_count != 0:
            log('minus_column_spec_count(%s, %s) != 0: %s' % (check_table, check_table + '_ext', ext_minus_count))
            return False

    return True


def schema_sync_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                            test_cursor, repo_client):
    """ The tests have been named/numbered in a way to make individual test cases easy to test
        Except for the last test, schema_sync_run_2, which runs multiple tables in the same command

        Example test command for a single test table:
            ./test --set=stories --story=schema_sync -v --filter=schema_sync_03

        The test tables test these scenarios:
            schema_sync_00: Dimension (no Oracle stats). Offload (no backend stats), add cols, schema sync (no-gather-stats), add cols, schema sync (gather stats and sample)
            schema_sync_01: Fact (partitioned). Offload, add cols, schema sync, offload next partition
                            Repeat for a number of data type sets ensuring good type coverage.
            schema_sync_09: Dimension with 995 columns. Offload. add cols. schema sync.

        SS@13-05-2020: The following test tables have prereqs:
            1. schema_sync_00
                prereq: backend.table_stats_compute_supported.
                reason: Using table_stats_compute_supported() to indicate whether it's possible to have no stats on a table.

        SS@17-09-2021: The following test tables had prereqs added:
            1. schema_sync_09
                prereq: backend is not DBTYPE_SYNAPSE.
                reason: Creates a 995 column table which causes a system limit to be breached in Synapse:
                        Creating or altering table 'Table_021f6d26ab1047bea48ff206247ceb16_8' failed because the minimum row size would be 11507, including 2558 bytes of internal overhead.
                            This exceeds the maximum allowable table row size of 8060 bytes
    """
    if backend_api and not backend_api.schema_evolution_supported():
        log('Skipping schema_sync stories because schema_evolution_supported() == False')
        return []

    if frontend_api and not frontend_api.gluent_schema_sync_supported():
        log('Skipping schema_sync stories because gluent_schema_sync_supported() == False')
        return []

    table_00_be = convert_backend_identifier_case(options, TABLE_00)
    table_01_be = convert_backend_identifier_case(options, TABLE_01)
    table_09_be = convert_backend_identifier_case(options, TABLE_09)
    if backend_api and backend_api.backend_type() in [DBTYPE_IMPALA, DBTYPE_HIVE]:
        partition_granularity = 'M'
    else:
        partition_granularity = 'D'
    return [
        {'id'             : 'schema_sync_00_setup',
         'type'           : STORY_TYPE_SETUP,
         'title'          : 'Create schema_sync_00 table with no Oracle statistics',
         'prereq'         : lambda: backend_api and backend_api.table_stats_compute_supported(),
         'setup'          : {'oracle': gen_rdbms_dim_create_ddl(frontend_api, schema, TABLE_00) +
                                       [dbms_stats_delete_string(schema, TABLE_00)],
                             'hybrid': gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, TABLE_00),
                             'python': [lambda: drop_backend_test_table(options, backend_api,
                                                                        data_db, TABLE_00)]
                            },
         'assertion_pairs': [
             (lambda test: bool(frontend_api.table_row_count_from_stats(hybrid_schema, '%s_ext' % TABLE_00) is None),
              lambda test: True)]},
        {'id'             : 'schema_sync_00_offload_init',
         'type'           : STORY_TYPE_OFFLOAD,
         'title'          : 'Initial Offload of schema_sync_00',
         'prereq'         : lambda: backend_api and backend_api.table_stats_compute_supported(),
         'narrative'      : 'Offloads schema_sync_00 table. Buckets and location files are also set.',
         'options'        : schema_sync_init_offload_dim_opts(schema, TABLE_00, num_buckets=3,
                                                              num_location_files=8),
         'assertion_pairs': [(lambda test: get_max_bucket_id(backend_api, data_db, table_00_be),
                              lambda test: 2 if backend_api.synthetic_bucketing_supported() else None)]},
        {'id'    : 'schema_sync_00_add_cols_1',
         'type'  : STORY_TYPE_SETUP,
         'title' : 'Add columns to schema_sync_00 table',
         'prereq': lambda: backend_api and backend_api.table_stats_compute_supported(),
         'setup' : {'oracle': [
             schema_sync_add_cols_setup(schema, TABLE_00, 'NUMBER', 'NUMBER(*)', 'NUMBER(*,10)', 'NUMBER(10)',
                                        'NUMBER(10,10)')]}},
        {'id'             : 'schema_sync_00_run_1',
         'type'           : STORY_TYPE_SCHEMA_SYNC,
         'title'          : 'Schema Sync Run 1',
         'prereq'         : lambda: backend_api and backend_api.table_stats_compute_supported(),
         'options'        : {'include'                 : schema + '.' + TABLE_00 + '*',
                             'command_file'            : None},
         'assertion_pairs': [
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_00, hybrid_schema,
                                                   TABLE_00), lambda test: 0),
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_00, hybrid_schema,
                                                   TABLE_00 + '_ext'), lambda test: 0),
             (lambda test: backend_column_exists(backend_api, data_db, table_00_be, 'newcol1'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_00_be, 'newcol2'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_00_be, 'newcol3'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_00_be, 'newcol4'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_00_be, 'newcol5'), lambda test: True),
         ]},
        {'id'    : 'schema_sync_00_add_cols_2',
         'type'  : STORY_TYPE_SETUP,
         'title' : 'Add more columns to schema_sync_00 table',
         'prereq': lambda: backend_api and backend_api.table_stats_compute_supported(),
         'setup' : {'oracle': ["ALTER TABLE %s.%s ADD (newcol6 %s)" % (schema, TABLE_00, 'NUMBER(2)')]}},
        {'id'             : 'schema_sync_00_run_2',
         'type'           : STORY_TYPE_SCHEMA_SYNC,
         'title'          : 'Run Schema Sync with the --sample-stats options. The hybrid ext table should have stats after this step',
         'prereq'         : lambda: backend_api and backend_api.table_stats_compute_supported(),
         'options'        : {'include'        : schema + '.' + TABLE_00 + '*',
                             'command_file'   : None,
                             'no_gather_stats': False,
                             'sample_stats'   : ESTIMATE_STATS_PERCENTAGE},
         'assertion_pairs': [
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_00, hybrid_schema,
                                                   TABLE_00), lambda test: 0),
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_00, hybrid_schema,
                                                   TABLE_00 + '_ext'), lambda test: 0),
             (lambda test: backend_column_exists(backend_api, data_db, table_00_be, 'newcol6'), lambda test: True),
             (lambda test: messages_step_executions(test.offload_messages, step_title(command_steps.STEP_GET_BACKEND_STATS)),
              lambda test: 1),
             # SS@2021-10-01 Revisit this as part of GOE-2135 which should introduce a new present message title
             (lambda test: messages_step_executions(test.offload_messages,
                                                    step_title(command_steps.STEP_ESTIMATING_STATS)),
              lambda test: 0 if backend_api and backend_api.backend_type() == DBTYPE_SYNAPSE else 1),
             (lambda test: bool(frontend_api.table_row_count_from_stats(hybrid_schema, '%s_ext' % TABLE_00) is None),
              lambda test: False)
         ]},
        {'id': 'schema_sync_01_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create schema_sync_01 table',
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_sales_based_fact_create_ddl(frontend_api, schema, TABLE_01),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, TABLE_01),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db, TABLE_01)]}},
        {'id': 'schema_sync_01_offload_init',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Initial Offload of schema_sync_01',
         'narrative': """Offloads some partitions from schema_sync_01 table.
                         The fact is partitioned by a single column with appropriate granularity.
                         Buckets and location files are also set.""",
         'options': {'owner_table': schema + '.' + TABLE_01,
                     'reset_backend_table': True,
                     'offload_partition_granularity': partition_granularity,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_NONE,
                     'older_than_date': SALES_BASED_FACT_HV_1,
                     'num_buckets': NUM_BUCKETS[TABLE_01],
                     'num_location_files': 8},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, TABLE_01, SALES_BASED_FACT_HV_1,
                                                    bucket_check=1, num_loc_files=8) +
                            [(lambda test: text_in_messages(test.offload_messages,
                                                            'No backend stats due to --offload-stats: NONE'),
                              lambda test: True)]},
        {'id': 'schema_sync_01_add_cols1',
         'type': STORY_TYPE_SETUP,
         'title': 'Add numeric columns to schema_sync_01 table',
         'setup': {'oracle': [
             schema_sync_add_cols_setup(schema, TABLE_01, 'NUMBER', 'NUMBER(22)', 'NUMBER(15,2)',
                                        'FLOAT', 'BINARY_DOUBLE')]}},
        {'id': 'schema_sync_01_run_0',
         'type': STORY_TYPE_SCHEMA_SYNC,
         'title': 'Run Schema Sync in verification mode',
         'options': {'include': schema + '.' + TABLE_01 + '*',
                     'no_gather_stats': True,
                     'execute': False},
         'assertion_pairs': [
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema,
                                                   TABLE_01), lambda test: 4),
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema,
                                                   'schema_sync_01_ext'), lambda test: 4),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol1'), lambda test: False),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol2'), lambda test: False),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol3'), lambda test: False),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol4'), lambda test: False),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol5'), lambda test: False),
         ]},
        {'id': 'schema_sync_01_run_1',
         'type': STORY_TYPE_SCHEMA_SYNC,
         'title': 'Run Schema Sync with the command_file option',
         'options': {'include': schema + '.' + TABLE_01 + '*',
                     'command_file': schema + '_schema_sync.cmd',
                     'no_gather_stats': True,
                     'sample_stats': ESTIMATE_STATS_PERCENTAGE},
         'assertion_pairs': [
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema, TABLE_01),
              lambda test: 0),
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema,
                                                   'schema_sync_01_ext'),
              lambda test: 0),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol1'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol2'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol3'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol4'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol5'), lambda test: True),
             (lambda test: messages_step_executions(test.offload_messages,
                                                    step_title(command_steps.STEP_GET_BACKEND_STATS)),
              lambda test: 0),
             (lambda test: messages_step_executions(test.offload_messages,
                                                    step_title(command_steps.STEP_ESTIMATING_STATS)),
              lambda test: 0)
         ]},
        {'id': 'schema_sync_01_offload_ipa1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Offload of schema_sync_01 table',
         'narrative': 'Offloads next partition from fact table.',
         'options': {'owner_table': schema + '.' + TABLE_01,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'allow_floating_point_conversions': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, TABLE_01, SALES_BASED_FACT_HV_2,
                                                    bucket_check=1, num_loc_files=8)},
        {'id': 'schema_sync_01_add_cols2',
         'type': STORY_TYPE_SETUP,
         'title': 'Add string columns to schema_sync_01 table',
         'setup': {STORY_SETUP_TYPE_ORACLE: [
             schema_sync_add_cols_setup(schema, TABLE_01, 'VARCHAR2(10)', 'NVARCHAR2(10)', 'CHAR(2)',
                                        'NCHAR(2)', 'RAW(10)', col_offset=5)]}},
        {'id': 'schema_sync_01_run_2',
         'type': STORY_TYPE_SCHEMA_SYNC,
         'title': 'Run Schema Sync',
         'options': {'include': schema + '.' + TABLE_01},
         'assertion_pairs': [
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema, TABLE_01),
              lambda test: 0),
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema,
                                                   'schema_sync_01_ext'),
              lambda test: 0),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol6'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol7'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol8'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol9'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol10'), lambda test: True),
         ]},
        {'id': 'schema_sync_01_offload_ipa2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Offload of schema_sync_01 table',
         'narrative': 'Offloads next partition from fact table.',
         'options': {'owner_table': schema + '.' + TABLE_01,
                     'older_than_date': SALES_BASED_FACT_HV_3,
                     'allow_floating_point_conversions': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, TABLE_01, SALES_BASED_FACT_HV_3)},
        {'id': 'schema_sync_01_add_cols3',
         'type': STORY_TYPE_SETUP,
         'title': 'Add datetime columns to schema_sync_01 table',
         'setup': {STORY_SETUP_TYPE_ORACLE: [
             schema_sync_add_cols_setup(schema, TABLE_01, 'DATE', 'TIMESTAMP(3)', 'TIMESTAMP(3) WITH TIME ZONE',
                                        'INTERVAL DAY(5) TO SECOND(3)', 'INTERVAL YEAR(9) TO MONTH',
                                        col_offset=10)]}},
        {'id': 'schema_sync_01_run_3',
         'type': STORY_TYPE_SCHEMA_SYNC,
         'title': 'Run Schema Sync',
         'options': {'include': schema + '.' + TABLE_01},
         'assertion_pairs': [
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema, TABLE_01),
              lambda test: 0),
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema,
                                                   'schema_sync_01_ext'),
              lambda test: 0),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol11'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol12'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol13'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol14'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol15'), lambda test: True),
         ]},
        {'id': 'schema_sync_01_offload_ipa3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Offload of schema_sync_01 table',
         'narrative': 'Offloads next partition from fact table.',
         'options': {'owner_table': schema + '.' + TABLE_01,
                     'older_than_date': SALES_BASED_FACT_HV_4,
                     'allow_floating_point_conversions': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, TABLE_01, SALES_BASED_FACT_HV_4)},
        {'id': 'schema_sync_01_add_cols4',
         'type': STORY_TYPE_SETUP,
         'title': 'Add LOB columns to schema_sync_01 table (not for Synapse)',
         'narrative': 'Exclude Synapse due to: Column x has a data type that cannot participate in a columnstore index',
         'setup': {STORY_SETUP_TYPE_ORACLE: [
             schema_sync_add_cols_setup(schema, TABLE_01, 'CLOB', 'BLOB', 'NCLOB', 'NUMBER(1)', 'NUMBER(1)',
                                        col_offset=15)]},
         'prereq': lambda : options.target != DBTYPE_SYNAPSE},
        {'id': 'schema_sync_01_run_4',
         'type': STORY_TYPE_SCHEMA_SYNC,
         'title': 'Run Schema Sync with the command_file option',
         'options': {'include': schema + '.' + TABLE_01,
                     'command_file': schema + '_schema_sync.cmd'},
         'assertion_pairs': [
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema, TABLE_01),
              lambda test: 0),
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema,
                                                   'schema_sync_01_ext'),
              lambda test: 0),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol16'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol17'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol18'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol19'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_01_be, 'newcol20'), lambda test: True),
         ],
         'prereq': lambda : options.target != DBTYPE_SYNAPSE},
        {'id': 'schema_sync_01_run_5',
         'type': STORY_TYPE_SCHEMA_SYNC,
         'title': 'Run no-op Schema Sync without the command_file option',
         'options': {'include': schema + '.' + TABLE_01 + '*',
                     'no_gather_stats': True,
                     'sample_stats': ESTIMATE_STATS_PERCENTAGE},
         'assertion_pairs': [
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema,
                                                   TABLE_01), lambda test: 0),
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_01, hybrid_schema,
                                                   'schema_sync_01_ext'), lambda test: 0)]
         },
        {'id'    : 'schema_sync_09_setup',
         'type'  : STORY_TYPE_SETUP,
         'title' : 'Create schema_sync_09 table with 995 columns',
         'setup' : {'oracle': gen_variable_columns_dim_ddl(schema, TABLE_09, 995),
                    'hybrid': gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, TABLE_09),
                    'python': [lambda: drop_backend_test_table(options, backend_api, data_db, TABLE_09),
                               lambda: repo_client.drop_offload_metadata(hybrid_schema, TABLE_09)]
                    }},
        {'id'             : 'schema_sync_09_offload_init',
         'type'           : STORY_TYPE_OFFLOAD,
         'title'          : 'Initial Offload of schema_sync_09',
         'narrative'      : 'Offloads schema_sync_09 table. Buckets and location files are also set.',
         'options'        : schema_sync_init_offload_dim_opts(schema, TABLE_09, num_buckets=4, num_location_files=9),
         'config_override': {'num_buckets_max': 8},
         'prereq': lambda : options.target != DBTYPE_SYNAPSE},
        {'id'    : 'schema_sync_09_add_cols',
         'type'  : STORY_TYPE_SETUP,
         'title' : 'Add columns to schema_sync_09 table',
         'setup' : {'oracle': [schema_sync_add_cols_setup(schema, TABLE_09, 'BINARY_FLOAT', 'DATE', 'TIMESTAMP',
                                                          'INTERVAL DAY(5) TO SECOND(3)', 'DATE')]},
         'prereq': lambda : options.target != DBTYPE_SYNAPSE},
        {'id'             : 'schema_sync_09_run_1',
         'type'           : STORY_TYPE_SCHEMA_SYNC,
         'title'          : 'Run Schema Sync. Should succeed as even though we are over the 998 column limit, IU is not enabled',
         'options'        : {'include'     : schema + '.' + TABLE_09 + '*',
                             'command_file': schema + '_schema_sync.cmd'},
         'assertion_pairs': [
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_09, hybrid_schema,
                                                   TABLE_09), lambda test: 0),
             (lambda test: minus_column_spec_count(test, test.name, schema, TABLE_09, hybrid_schema,
                                                   'schema_sync_09_ext'), lambda test: 0),
             (lambda test: backend_column_exists(backend_api, data_db, table_09_be, 'newcol1'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_09_be, 'newcol2'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_09_be, 'newcol3'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_09_be, 'newcol4'), lambda test: True),
             (lambda test: backend_column_exists(backend_api, data_db, table_09_be, 'newcol5'), lambda test: True),
             (lambda test: rdbms_count_minus_backend_count(schema, TABLE_09, data_db, table_09_be,
                                                           test_cursor, backend_api), lambda test: 0)],
         'prereq': lambda : options.target != DBTYPE_SYNAPSE},
        {'id'             : 'schema_sync_run_2',
         'type'           : STORY_TYPE_SCHEMA_SYNC,
         'title'          : 'Run Schema Sync for all tables in this story (i.e. wildcarded include option)',
         'prereq'         : lambda: bool(backend_api.incremental_update_supported()),
         'options'        : {'include'     : schema + '.schema_sync*',
                             'command_file': schema + '_schema_sync_2.cmd'},
         'assertion_pairs': [
             (lambda test: schema_sync_run_2_assertion(test, options, backend_api, test_cursor,
                                                       schema, hybrid_schema, data_db),
              lambda test: True)
        ]
        }
    ]
