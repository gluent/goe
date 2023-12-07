from distutils.version import StrictVersion

from test_sets.stories.story_globals import STORY_TYPE_LINK_ID, STORY_TYPE_OFFLOAD, \
    STORY_TYPE_SETUP, STORY_TYPE_SHOW_BACKEND_STATS, STORY_SETUP_TYPE_FRONTEND, STORY_SETUP_TYPE_HYBRID, \
    STORY_SETUP_TYPE_PYTHON
from test_sets.stories.story_setup_functions import drop_backend_test_table, gen_hybrid_drop_ddl,\
    gen_rdbms_dim_create_ddl, gen_sales_based_fact_create_ddl, gen_sales_based_list_create_ddl,\
    gen_truncate_sales_based_fact_partition_ddls, \
    SALES_BASED_FACT_HV_1, SALES_BASED_FACT_HV_2, SALES_BASED_LIST_HV_1, SALES_BASED_LIST_HV_3
from test_sets.stories.story_assertion_functions import offload_lpa_fact_assertion, partition_has_stats, \
    synthetic_part_col_name, table_has_stats, text_in_warnings

from goe.gluent import OFFLOAD_STATS_COPY_EXCEPTION_TEXT
from goe.config import orchestration_defaults
from goe.offload.offload_constants import DBTYPE_IMPALA, DBTYPE_HIVE, OFFLOAD_BUCKET_NAME, \
    OFFLOAD_STATS_METHOD_NONE, OFFLOAD_STATS_METHOD_NATIVE, OFFLOAD_STATS_METHOD_COPY, \
    OFFLOAD_STATS_METHOD_HISTORY, HADOOP_BASED_BACKEND_DISTRIBUTIONS
from goe.offload.offload_functions import convert_backend_identifier_case
from goe.offload.offload_metadata_functions import INCREMENTAL_PREDICATE_TYPE_LIST
from goe.orchestration import command_steps
from goe.orchestration.command_steps import step_title


LPA_FACT_TABLE = 'BACKEND_STATS_LFACT'
BACKEND_STATS_DIM1 = 'BACKEND_STATS_DIM_1'
BACKEND_STATS_DIM2 = 'BACKEND_STATS_DIM_2'
BACKEND_STATS_FACT1 = 'HADOOP_STATS_FACT_1'
BACKEND_STATS_FACT2 = 'HADOOP_STATS_FACT_2'


def should_have_column_stats(backend_api, ver_str, options, stats_method, hive_column_stats=False):
    """ We expect column stats if:
        1. Hive and --hive-column-stats
        2. Impala and --offload-stats-method=NATIVE/HISTORY
        3. Impala and --offload-stats-method=COPY and Impala version >=2.6.0
    """
    if not options:
        return False
    target_version = backend_api.target_version()
    target = options.target
    if (target == DBTYPE_HIVE and hive_column_stats) or \
       (target == DBTYPE_IMPALA and stats_method in [OFFLOAD_STATS_METHOD_NATIVE, OFFLOAD_STATS_METHOD_HISTORY]) or \
       (target == DBTYPE_IMPALA and stats_method == OFFLOAD_STATS_METHOD_COPY and StrictVersion(target_version) >= StrictVersion(ver_str)):
        return True
    else:
        return False


def copy_stats_supported(backend_api):
    return bool(backend_api and backend_api.table_stats_get_supported() and backend_api.table_stats_set_supported())


def copy_stats_expected_exception_string(backend_api):
    return OFFLOAD_STATS_COPY_EXCEPTION_TEXT if not copy_stats_supported(backend_api) else None


def backend_stats_story_tests(schema, hybrid_schema, data_db, options, backend_api,
                              frontend_api, list_only, repo_client):
    if not list_only and not options:
        return []

    option_target = options.target if options else None

    if backend_api and not backend_api.table_stats_set_supported() and not backend_api.table_stats_compute_supported():
        # If we can't control backend stats then these tests have no purpose
        return []

    lpa_fact_table_be = convert_backend_identifier_case(options, LPA_FACT_TABLE)
    backend_stats_dim1_be = convert_backend_identifier_case(options, BACKEND_STATS_DIM1)
    backend_stats_fact1_be = convert_backend_identifier_case(options, BACKEND_STATS_FACT1)
    def_synth_part_digits = orchestration_defaults.synthetic_partition_digits_default()

    dim_fact_setup = [
        {'id': 'backend_stats_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create dimension tables',
         'setup': {
             STORY_SETUP_TYPE_FRONTEND: lambda: (gen_rdbms_dim_create_ddl(frontend_api, schema, BACKEND_STATS_DIM1)
                                                 + gen_rdbms_dim_create_ddl(frontend_api, schema, BACKEND_STATS_DIM2)),
             STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, BACKEND_STATS_DIM1) +
                                      gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, BACKEND_STATS_DIM2),
             STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db, BACKEND_STATS_DIM1),
                                       lambda: drop_backend_test_table(options, backend_api, data_db, BACKEND_STATS_DIM2)]}},
        {'id': 'backend_stats_fact_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create fact tables',
         'setup': {
             STORY_SETUP_TYPE_FRONTEND: lambda: (gen_sales_based_fact_create_ddl(frontend_api, schema, BACKEND_STATS_FACT1)
                                                 + gen_sales_based_fact_create_ddl(frontend_api, schema, BACKEND_STATS_FACT2)),
             STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, BACKEND_STATS_FACT1) +
                                      gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, BACKEND_STATS_FACT2),
             STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db, BACKEND_STATS_FACT1),
                                       lambda: drop_backend_test_table(options, backend_api, data_db, BACKEND_STATS_FACT2)]}}
    ]

    common_stories = [
        {'id': 'backend_stats_dim_stats_none',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of %s' % BACKEND_STATS_DIM1,
         'narrative': 'Offload BACKEND_STATS_DIM1 table. Do not create aggregations or sample data to save time. Use --offload-stats=NONE and assert that the table has no stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_DIM1,
                     'reset_backend_table': True,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_NONE,
                     'num_buckets': 1,
                     'data_sample_pct': 0},
         'assertion_pairs': [
             (lambda test: table_has_stats(backend_api, data_db, backend_stats_dim1_be), lambda test: False)]
         },
        {'id': 'backend_stats_dim_stats_native',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of %s' % BACKEND_STATS_DIM1,
         'narrative': 'Offload BACKEND_STATS_DIM1 table. Do not create aggregations or sample data to save time. Use default --offload-stats=NATIVE and assert that the table has stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_DIM1,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'data_sample_pct': 0},
         'assertion_pairs': [
             (lambda test: table_has_stats(backend_api, data_db, backend_stats_dim1_be), lambda test: True)]
         }
    ]

    """ These stories rely on partition stats being exposed for assertions which is the case for Hadoop
        based backends only. The statistics code and the testing of it was written before we supported Cloud
        backends and was tied into partitions being able to have stats or not depending on the options
        passed into offload during an IPA operation. Our currently (2021-09) supported Cloud backends do
        not fit this model. Preserving these stories here but they only make sense on Hive/Impala.
    """
    hadoop_stories = [
        {'id': 'backend_stats_none_1_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of %s' % BACKEND_STATS_DIM1,
         'narrative': 'Offload BACKEND_STATS_DIM1 table. Do not create aggregations or sample data to save time. Use --offload-stats=NONE and assert that no partitions or columns have stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_DIM1,
                     'reset_backend_table': True,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_NONE,
                     'num_buckets': 1,
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_none_1_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_DIM1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], colstats=False), lambda test: False),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], colstats=True), lambda test: False)]},
        {'id': 'backend_stats_none_2_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of BACKEND_STATS_FACT1',
         'narrative': 'Offload some partitions from BACKEND_STATS_FACT1 table. Do not create aggregations or sample data to save time. Use --offload-stats=NONE and assert that no partitions or columns have stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_FACT1,
                     'reset_backend_table': True,
                     'offload_partition_columns': 'time_id',
                     'offload_partition_granularity': 'M',
                     'offload_stats_method': OFFLOAD_STATS_METHOD_NONE,
                     'older_than_date': '2012-02-01',
                     'num_buckets': 1,
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_none_2_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_FACT1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], False),
              lambda test: False),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], True),
              lambda test: False)]},
        {'id': 'backend_stats_copy_1_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_dim_setup'},
        {'id': 'backend_stats_copy_1_offload_verification',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of %s' % BACKEND_STATS_DIM1,
         'narrative': 'Offload BACKEND_STATS_DIM1 table. Do not create aggregations or sample data to save time. Use --offload-stats=COPY and assert that backend partitions have partition and column stats if Impala version 2.6.0 or greater.',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_DIM1,
                     'reset_backend_table': True,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_COPY,
                     'num_buckets': 1,
                     'data_sample_pct': 0},
         'config_override': {'execute': False},
         'expected_exception_string': copy_stats_expected_exception_string(backend_api)},
        {'id': 'backend_stats_copy_1_offload_run',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of %s' % BACKEND_STATS_DIM1,
         'narrative': 'Offload BACKEND_STATS_DIM1 table. Do not create aggregations or sample data to save time. Use --offload-stats=COPY and assert that backend partitions have partition and column stats if Impala version 2.6.0 or greater.',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_DIM1,
                     'reset_backend_table': True,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_COPY,
                     'num_buckets': 1,
                     'data_sample_pct': 0},
         'expected_exception_string': copy_stats_expected_exception_string(backend_api)},
        {'id': 'backend_stats_copy_1_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_DIM1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], colstats=False), lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], colstats=True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_COPY))],
         'prereq': lambda: copy_stats_supported(backend_api)},
        {'id': 'backend_stats_copy_2_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_fact_setup'},
        {'id': 'backend_stats_copy_2_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of BACKEND_STATS_FACT1',
         'narrative': 'Offloads some partitions from BACKEND_STATS_FACT1 table. Do not create aggregations or sample data to save time. Use --offload-stats=COPY and assert that partitions have partition and column stats if Impala version 2.6.0 or greater.',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_FACT1,
                     'reset_backend_table': True,
                     'offload_partition_columns': 'time_id',
                     'offload_partition_granularity': 'M',
                     'offload_stats_method': OFFLOAD_STATS_METHOD_COPY,
                     'older_than_date': '2012-02-01',
                     'num_buckets': 1,
                     'data_sample_pct': 0},
         'expected_exception_string': copy_stats_expected_exception_string(backend_api)},
        {'id': 'backend_stats_copy_2_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_FACT1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], colstats=False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], colstats=True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_COPY))],
         'prereq': lambda: copy_stats_supported(backend_api)},
        {'id': 'backend_stats_copy_3_setup1',
         'type': STORY_TYPE_SETUP,
         'title': 'Create fact table',
         'narrative': 'Create fact table with empty partition right after first HWM',
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_fact_create_ddl(frontend_api, schema,
                                                                                      BACKEND_STATS_FACT1),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema,
                                                                BACKEND_STATS_FACT1),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db,
                                                                             BACKEND_STATS_FACT1)]}},
        {'id': 'backend_stats_copy_3_setup2',
         'type': STORY_TYPE_SETUP,
         'title': 'Truncate first partition',
         'setup': {
             STORY_SETUP_TYPE_FRONTEND:
                 lambda: gen_truncate_sales_based_fact_partition_ddls(schema, BACKEND_STATS_FACT1,
                                                                      [SALES_BASED_FACT_HV_2], frontend_api),
             STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, BACKEND_STATS_FACT1),
             STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db,
                                                                       BACKEND_STATS_FACT1)]}},
        {'id': 'backend_stats_copy_3a_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of BACKEND_STATS_FACT1',
         'narrative': 'Offloads some partitions from BACKEND_STATS_FACT1 table without stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_FACT1,
                     'reset_backend_table': True,
                     'offload_partition_columns': 'time_id',
                     'offload_partition_granularity': 'M',
                     'offload_stats_method': OFFLOAD_STATS_METHOD_NONE,
                     'older_than_date': SALES_BASED_FACT_HV_1,
                     'num_buckets': 1,
                     'data_sample_pct': 0},
         'prereq': lambda: copy_stats_supported(backend_api),
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], False),
              lambda test: False),
             # The stats step should not fail
             (lambda test: text_in_warnings(test.offload_messages, step_title(command_steps.STEP_COPY_STATS_TO_BACKEND)),
              lambda test: False)]},
        {'id': 'backend_stats_copy_3b_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of BACKEND_STATS_FACT1',
         'narrative': 'Offloads no-segment partition from BACKEND_STATS_FACT1 table with copy stats, prior partitions should continue to have no stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_FACT1,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_COPY,
                     'older_than_date': SALES_BASED_FACT_HV_2},
         'prereq': lambda: copy_stats_supported(backend_api),
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], False),
              lambda test: False),
             # The stats step should not fail
             (lambda test: text_in_warnings(test.offload_messages, step_title(command_steps.STEP_COPY_STATS_TO_BACKEND)),
              lambda test: False)]},
        {'id': 'backend_stats_native_1_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_dim_setup'},
        {'id': 'backend_stats_native_1_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of %s' % BACKEND_STATS_DIM1,
         'narrative': 'Offload BACKEND_STATS_DIM1 table. Do not create aggregations or sample data to save time. Do not set --offload-stats so that it defaults to NATIVE. Assert that partitions and column have stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_DIM1,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_native_1_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_DIM1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_NATIVE))]},
        {'id': 'backend_stats_native_5_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_fact_setup'},
        {'id': 'backend_stats_native_5a_offload',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_none_4a_offload'},
        {'id': 'backend_stats_native_5b_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of BACKEND_STATS_FACT1',
         'narrative': 'Offloads another parition from BACKEND_STATS_FACT1 table. Do not create aggregations or sample data to save time. Do not set --offload-stats so that it defaults to NATIVE. Do not set --hive-column-stats. Assert that previous partitions have no partition and no column stats, but this partition has partition stats and no column stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_FACT1,
                     'older_than_date': '2012-03-01',
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_native_5_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_FACT1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], False),
              lambda test: bool(option_target == DBTYPE_IMPALA)),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options, stats_method=OFFLOAD_STATS_METHOD_NATIVE)),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-02')], False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-02')], True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options, stats_method=OFFLOAD_STATS_METHOD_NATIVE))]},
        {'id': 'backend_stats_native_6_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_dim_setup'},
        {'id': 'backend_stats_native_6_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of %s' % BACKEND_STATS_DIM1,
         'narrative': 'Offloads BACKEND_STATS_DIM1 table. Do not create aggregations or sample data to save time. Do not set --offload-stats so that it defaults to NATIVE. Set --hive-column-stats. Assert that partitions and column have stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_DIM1,
                     'hive_column_stats': True,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_native_6_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_DIM1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_NATIVE, hive_column_stats=True))]
         },
        {'id': 'backend_stats_native_7_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_fact_setup'},
        {'id': 'backend_stats_native_7a_offload',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_none_4a_offload'},
        {'id': 'backend_stats_native_7b_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of BACKEND_STATS_FACT1',
         'narrative': 'Offloads another parition from BACKEND_STATS_FACT1 table. Do not create aggregations or sample data to save time. Do not set --offload-stats so that it defaults to NATIVE. Set --hive-column-stats. Assert that previous partitions have no partition and no column stats, but this partition has partition stats and column stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_FACT1,
                     'hive_column_stats': True,
                     'older_than_date': '2012-03-01',
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_native_7_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_FACT1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], False),
              lambda test: bool(option_target == DBTYPE_IMPALA)),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options, stats_method=OFFLOAD_STATS_METHOD_NATIVE)),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-02')], False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-02')], True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options, stats_method=OFFLOAD_STATS_METHOD_NATIVE, hive_column_stats=True))]},
        {'id': 'backend_stats_history_5_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_dim_setup'},
        {'id': 'backend_stats_history_5_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of %s' % BACKEND_STATS_DIM1,
         'narrative': 'Offloads BACKEND_STATS_DIM1 table. Do not create aggregations or sample data to save time. Set --offload-stats=HISTORY. Do not set --hive-column-stats. Assert that all backend partitions have partition stats but no column stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_DIM1,
                     'reset_backend_table': True,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_HISTORY,
                     'num_buckets': 1,
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_history_5_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_DIM1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_HISTORY))]},
        {'id': 'backend_stats_history_6_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_fact_setup'},
        {'id': 'backend_stats_history_6a_offload',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_none_4a_offload'},
        {'id': 'backend_stats_history_6a_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_FACT1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')],
                                               colstats=False),
              lambda test: False),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')],
                                               colstats=True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_NONE))]},
        {'id': 'backend_stats_history_6b_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of BACKEND_STATS_FACT1',
         'narrative': 'Offloads another parition from BACKEND_STATS_FACT1 table. Do not create aggregations or sample data to save time. Set --offload-stats=HISTORY. Do not set --hive-column-stats. Assert that all backend partitions have partition stats but no column stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_FACT1,
                     'older_than_date': '2012-03-01',
                     'offload_stats_method': OFFLOAD_STATS_METHOD_HISTORY,
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_history_6b_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_FACT1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')],
                                               colstats=False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')],
                                               colstats=True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_HISTORY)),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-02')],
                                               colstats=False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-02')],
                                               colstats=True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_HISTORY))]},
        {'id': 'backend_stats_history_9_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_dim_setup'},
        {'id': 'backend_stats_history_9_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of %s' % BACKEND_STATS_DIM1,
         'narrative': 'Offloads BACKEND_STATS_DIM1 table. Do not create aggregations or sample data to save time. Set --offload-stats=HISTORY. Set --hive-column-stats. Assert that all partitions have partition stats but no column stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_DIM1,
                     'reset_backend_table': True,
                     'hive_column_stats': True,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_HISTORY,
                     'num_buckets': 1,
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_history_9_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_DIM1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_dim1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0')], True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_HISTORY, hive_column_stats=True))
         ]},
        {'id': 'backend_stats_history_10_setup',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_fact_setup'},
        {'id': 'backend_stats_history_10a_offload',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'backend_stats_none_4a_offload'},
        {'id': 'backend_stats_history_10a_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_FACT1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be, [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], colstats=False), lambda test: False),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be, [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')], colstats=True), lambda test: should_have_column_stats(backend_api, '2.6.0', options, stats_method=OFFLOAD_STATS_METHOD_NONE))]},
        {'id': 'backend_stats_history_10b_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of BACKEND_STATS_FACT1',
         'narrative': 'Offloads another parition from BACKEND_STATS_FACT1 table. Do not create aggregations or sample data to save time. Set --offload-stats=HISTORY. Set --hive-column-stats. Assert that all partitions have partition stats and column stats',
         'options': {'owner_table': schema + '.' + BACKEND_STATS_FACT1,
                     'older_than_date': '2012-03-01',
                     'hive_column_stats': True,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_HISTORY,
                     'data_sample_pct': 0}},
        {'id': 'backend_stats_history_10b_show',
         'type': STORY_TYPE_SHOW_BACKEND_STATS,
         'title': 'Display Backend Stats',
         'db': data_db,
         'table': BACKEND_STATS_FACT1,
         'assertion_pairs': [
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')],
                                               colstats=False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-01')],
                                               colstats=True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_HISTORY, hive_column_stats=True)),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-02')],
                                               colstats=False),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, backend_stats_fact1_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'), ('gl_part_m_time_id', '2012-02')],
                                               colstats=True),
              lambda test: should_have_column_stats(backend_api, '2.6.0', options,
                                                    stats_method=OFFLOAD_STATS_METHOD_HISTORY, hive_column_stats=True))]},
        {'id': 'backend_stats_lpa_setup1',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % LPA_FACT_TABLE,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_list_create_ddl(frontend_api, schema,
                                                                                      LPA_FACT_TABLE,
                                                                                      default_partition=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, LPA_FACT_TABLE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db,
                                                                             LPA_FACT_TABLE)]}},
        {'id': 'backend_stats_lpa_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload First LIST Partition',
         'options': {'owner_table': '%s.%s' % (schema, LPA_FACT_TABLE),
                     'equal_to_values': [SALES_BASED_LIST_HV_1, SALES_BASED_LIST_HV_3],
                     'offload_stats_method': OFFLOAD_STATS_METHOD_COPY,
                     'num_buckets': 1,
                     'reset_backend_table': True,
                     'data_sample_pct': 0},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(test, schema, hybrid_schema, data_db, LPA_FACT_TABLE,
                                                      options, backend_api, frontend_api, repo_client,
                                                      [SALES_BASED_LIST_HV_1, SALES_BASED_LIST_HV_3],
                                                      check_rowcount=True, check_aggs=False,
                                                      incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, lpa_fact_table_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'),
                                                (synthetic_part_col_name('1', 'yrmon', def_synth_part_digits).lower(),
                                                 ('{:0%sd}' % def_synth_part_digits).format(int(SALES_BASED_LIST_HV_1)))],
                                               colstats=False),
              lambda test: True)],
         # Both assertion_pairs and expected_exception_string are set because on Hive we throw exception, Impala runs and needs assertions
         'expected_exception_string': copy_stats_expected_exception_string(backend_api)},
        {'id': 'backend_stats_lpa_setup2',
         'type': STORY_TYPE_SETUP,
         'title': 'Create VC2 version of %s' % LPA_FACT_TABLE,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_list_create_ddl(frontend_api, schema,
                                                                                      LPA_FACT_TABLE,
                                                                                      part_key_type='VARCHAR2'),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, LPA_FACT_TABLE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db,
                                                                             LPA_FACT_TABLE)]}},
        {'id': 'backend_stats_lpa_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload First LIST Partition',
         'options': {'owner_table': '%s.%s' % (schema, LPA_FACT_TABLE),
                     'equal_to_values': [SALES_BASED_LIST_HV_1, SALES_BASED_LIST_HV_3],
                     'offload_stats_method': OFFLOAD_STATS_METHOD_COPY,
                     'num_buckets': 1,
                     'reset_backend_table': True,
                     'data_sample_pct': 0},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(test, schema, hybrid_schema, data_db, LPA_FACT_TABLE,
                                                      options, backend_api, frontend_api, repo_client,
                                                      [SALES_BASED_LIST_HV_1, SALES_BASED_LIST_HV_3],
                                                      check_rowcount=True, check_aggs=False,
                                                      incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST),
              lambda test: True),
             (lambda test: partition_has_stats(backend_api, data_db, lpa_fact_table_be,
                                               [(OFFLOAD_BUCKET_NAME, '0'),
                                                (synthetic_part_col_name('6', 'yrmon', '1').lower(),
                                                 SALES_BASED_LIST_HV_1)],
                                               colstats=False),
              lambda test: True)],
         'expected_exception_string': copy_stats_expected_exception_string(backend_api)},
    ]

    if list_only:
        return dim_fact_setup + common_stories + hadoop_stories
    else:
        return dim_fact_setup + common_stories + (
            hadoop_stories if options and options.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS else [])
