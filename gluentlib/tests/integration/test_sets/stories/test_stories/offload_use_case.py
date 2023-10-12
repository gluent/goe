from typing import TYPE_CHECKING

from gluent import IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT, TOKENISE_STRING_CHAR_1, log
from gluentlib.offload.backend_api import IMPALA_NOSHUFFLE_HINT
from gluentlib.offload.column_metadata import (
    GLUENT_TYPE_DOUBLE,
    match_table_column,
    str_list_of_columns,
)
from gluentlib.offload.hadoop.hadoop_backend_table import (
    COMPUTE_LOAD_TABLE_STATS_LOG_TEXT,
)
from gluentlib.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_TERADATA,
    OFFLOAD_STATS_METHOD_COPY,
    OFFLOAD_STATS_METHOD_NATIVE,
    PART_COL_GRANULARITY_DAY,
    PART_COL_GRANULARITY_MONTH,
)
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_metadata_functions import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
)
from gluentlib.offload.offload_source_data import MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE
from test_sets.stories.offload_fs_location import base_table_fs_scheme_is_correct
from test_sets.stories.story_assertion_functions import (
    backend_column_exists,
    backend_table_count,
    backend_table_exists,
    date_gl_part_column_name,
    offload_dim_assertion,
    offload_fact_assertions,
    select_one_value_from_frontend_table_column,
    text_in_events,
    text_in_log,
)
from test_sets.stories.story_globals import (
    STORY_SETUP_TYPE_FRONTEND,
    STORY_SETUP_TYPE_HYBRID,
    STORY_SETUP_TYPE_ORACLE,
    STORY_SETUP_TYPE_PYTHON,
    STORY_TYPE_OFFLOAD,
    STORY_TYPE_SETUP,
)
from test_sets.stories.story_setup_functions import (
    SALES_BASED_FACT_HV_1,
    SALES_BASED_FACT_HV_2,
    SALES_BASED_FACT_HV_3,
    SALES_BASED_FACT_HV_4,
    SALES_BASED_FACT_HV_5,
    SALES_BASED_FACT_HV_6,
    SALES_BASED_FACT_HV_7,
    SALES_BASED_FACT_PRE_HV,
    drop_backend_test_load_table,
    drop_backend_test_table,
    gen_drop_sales_based_fact_partition_ddls,
    gen_hybrid_drop_ddl,
    gen_rdbms_dim_create_ddl,
    gen_sales_based_fact_create_ddl,
    gen_truncate_sales_based_fact_partition_ddls,
    partition_columns_if_supported,
    sales_based_fact_partition_exists,
)
from test_sets.stories.test_stories.offload_lpa import LPA_UNICODE_PART2_KEY1

if TYPE_CHECKING:
    from gluentlib.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )
    from testlib.test_framework.backend_testing_api import BackendTestingApiInterface
    from testlib.test_framework.frontend_testing_api import FrontendTestingApiInterface


OFFLOAD_DIM = 'STORY_DIM'
DEPENDENT_VIEW_DIM = 'STORY_VIEW_DIM'
DEPENDENT_VIEW_DIM_VW9_BAD_TOKEN = TOKENISE_STRING_CHAR_1 + '1' + TOKENISE_STRING_CHAR_1
OFFLOAD_FACT = 'STORY_FACT'


def offload_story_dim_actual_partition_options(backend_api):
    if not backend_api or not backend_api.partition_by_column_supported():
        return {}
    elif backend_api.max_partition_columns() == 1:
        return {'offload_partition_columns': 'prod_category_id',
                'offload_partition_granularity': '1'}
    else:
        return {'offload_partition_columns': 'prod_category_id,prod_subcategory_desc',
                'offload_partition_granularity': '1,1'}


def offload_story_dim_actual_assertion(backend_api, frontend_api, options, hybrid_schema, data_db):
    def check_column_exists(column_name: str, list_of_columns: list) -> bool:
        if not match_table_column(column_name, list_of_columns):
            log('False from: match_table_column(%s, %s)' % (column_name, str_list_of_columns(list_of_columns)))
            return False
        return True

    if not backend_api:
        return True
    backend_name = convert_backend_identifier_case(options, OFFLOAD_DIM)
    double_type = backend_api.expected_canonical_to_backend_type_map()[GLUENT_TYPE_DOUBLE]
    if backend_api.partition_by_column_supported():
        if backend_api.backend_type() == DBTYPE_BIGQUERY:
            part_cols = backend_api.get_partition_columns(data_db, backend_name)
            if not check_column_exists('prod_category_id', part_cols):
                return False
        else:
            # Hadoop based
            if not backend_column_exists(backend_api, data_db, backend_name, 'gl_part_000000000000001_prod_category_id'):
                return False
            if not backend_column_exists(backend_api, data_db, backend_name, 'gl_part_1_prod_subcategory_desc'):
                return False

    if not backend_column_exists(backend_api, data_db, backend_name, 'prod_category_id',
                                 search_type=backend_api.backend_test_type_canonical_int_8()):
        return False
    if not backend_column_exists(backend_api, data_db, backend_name, 'prod_subcategory_id',
                                 search_type=backend_api.backend_test_type_canonical_int_8()):
        return False
    # Checks that --double-columns does the right thing (GOE-1397)
    if not backend_column_exists(backend_api, data_db, backend_name, 'prod_total_id', search_type=double_type):
        return False

    return True


def offload_story_fact_init_assertion(backend_api, data_db, backend_name):
    if not backend_api:
        return True
    if backend_api.partition_by_column_supported():
        if backend_api.backend_type() in [DBTYPE_IMPALA, DBTYPE_HIVE]:
            if not backend_column_exists(backend_api, data_db, backend_name,
                                         date_gl_part_column_name(backend_api, 'TIME_ID')):
                return False
            if not backend_column_exists(backend_api, data_db, backend_name, 'gl_part_000000000000001_channel_id'):
                return False
    if not backend_column_exists(backend_api, data_db, backend_name, 'cust_id',
                                 search_type=backend_api.backend_test_type_canonical_int_8()):
        return False
    if not backend_column_exists(backend_api, data_db, backend_name, 'channel_id',
                                 search_type=backend_api.backend_test_type_canonical_int_2()):
        return False
    if not backend_column_exists(backend_api, data_db, backend_name, 'prod_id',
                                 search_type=backend_api.backend_test_type_canonical_int_8()):
        return False
    if backend_api.backend_type() in [DBTYPE_IMPALA, DBTYPE_HIVE]:
        search_type1 = 'decimal(18,4)'
        search_type2 = 'decimal(38,4)'
    else:
        search_type1 = backend_api.backend_test_type_canonical_decimal()
        search_type2 = backend_api.backend_test_type_canonical_decimal()
    if not backend_column_exists(backend_api, data_db, backend_name, 'quantity_sold', search_type=search_type1):
        return False
    if not backend_column_exists(backend_api, data_db, backend_name, 'amount_sold', search_type=search_type2):
        return False
    return True


def offload_story_fact_1st_incr_assertion(test, backend_api, options, data_db, backend_name):
    if not options:
        return True
    backend_columns = backend_api.get_partition_columns(data_db, backend_name)
    if not backend_columns:
        return True
    # Check that OffloadSourceData added an optimistic partition pruning clause when appropriate
    granularity = PART_COL_GRANULARITY_MONTH if options.target == DBTYPE_IMPALA else PART_COL_GRANULARITY_DAY
    expect_optimistic_prune_clause = backend_api.partition_column_requires_synthetic_column(backend_columns[0],
                                                                                            granularity)
    if text_in_events(test.offload_messages, MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE) != expect_optimistic_prune_clause:
        log('text_in_events(MAX_QUERY_OPTIMISTIC_PRUNE_CLAUSE) != %s' % expect_optimistic_prune_clause)
        return False
    return True


def offload_story_fact_2nd_incr_assertion(backend_api, options, data_db, backend_name):
    if not backend_column_exists(backend_api, data_db, backend_name, 'cust_id',
                                 search_type=backend_api.backend_test_type_canonical_int_8()):
        return False
    if not backend_column_exists(backend_api, data_db, backend_name, 'channel_id',
                                 search_type=backend_api.backend_test_type_canonical_int_4()):
        return False
    return True


def get_dependent_app_view_ddls(schema, hybrid_schema):
    """ Create dependent views in the app schema with a number of variations:
        VW1: Standard lower case, as you'd expect it
        VW2: Schema and table in double quotes
        VW3: Standard table, schema inferred from view owner
        VW4: Table in double quotes, schema inferred from view owner
        VW5: Sneaky schema.table in a string inside the view, must survive (see dependent_app_view_assertions_vw5() for check)
        VW6: View over a dependent view
        VW7: View over a view over a dependent view
        VW8: View with a regexp which uses group matching (GOE-1429)
        VW9: Sneaky ~1~ string in the view. ~n~ is our replacement token protecting from replacement in strings (e.g. VW5)
             See dependent_app_view_assertions_vw9() for check
        VW10: View containing a non-ascii character
    """
    dependent_app_view_subs = {'hybrid_schema': hybrid_schema,
                               'schema': schema,
                               'table': DEPENDENT_VIEW_DIM,
                               'schema_upper': schema.upper() if schema else None,
                               'table_upper': DEPENDENT_VIEW_DIM.upper(),
                               'bad_token': DEPENDENT_VIEW_DIM_VW9_BAD_TOKEN,
                               'unicode_string': LPA_UNICODE_PART2_KEY1}
    ddls = []
    for i in range(1, 11):
        if schema:
            drop_pattern = 'DROP VIEW %(schema)s.%(table)s_vw' + str(i)
            ddls.append(drop_pattern % dependent_app_view_subs)
        if hybrid_schema:
            drop_pattern = 'DROP VIEW %(hybrid_schema)s.%(table)s_vw' + str(i)
            ddls.append(drop_pattern % dependent_app_view_subs)
    if schema:
        ddls.extend([
            'CREATE VIEW %(schema)s.%(table)s_vw1 AS SELECT * FROM %(schema)s.%(table)s' % dependent_app_view_subs,
            'CREATE VIEW %(schema)s.%(table)s_vw2 AS SELECT * FROM "%(schema_upper)s"."%(table_upper)s"' % dependent_app_view_subs,
            'CREATE VIEW %(schema)s.%(table)s_vw3 AS SELECT * FROM %(table)s' % dependent_app_view_subs,
            'CREATE VIEW %(schema)s.%(table)s_vw4 AS SELECT * FROM "%(table_upper)s"' % dependent_app_view_subs,
            'CREATE VIEW %(schema)s.%(table)s_vw5 AS SELECT d.*, \'%(schema)s.%(table)s\' AS no_repl_str FROM "%(table_upper)s" d' % dependent_app_view_subs,
            'CREATE VIEW %(schema)s.%(table)s_vw6 AS SELECT * FROM "%(table_upper)s_VW3"' % dependent_app_view_subs,
            'CREATE VIEW %(schema)s.%(table)s_vw7 AS SELECT * FROM "%(table_upper)s_VW6"' % dependent_app_view_subs,
            r"""CREATE VIEW %(schema)s.%(table)s_vw8 AS SELECT prod_id, REGEXP_REPLACE(prod_name, '^.*[+-]([0-9]+)', '\1') AS re_with_group FROM %(table)s""" % dependent_app_view_subs,
            """CREATE VIEW %(schema)s.%(table)s_vw9 AS SELECT prod_id,prod_name,'%(bad_token)s' AS bad_token, 'OK' AS good_token FROM %(table)s""" % dependent_app_view_subs,
            """CREATE VIEW %(schema)s.%(table)s_vw10 AS SELECT prod_id,prod_name,'%(unicode_string)s' AS unicode_string FROM %(table)s""" % dependent_app_view_subs,
        ])
    return ddls


def dependent_app_view_valid_assertion(frontend_api, options, schema_to_check, table_name, vw_start, vw_end):
    if not options:
        return True
    assert schema_to_check and table_name, 'Invalid schema/table: %s/%s' % (schema_to_check, table_name)
    assert vw_start and isinstance(vw_start, int), 'Invalid vw_start: %s' % vw_start
    assert vw_end and isinstance(vw_end, int) and vw_end >= vw_start, 'Invalid vw_end: %s' % vw_end
    for i in range(vw_start, vw_end+1):
        vw_name = f'{table_name}_vw{i}'.upper()
        if not frontend_api.view_is_valid(schema_to_check, vw_name):
            log('ora_view_is_valid(%s, %s) is False' % (schema_to_check, vw_name))
            return False
    return True


def dependent_app_view_assertion_vw5(frontend_api, schema_to_check, schema, table_name):
    no_repl_str = '%(schema)s.%(table)s' % {'schema': schema, 'table': table_name}
    return bool(
        select_one_value_from_frontend_table_column(frontend_api, schema_to_check,
                                                    '%s_vw5' % table_name, 'no_repl_str') == no_repl_str
    )


def dependent_app_view_assertion_vw9(frontend_api, schema_to_check, table_name):
    return bool(
        select_one_value_from_frontend_table_column(frontend_api, schema_to_check,
                                                    '%s_vw9' % table_name, 'bad_token') == DEPENDENT_VIEW_DIM_VW9_BAD_TOKEN
    )


def dependent_app_view_assertion_vw10(frontend_api, schema_to_check, table_name):
    return bool(
        select_one_value_from_frontend_table_column(frontend_api, schema_to_check,
                                                    '%s_vw10' % table_name, 'unicode_string') == LPA_UNICODE_PART2_KEY1
    )


def dim_set_buckets_assertion(backend_api, options, data_db, load_db, table_name):
    if not options:
        return True
    table_name = convert_backend_identifier_case(options, table_name)
    if text_in_log(COMPUTE_LOAD_TABLE_STATS_LOG_TEXT,
                   '(%s)' % ('offload_story_dim_set_buckets')) != backend_api.table_stats_compute_supported():
        log('text_in_log(COMPUTE_LOAD_TABLE_STATS_LOG_TEXT) != %s' % backend_api.table_stats_compute_supported())
        return False
    if text_in_log('[%s]' % IMPALA_NOSHUFFLE_HINT,
                   '(%s)' % ('offload_story_dim_set_buckets')) != bool(options.target == DBTYPE_IMPALA):
        log('text_in_log(IMPALA_NOSHUFFLE_HINT) != %s' % bool(options.target == DBTYPE_IMPALA))
        return False
    if not base_table_fs_scheme_is_correct(data_db, table_name, options, backend_api):
        log('base_table_fs_scheme_is_correct() != True')
        return False
    if backend_api.load_db_transport_supported():
        if not backend_table_exists(backend_api, load_db, table_name):
            return False
        if backend_table_count(backend_api, load_db, table_name) == 0:
            log('backend_table_count() == 0')
            return False
        if not backend_api.load_table_fs_scheme_is_correct(load_db, table_name):
            log('load_table_fs_scheme_is_correct() != True')
            return False
    return True


def offload_story_fact_init_options(schema, table_name, option_target, backend_api):
    if not backend_api:
        return {}
    offload_stats_method = OFFLOAD_STATS_METHOD_COPY if option_target == DBTYPE_IMPALA else OFFLOAD_STATS_METHOD_NATIVE
    opts = {'owner_table': schema + '.' + table_name,
            'reset_backend_table': True,
            'integer_2_columns_csv': 'channel_id',
            'integer_8_columns_csv': 'cust_id,prod_id,promo_id',
            'decimal_columns_csv_list': ['quantity_sold', 'amount_sold'],
            'decimal_columns_type_list': ['10,2', '20,2'],
            'offload_stats_method': offload_stats_method,
            'older_than_date': SALES_BASED_FACT_HV_1,
            'num_buckets': 4,
            'num_location_files': 8,
            'decimal_padding_digits': 2}
    if backend_api.partition_by_column_supported():
        if option_target == DBTYPE_BIGQUERY:
            opts.update({'offload_partition_granularity': PART_COL_GRANULARITY_DAY})
        else:
            opts.update({'offload_partition_columns': 'time_id,channel_id',
                         'offload_partition_granularity': PART_COL_GRANULARITY_MONTH + ',1'})
    return opts


def offload_use_case_story_tests(schema: str, hybrid_schema: str, data_db: str, load_db: str, options,
                                 backend_api: "BackendTestingApiInterface",
                                 frontend_api: "FrontendTestingApiInterface",
                                 repo_client: "OrchestrationRepoClientInterface"):
    option_target = options.target if options else None
    copy_stats_available = bool(backend_api and backend_api.table_stats_set_supported())
    offload_fact_be = convert_backend_identifier_case(options, OFFLOAD_FACT)
    return [
        {'id': 'offload_story_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_DIM,
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_rdbms_dim_create_ddl(frontend_api, schema, OFFLOAD_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_DIM),
                                             lambda: drop_backend_test_load_table(options, backend_api,
                                                                                  load_db, OFFLOAD_DIM)]}},
        {'id': 'offload_story_dim_verification',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension - Non-Execute',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'assertion_pairs': [(lambda test: frontend_api.view_exists(hybrid_schema, OFFLOAD_DIM),
                              lambda test: False),
                             (lambda test: frontend_api.view_exists(hybrid_schema, OFFLOAD_DIM + '_AGG'),
                              lambda test: False)]},
        {'id': 'offload_story_dim_set_buckets',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with Num Buckets',
         'narrative': """Offload the dimension ticking a few boxes:
                         Ensure no usage of WebHDFS (this may be a duplication of offload_story_dim if not configured)
                         Offload to 8 buckets with 12 location files and assert it really happens
                         Compute load table stats
                         Prevent auto FAP rule creation
                         Manually copy stats
                         Test IMPALA_NOSHUFFLE_HINT if running on Impala""",
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'num_buckets': 8,
                     'bucket_hash_col': 'prod_desc',
                     'reset_backend_table': True,
                     'offload_stats_method':
                         OFFLOAD_STATS_METHOD_COPY if copy_stats_available else OFFLOAD_STATS_METHOD_NATIVE,
                     'num_location_files': 12,
                     'compute_load_table_stats': True,
                     'preserve_load_table': True,
                     'impala_insert_hint': IMPALA_NOSHUFFLE_HINT},
         'config_override': {'webhdfs_host': None,
                             'num_buckets_max': 16},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, OFFLOAD_DIM,
                                                 bucket_check=8, check_aggs=False,
                                                 bucket_col='PROD_DESC', num_loc_files=12),
              lambda test: True),
             (lambda test: dim_set_buckets_assertion(backend_api, options, data_db, load_db, OFFLOAD_DIM),
              lambda test: True)]},
        {'id': 'offload_story_dim_no_reoffload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Re-Offload Dimension',
         'narrative': 'Attempt to re-offload the dimension checking that offload aborts',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM},
         'expected_status': False},
        {'id': 'offload_story_dim_actual',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension',
         'narrative': 'Offload the dimension modifying data types and backend partitioning',
         'options': dict(list({'owner_table': schema + '.' + OFFLOAD_DIM,
                               'reset_backend_table': True,
                               'bucket_hash_col': 'prod_id',
                               'num_buckets': 2,
                               'offload_partition_lower_value': 0,
                               'offload_partition_upper_value': 1000,
                               'integer_8_columns_csv': 'prod_id,prod_SubCategory_id,prod_category_id',
                               'double_columns_csv': 'PROD_TOTAL_ID',
                               'num_location_files': 8,
                               'purge_backend_table': True}.items()) +
                         list(offload_story_dim_actual_partition_options(backend_api).items())),
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, OFFLOAD_DIM, bucket_check=2, check_aggs=True,
                 bucket_col='PROD_ID', bucket_method='GLUENT_BUCKET', num_loc_files=8),
              lambda test: True),
             (lambda test: offload_story_dim_actual_assertion(backend_api, frontend_api, options,
                                                              hybrid_schema, data_db),
              lambda test: True)]},
        {'id': 'offload_story_fact_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_FACT,
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_sales_based_fact_create_ddl(frontend_api, schema, OFFLOAD_FACT),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_FACT)]}},
        {'id': 'offload_story_fact_empty_init',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Zero Rows Offload of Fact',
         'narrative': 'Offloads only empty partitions. Ensure 0 rows in backend',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'reset_backend_table': True,
                     'older_than_date': SALES_BASED_FACT_PRE_HV},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_PRE_HV,
                                                    check_rowcount=False) + \
                          [(lambda test: backend_table_count(backend_api, data_db, offload_fact_be), lambda test: 0)],
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_story_fact_init_verification1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Initial Fact Offload - Non-Execute',
         'narrative': 'Non-Execute offload of first partition with basic options',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_1,
                     'ipa_predicate_type': INCREMENTAL_PREDICATE_TYPE_RANGE,
                     'reset_backend_table': True},
         'config_override': {'execute': False}},
        {'id': 'offload_story_fact_init_verification2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Initial Fact Offload - Expect Exception',
         'narrative': 'Offload of RANGE requesting LIST',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_1,
                     'ipa_predicate_type': INCREMENTAL_PREDICATE_TYPE_LIST,
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'expected_exception_string': IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT},
        {'id': 'offload_story_fact_init_verification3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Initial Fact Offload - Non-Execute',
         'narrative': 'Non-Execute offload of first partition with advanced options',
         'options': offload_story_fact_init_options(schema, OFFLOAD_FACT, option_target, backend_api),
         'config_override': {'execute': False}},
        {'id': 'offload_story_fact_init_run',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Initial Offload of Fact',
         'narrative': """Offload some partitions from a fact table.
                         The fact is partitioned by multiple columns with appropriate granularity.
                         We use COPY stats on this initial offload, also specify some specific data types.""",
         'options': offload_story_fact_init_options(schema, OFFLOAD_FACT, option_target, backend_api),
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_1,
                                                    bucket_check=4, num_loc_files=8, check_backend_rowcount=True) + \
            [(lambda test: offload_story_fact_init_assertion(backend_api, data_db, offload_fact_be),
              lambda test: True)]},
        {'id': 'offload_story_fact_1st_incr_verification',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Offload of Fact - Non-Execute',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_2},
         'config_override': {'execute': False},
         'assertion_pairs': [
             (lambda test: offload_story_fact_1st_incr_assertion(test, backend_api, options, data_db, offload_fact_be),
              lambda test: True)]},
        {'id': 'offload_story_fact_1st_incr_run',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Offload of Fact',
         'narrative': 'Offloads next partition from fact table. Try change --num-buckets which will be ignored.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'num_buckets': 16,
                     'num_location_files': 4},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_2,
                                                    bucket_check=4, num_loc_files=4)},
        {'id': 'offload_story_fact_pointless_incr1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Re-offload of Fact',
         'narrative': 'Try re-offload same partition which will result in no action and early abort',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_2},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_2,
                                                    check_aggs=False, bucket_check=4, num_loc_files=4),
         'expected_status': False},
        {'id': 'offload_story_fact_2nd_incr',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Further Incremental Offload of Fact',
         'narrative': "Offloads next partition with dodgy settings, offload will override these with sensible options.",
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_3,
                     'integer_1_columns_csv': 'cust_id,channel_id,prod_id',
                     'offload_partition_columns': partition_columns_if_supported(backend_api, 'promo_id'),
                     'synthetic_partition_digits': 5,
                     'offload_partition_granularity': 100,
                     'offload_partition_lower_value': 0,
                     'offload_partition_upper_value': 10000},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_3,
                                                    bucket_check=4, num_loc_files=16) + \
             [(lambda test: offload_story_fact_2nd_incr_assertion(backend_api, options, data_db, offload_fact_be),
               lambda test: True)]},
        {'id': 'offload_story_fact_drop_partition',
         'type': STORY_TYPE_SETUP,
         'title': 'Drop Oldest Partition from Fact',
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       lambda: gen_drop_sales_based_fact_partition_ddls(schema, OFFLOAD_FACT,
                                                                        [SALES_BASED_FACT_PRE_HV, SALES_BASED_FACT_HV_1],
                                                                        frontend_api, dropping_oldest=True)},
         'assertion_pairs': [(lambda test: sales_based_fact_partition_exists(schema, OFFLOAD_FACT,
                                                                             [SALES_BASED_FACT_HV_1], frontend_api),
                              lambda test: False)]},
        {'id': 'offload_story_fact_3rd_incr',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Offload of Fact after Partition Drop',
         'narrative': 'Offloads next partition after the oldest partition was dropped. Verification step succeeds.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_4},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_4,
                                                    check_aggs=False, check_rowcount=False, num_loc_files=16)},
        {'id': 'offload_story_fact_drop_ofl_parts',
         'type': STORY_TYPE_SETUP,
         'title': 'Drop Offloaded Partitions',
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       lambda: gen_drop_sales_based_fact_partition_ddls(schema, OFFLOAD_FACT,
                                                                        [SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3,
                                                                         SALES_BASED_FACT_HV_4], frontend_api,
                                                                        dropping_oldest=True)},
         'assertion_pairs': [
             (lambda test: sales_based_fact_partition_exists(schema, OFFLOAD_FACT,
                                                             [SALES_BASED_FACT_HV_4], frontend_api),
              lambda test: False)]},
        {'id': 'offload_story_fact_4th_incr',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'No-op Offload of Fact After Drop',
         'narrative': 'Offloads no partitions from fact after all offloaded partitions have been dropped (GOE-1035).',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_4},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_4,
                                                    check_aggs=False, check_rowcount=False, num_loc_files=16),
         'expected_status': False},
        {'id': 'offload_story_fact_5th_incr',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Offload of Fact after Drop',
         'narrative': 'Offloads next partition from fact table after all offloaded partitions have been dropped.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_5},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_5,
                                                    check_aggs=False, check_rowcount=False, num_loc_files=16)},
        {'id': 'offload_story_fact_trunc_partitions',
         'type': STORY_TYPE_SETUP,
         'title': 'Truncate All Offloaded Partitions',
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       lambda: gen_truncate_sales_based_fact_partition_ddls(schema, OFFLOAD_FACT,
                                                                            [SALES_BASED_FACT_HV_5], frontend_api)}},
        {'id': 'offload_story_fact_6th_incr',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Offload of Fact after Truncate',
         'narrative': 'Offloads next partition from fact table after all offloaded partitions have been truncated.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_6},
         # TODO We need to be able to assert on whether the empty partitions were picked up or not,
         #      needs access to the offload log file...
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_6,
                                                    check_aggs=False, check_rowcount=False, num_loc_files=16)},
        {'id': 'offload_story_fact_delete_all_rows',
        'type': STORY_TYPE_SETUP,
        'title': 'Delete All Frontend Rows',
        'setup': {STORY_SETUP_TYPE_ORACLE: [f"""DELETE {schema}.{OFFLOAD_FACT}"""]}},
        {'id': 'offload_story_fact_7th_incr',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Incremental Offload after Delete All Rows',
         'narrative': 'Offloads next partition from fact table after all frontend rows have been deleted.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_7},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_7,
                                                    check_aggs=False, check_rowcount=False),
         # On Teradata, as far as GOE is concerned, deleting all rows is the same as dropping the partitions
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
    ]
