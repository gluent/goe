from goe.offload.offload_constants import DBTYPE_ORACLE
from test_sets.stories.story_globals import STORY_TYPE_AGG_VALIDATE, STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP, \
    STORY_SETUP_TYPE_FRONTEND, STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_ORACLE, STORY_SETUP_TYPE_PYTHON
from test_sets.stories.story_setup_functions import drop_backend_test_table, gen_hybrid_drop_ddl, \
    gen_sales_based_fact_create_ddl, gen_sales_based_list_create_ddl, gen_sales_based_multi_pcol_fact_create_ddl, \
    SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3, SALES_BASED_LIST_HV_2, SALES_BASED_LIST_HV_3, \
    LOWER_YRMON_NUM, UPPER_YRMON_NUM
from test_sets.stories.story_assertion_functions import offload_dim_assertion, offload_fact_assertions,\
    offload_lpa_fact_assertion, text_in_log
from test_sets.stories.test_stories.offload_pbo import gen_simple_unicode_dimension_ddl

from goe.offload.predicate_offload import GenericPredicate
from goe.persistence.orchestration_metadata import INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE


LPA_FACT = 'STORY_AV_LPA'
RPA_FACT = 'STORY_AV_RPA'
MCOL_FACT = 'STORY_AV_MCOL'
PBO_DIM = 'STORY_AV_PBO'
UCODE_VALUE1 = '\u03a3'
UCODE_VALUE2 = '\u30ad'
PBO_FACT = 'STORY_AV_PBO_INTRA'


def mcol_partition_options(backend_api):
    if not backend_api or not backend_api.partition_by_column_supported():
        return {}
    elif backend_api.max_partition_columns() == 1:
        return {'offload_partition_columns': 'time_id',
                'offload_partition_granularity': None}
    else:
        return {'offload_partition_columns': None,
                'offload_partition_granularity': '1,1,Y'}


def agg_validate_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api, repo_client):
    return [
        {'id': 'agg_validate_range_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % RPA_FACT,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, RPA_FACT),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, RPA_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, RPA_FACT)]}},
        {'id': 'agg_validate_range_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload RANGE',
         'options': {'owner_table': schema + '.' + RPA_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'reset_backend_table': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, RPA_FACT, SALES_BASED_FACT_HV_2)},
        {'id': 'agg_validate_range_validation1',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate Over RANGE',
         'options': {'owner_table': schema + '.' + RPA_FACT}},
        {'id': 'agg_validate_range_validation2',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate With Custom Filter Over RANGE',
         'options': {'owner_table': schema + '.' + RPA_FACT,
                     'filters': 'CUST_ID > 1000'}},
        {'id': 'agg_validate_range_validation3',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate With Wildcard Column Names',
         'options': {'owner_table': schema + '.' + RPA_FACT,
                     'selects': '*ID',
                     'group_bys': 'Channel*'}},
        {'id': 'agg_validate_range_validation4',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate Over RANGE - parallel=2',
         'options': {'owner_table': schema + '.' + RPA_FACT,
                     'frontend_parallelism': 2},
         'assertion_pairs': [
             (lambda test: text_in_log('PARALLEL(2)', '(agg_validate_range_validation4)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ]},
        {'id': 'agg_validate_range_validation5',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate Over RANGE - parallel=1',
         'options': {'owner_table': schema + '.' + RPA_FACT,
                     'frontend_parallelism': 1},
         'assertion_pairs': [
             (lambda test: text_in_log('NO_PARALLEL', '(agg_validate_range_validation5)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ]},
        {'id': 'agg_validate_list_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % LPA_FACT,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_list_create_ddl(frontend_api, schema, LPA_FACT),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, LPA_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, LPA_FACT)]},
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'agg_validate_list_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload LIST',
         'options': {'owner_table': schema + '.' + LPA_FACT,
                     'equal_to_values': [SALES_BASED_LIST_HV_2, SALES_BASED_LIST_HV_3],
                     'offload_partition_lower_value': LOWER_YRMON_NUM,
                     'offload_partition_upper_value': UPPER_YRMON_NUM,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(
                 test, schema, hybrid_schema, data_db, LPA_FACT, options, backend_api,
                 frontend_api, repo_client, [SALES_BASED_LIST_HV_2, SALES_BASED_LIST_HV_3]),
              lambda test: True)],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'agg_validate_list_validation',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate Over LIST',
         'options': {'owner_table': schema + '.' + LPA_FACT},
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'agg_validate_mcol_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % MCOL_FACT,
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_sales_based_multi_pcol_fact_create_ddl(schema, MCOL_FACT),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, MCOL_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, MCOL_FACT)]},
         'prereq': lambda: frontend_api.gluent_multi_column_incremental_key_supported()},
        {'id': 'agg_validate_mcol_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload RANGE',
         'options': dict(list({'owner_table': schema + '.' + MCOL_FACT,
                          'less_than_value': '2012,12,2012-12-01',
                          'reset_backend_table': True}.items()) + list(mcol_partition_options(backend_api).items())),
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, MCOL_FACT, '2012,12,2012-12-01',
                                                    incremental_key='TIME_YEAR, TIME_MONTH, TIME_ID',
                                                    check_hwm_in_hybrid_view=False),
         'prereq': lambda: frontend_api.gluent_multi_column_incremental_key_supported()},
        {'id': 'agg_validate_mcol_validation',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate Over multi-column RANGE',
         'options': {'owner_table': schema + '.' + MCOL_FACT},
         'prereq': lambda: frontend_api.gluent_multi_column_incremental_key_supported()},
        {'id': 'agg_validate_pbo_std_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % PBO_DIM,
         'narrative': 'Create dimension for testing over PBO but with unicode data to make life hard',
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_simple_unicode_dimension_ddl(options, frontend_api, schema, PBO_DIM,
                                                                               UCODE_VALUE1, UCODE_VALUE2),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, PBO_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, PBO_DIM)]}},
        {'id': 'agg_validate_pbo_std_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % PBO_DIM,
         'narrative': 'Offload 1st predicate of dimension',
         'options': {'owner_table': schema + '.' + PBO_DIM,
                     'offload_predicate': GenericPredicate(
                         '((column(id) = numeric(1)) and (column(data) = string("%s")))' % UCODE_VALUE1),
                     'unicode_string_columns_csv': 'data',
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, PBO_DIM),
              lambda test: True)]},
        {'id': 'agg_validate_pbo_std_validation',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate Over PREDICATE',
         'options': {'owner_table': schema + '.' + PBO_DIM}},
        {'id': 'agg_validate_pbo_intra_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % PBO_FACT,
         'setup': {STORY_SETUP_TYPE_FRONTEND: gen_sales_based_fact_create_ddl(frontend_api, schema, PBO_FACT),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, PBO_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, PBO_FACT)]}},
        {'id': 'agg_validate_pbo_intra_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload RANGE',
         'options': {'owner_table': schema + '.' + PBO_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'reset_backend_table': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, PBO_FACT, SALES_BASED_FACT_HV_2)},
        {'id': 'agg_validate_pbo_intra_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload RANGE_AND_PREDICATE',
         'options': {'owner_table': schema + '.' + PBO_FACT,
                     'offload_predicate': GenericPredicate(
                         '(((column(time_id) >= datetime(%s)) and (column(time_id) < datetime(%s))) and (column(channel_id) = numeric(2)))'
                         % (SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3)),
                     'ipa_predicate_type': INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE}},
        {'id': 'agg_validate_pbo_intra_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload RANGE_AND_PREDICATE',
         'options': {'owner_table': schema + '.' + PBO_FACT,
                     'offload_predicate': GenericPredicate(
                         '(((column(time_id) >= datetime(%s)) and (column(time_id) < datetime(%s))) and (column(channel_id) = numeric(3)))'
                         % (SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3))}},
        {'id': 'agg_validate_pbo_intra_validation',
         'type': STORY_TYPE_AGG_VALIDATE,
         'title': 'Validate Over RANGE_AND_PREDICATE',
         'options': {'owner_table': schema + '.' + PBO_FACT}},
    ]
