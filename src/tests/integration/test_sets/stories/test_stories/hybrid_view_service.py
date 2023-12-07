from test_sets.stories.story_globals import STORY_TYPE_PYTHON_FN, STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP, \
    STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_ORACLE, STORY_SETUP_TYPE_PYTHON
from test_sets.stories.story_setup_functions import drop_backend_test_table, gen_hybrid_drop_ddl, \
    gen_sales_based_fact_create_ddl, gen_sales_based_list_create_ddl, gen_sales_based_multi_pcol_fact_create_ddl, \
    get_oracle_hv_for_partition_name, \
    SALES_BASED_FACT_HV_4, SALES_BASED_LIST_HV_3, SALES_BASED_LIST_HV_4, \
    SALES_BASED_LIST_PNAMES_BY_HV, LOWER_YRMON_NUM, UPPER_YRMON_NUM

from goe.gluent import get_log_fh
from goe.conductor.hybrid_view_service import HybridViewService,\
    JSON_KEY_BACKEND_NUM_ROWS, JSON_KEY_BACKEND_PARTITIONS,\
    JSON_KEY_VALIDATE_STATUS, JSON_KEY_VALIDATE_MESSAGE
from goe.offload.offload_constants import PART_COL_GRANULARITY_MONTH

from testlib.test_framework.test_functions import log


LPA_FACT = 'story_hvs_lpa'
RPA_FACT = 'story_hvs_rpa'
MCOL_FACT = 'story_hvs_mcol'
DIMENSION = 'story_hvs_dim'


def hvs_assertion(schema, hybrid_schema, hybrid_view, test_cursor, backend_api):
    if hybrid_view == RPA_FACT.upper():
        low_hv = get_oracle_hv_for_partition_name(schema, hybrid_view, '{}_P3'.format(hybrid_view), test_cursor)
        high_hv = get_oracle_hv_for_partition_name(schema, hybrid_view, '{}_P4'.format(hybrid_view), test_cursor)
        # There are 4 months below SALES_BASED_FACT_HV_4
        expected_backend_partitions = 4
    elif hybrid_view == MCOL_FACT.upper():
        low_hv = get_oracle_hv_for_partition_name(schema, hybrid_view, '{}_P1'.format(hybrid_view), test_cursor)
        high_hv = get_oracle_hv_for_partition_name(schema, hybrid_view, '{}_P2'.format(hybrid_view), test_cursor)
        # The offload copies 1 year which = 12 months
        expected_backend_partitions = 12
    else:
        low_hv = get_oracle_hv_for_partition_name(schema, hybrid_view,
                                                  SALES_BASED_LIST_PNAMES_BY_HV[SALES_BASED_LIST_HV_3], test_cursor)
        high_hv = get_oracle_hv_for_partition_name(schema, hybrid_view,
                                                   SALES_BASED_LIST_PNAMES_BY_HV[SALES_BASED_LIST_HV_4], test_cursor)
        # The offload copies 2 partitions
        expected_backend_partitions = 2

    conductor_api = HybridViewService(hybrid_schema, hybrid_view, existing_log_fh=get_log_fh())
    attribs = conductor_api.backend_attributes(as_json=False)
    if attribs[JSON_KEY_BACKEND_NUM_ROWS] is None:
        log('backend_num_rows is None')
        return False
    resp = conductor_api.validate_by_aggregation(upper_hv=high_hv, as_json=False)
    if not resp[JSON_KEY_VALIDATE_STATUS]:
        log('validate_by_aggregation(%s) returned False' % high_hv)
        return False
    resp = conductor_api.validate_by_aggregation(lower_hv=low_hv, upper_hv=high_hv, as_json=False)
    if not resp[JSON_KEY_VALIDATE_STATUS]:
        log('validate_by_aggregation(%s, %s) returned False' % (low_hv, high_hv))
        return False
    resp = conductor_api.validate_by_count(upper_hv=high_hv, as_json=False)
    if not resp[JSON_KEY_VALIDATE_STATUS]:
        log('validate_by_count(%s) returned False: %s' % (high_hv, resp[JSON_KEY_VALIDATE_MESSAGE]))
        return False
    resp = conductor_api.validate_by_count(lower_hv=low_hv, upper_hv=high_hv, as_json=False)
    if not resp[JSON_KEY_VALIDATE_STATUS]:
        log('validate_by_count(%s, %s) returned False: %s' % (low_hv, high_hv, resp[JSON_KEY_VALIDATE_MESSAGE]))
        return False
    if backend_api.partition_by_column_supported():
        num_backend_parts = len(attribs[JSON_KEY_BACKEND_PARTITIONS])
        if num_backend_parts != expected_backend_partitions:
            log('num_backend_parts != %s: %s' % (expected_backend_partitions, num_backend_parts))
            return False
    return True


def mcol_partition_options(backend_api):
    if not backend_api or not backend_api.partition_by_column_supported():
        return {}
    elif backend_api.max_partition_columns() == 1:
        return {'offload_partition_columns': 'time_id',
                'offload_partition_granularity': 'M'}
    else:
        return {'offload_partition_columns': None,
                'offload_partition_granularity': '1,1,M'}


def hybrid_view_service_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api, test_cursor):
    return [
        {'id': 'hybrid_view_service_range_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % RPA_FACT,
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_sales_based_fact_create_ddl(frontend_api, schema, RPA_FACT),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, RPA_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, RPA_FACT)]}},
        {'id': 'hybrid_view_service_range_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload RANGE',
         'options': {'owner_table': schema + '.' + RPA_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_4,
                     'offload_partition_granularity': PART_COL_GRANULARITY_MONTH,
                     'reset_backend_table': True}},
        {'id': 'hybrid_view_service_range_service',
         'type': STORY_TYPE_PYTHON_FN,
         'title': 'Call Hybrid View Service for %s' % RPA_FACT,
         'python_fns': [lambda: hvs_assertion(schema, hybrid_schema, RPA_FACT.upper(), test_cursor, backend_api)]},
        {'id': 'hybrid_view_service_list_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % LPA_FACT,
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_sales_based_list_create_ddl(frontend_api, schema, LPA_FACT),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, LPA_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, LPA_FACT)]}},
        {'id': 'hybrid_view_service_list_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload LIST',
         'options': {'owner_table': schema + '.' + LPA_FACT,
                     'equal_to_values': [SALES_BASED_LIST_HV_3, SALES_BASED_LIST_HV_4],
                     'offload_partition_lower_value': LOWER_YRMON_NUM,
                     'offload_partition_upper_value': UPPER_YRMON_NUM,
                     'reset_backend_table': True}},
        {'id': 'hybrid_view_service_list_service',
         'type': STORY_TYPE_PYTHON_FN,
         'title': 'Call Hybrid View Service for %s' % LPA_FACT,
         'python_fns': [lambda: hvs_assertion(schema, hybrid_schema, LPA_FACT.upper(), test_cursor, backend_api)]},
        {'id': 'hybrid_view_service_mcol_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % MCOL_FACT,
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_sales_based_multi_pcol_fact_create_ddl(schema, MCOL_FACT),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, MCOL_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, MCOL_FACT)]}},
        {'id': 'hybrid_view_service_mcol_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload MCOL RANGE',
         'options': dict(list({'owner_table': schema + '.' + MCOL_FACT,
                               'less_than_value': '2012,12,2013-01-01',
                               'reset_backend_table': True}.items()) +
                         list(mcol_partition_options(backend_api).items()))},
        {'id': 'hybrid_view_service_mcol_service',
         'type': STORY_TYPE_PYTHON_FN,
         'title': 'Call Hybrid View Service for %s' % MCOL_FACT,
         'python_fns': [lambda: hvs_assertion(schema, hybrid_schema, MCOL_FACT.upper(), test_cursor, backend_api)]},
    ]
