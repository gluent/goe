""" Tests --sort-columns Offload option, this option controls backend table sorting/clustering.
    Tests ensures we can define the clustering, change it once already defined and remove it.
"""

from test_sets.stories.story_globals import STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_PYTHON, STORY_SETUP_TYPE_ORACLE, \
    STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import drop_backend_test_table, gen_hybrid_drop_ddl,\
    gen_rdbms_dim_create_ddl, gen_sales_based_fact_create_ddl,\
    SALES_BASED_FACT_HV_1, SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3, SALES_BASED_FACT_HV_4, SALES_BASED_FACT_HV_5
from test_sets.stories.story_assertion_functions import check_metadata

from gluentlib.offload.offload_constants import DBTYPE_HIVE, SORT_COLUMNS_NO_CHANGE
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.operation.sort_columns import SORT_COLUMN_MAX_EXCEEDED_EXCEPTION_TEXT,\
    SORT_COLUMN_NO_MODIFY_EXCEPTION_TEXT, UNKNOWN_SORT_COLUMN_EXCEPTION_TEXT
from testlib.test_framework.test_functions import log


RDBMS_DIM = 'story_sort_dim'
RDBMS_FACT = 'story_sort_fact'


def dim_sorted_table_supported(backend_api, modify=False):
    if not backend_api:
        return False
    if modify:
        return bool(backend_api.sorted_table_supported() and backend_api.sorted_table_modify_supported())
    else:
        return backend_api.sorted_table_supported()


def fact_sorted_table_supported(backend_api, modify=False):
    if not backend_api:
        return False
    if modify:
        return bool(backend_api.sorted_table_supported() and backend_api.sorted_table_modify_supported())
    else:
        return backend_api.sorted_table_supported()


def column_supports_sorting(backend_api, data_db, table_name, column_name):
    if backend_api:
        backend_col = backend_api.get_column(data_db, table_name, column_name)
        if backend_api.is_valid_sort_data_type(backend_col.data_type):
            return column_name
        else:
            return None


def offload_sorting_fact_offload1_partition_columns(backend_api):
    if not backend_api or not backend_api.partition_by_column_supported():
        return None
    if backend_api.max_partition_columns() == 1:
        return 'TIME_ID'
    else:
        return 'TIME_ID,CHANNEL_ID'


def offload_sorting_fact_offload1_granularity(backend_api, date=True):
    if backend_api and backend_api.max_partition_columns() <= 1:
        # Let default kick in for date based columns
        return None if date else '1'
    else:
        return '%s,1' % ('M' if date else '1')


def sort_story_assertion(hybrid_schema, hybrid_view, backend_name, data_db, options, backend_api, repo_client,
                         offload_sort_columns='NULL'):
    if not options:
        return True
    if not backend_api.sorted_table_supported():
        # Ignore the desired offload_sort_columns, it should always be blank
        offload_sort_columns = 'NULL'
    if offload_sort_columns is None:
        offload_sort_columns = 'NULL'
    if not check_metadata(hybrid_schema, hybrid_view, repo_client, offload_sort_columns=offload_sort_columns):
        return False
    # Hive doesn't store sort columns in the metastore
    if backend_api.backend_type() != DBTYPE_HIVE:
        table_sort_columns = (backend_api.get_table_sort_columns(data_db, backend_name) or 'NULL').upper()
        if table_sort_columns.upper() != offload_sort_columns.upper():
            log('table_sort_columns (%s) != offload_sort_columns (%s)' % (table_sort_columns, offload_sort_columns))
            return False
    return True


def offload_sorting_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                list_only, repo_client):
    """ Tests --sort-columns Offload option, this option controls backend table sorting/clustering.
        Tests ensures we can define the clustering, change it once already defined and remove it.
    """
    if not list_only and not options:
        return []

    rdbms_dim_be = convert_backend_identifier_case(options, RDBMS_DIM)
    rdbms_fact_be = convert_backend_identifier_case(options, RDBMS_FACT)

    # A number of tests are skipped if table sorting is not supported
    # We let some run though just to prove nothing explodes and sort columns are blanked out

    return [
       {'id': 'offload_sorting_dim_setup',
        'type': STORY_TYPE_SETUP,
        'title': 'Setup Table For Offload Sort Tests',
        'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, RDBMS_DIM),
                  STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, RDBMS_DIM),
                  STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                            data_db, RDBMS_DIM)]}},
       {'id': 'offload_sorting_dim_offload1',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Default Offload Of Dimension',
        'narrative': 'No column defaults for a non-partitioned table',
        'options': {'owner_table': schema + '.' + RDBMS_DIM,
                    'sort_columns_csv': SORT_COLUMNS_NO_CHANGE,
                    'reset_backend_table': True},
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_DIM, rdbms_dim_be, data_db, options, backend_api,
                                               repo_client, offload_sort_columns='NULL'),
             lambda test: True),
        ]},
       {'id': 'offload_sorting_dim_offload2',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Offload Dim With 2 Sorting Columns',
        'options': {'owner_table': schema + '.' + RDBMS_DIM,
                    'reset_backend_table': True,
                    'sort_columns_csv': 'prod_category_id,prod_subcategory_id'},
        'prereq': lambda: dim_sorted_table_supported(backend_api),
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_DIM, rdbms_dim_be, data_db, options, backend_api,
                                               repo_client,
                                               offload_sort_columns='PROD_CATEGORY_ID,PROD_SUBCATEGORY_ID'),
             lambda test: True),
        ]},
       {'id': 'offload_sorting_dim_offload3',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'No-Op Offload Sort Dimension (Error Recovery)',
        'options': {'owner_table': schema + '.' + RDBMS_DIM,
                    'sort_columns_csv': SORT_COLUMNS_NO_CHANGE,
                    'reset_backend_table': False,
                    'force': True},
        'prereq': lambda: dim_sorted_table_supported(backend_api),
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_DIM, rdbms_dim_be, data_db, options, backend_api,
                                               repo_client,
                                               offload_sort_columns='PROD_CATEGORY_ID,PROD_SUBCATEGORY_ID'),
             lambda test: True)
        ]},
       {'id': 'schema_sort_story_dim_offload4',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Offload Dim With 5 Sorting Columns',
        'narrative': 'BigQuery has a limit on cluster columns, this test confirms we throw an exception',
        'options': {'owner_table': schema + '.' + RDBMS_DIM,
                    'reset_backend_table': True,
                    'sort_columns_csv': 'prod_category,prod_category_id,prod_subcategory,prod_subcategory_id,prod_id'},
        'prereq': lambda: dim_sorted_table_supported(backend_api) and backend_api.max_sort_columns() < 5,
        'expected_exception_string': SORT_COLUMN_MAX_EXCEEDED_EXCEPTION_TEXT},
       {'id': 'offload_sorting_dim_bad_col',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Offload Dim With Bad Sort Column - Expect Exception',
        'options': {'owner_table': schema + '.' + RDBMS_DIM,
                    'sort_columns_csv': 'not_a_column,prod_category,prod_subcategory',
                    'reset_backend_table': True},
        'expected_exception_string': UNKNOWN_SORT_COLUMN_EXCEPTION_TEXT},
       {'id': 'offload_sorting_dim_offload5',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Match sort columns with a wildcard',
        'options': {'owner_table': schema + '.' + RDBMS_DIM,
                    'sort_columns_csv': '*price',
                    'reset_backend_table': True,
                    'force': True},
        'prereq': lambda: dim_sorted_table_supported(backend_api),
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_DIM, rdbms_dim_be, data_db, options, backend_api,
                                               repo_client, offload_sort_columns='PROD_LIST_PRICE,PROD_MIN_PRICE'),
             lambda test: True)
        ]},
       {'id': 'offload_sorting_fact_setup',
        'type': STORY_TYPE_SETUP,
        'title': 'Setup Partitioned Table For Offload Sort Tests',
        'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, RDBMS_FACT),
                  STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, RDBMS_FACT),
                  STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                            data_db, RDBMS_FACT)]}},
       {'id': 'offload_sorting_fact_offload1',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Default Offload Of Fact',
        'narrative': 'Expect no sort columns',
        'options': {'owner_table': schema + '.' + RDBMS_FACT,
                    'sort_columns_csv': SORT_COLUMNS_NO_CHANGE,
                    'older_than_date': SALES_BASED_FACT_HV_1,
                    'offload_partition_columns': offload_sorting_fact_offload1_partition_columns(backend_api),
                    'offload_partition_granularity': offload_sorting_fact_offload1_granularity(backend_api),
                    'reset_backend_table': True},
        'prereq': lambda: fact_sorted_table_supported(backend_api),
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_FACT, rdbms_fact_be, data_db, options, backend_api,
                                               repo_client, offload_sort_columns='NULL'),
             lambda test: True)
        ]},
       {'id': 'offload_sorting_fact_offload2',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Offload Fact With Custom Sorting',
        'options': {'owner_table': schema + '.' + RDBMS_FACT,
                    'sort_columns_csv': 'channel_id,promo_id',
                    'older_than_date': SALES_BASED_FACT_HV_1,
                    'reset_backend_table': True},
        'prereq': lambda: fact_sorted_table_supported(backend_api),
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_FACT, rdbms_fact_be, data_db, options, backend_api,
                                               repo_client, offload_sort_columns='CHANNEL_ID,PROMO_ID'),
             lambda test: True)
        ]},
       {'id': 'offload_sorting_fact_offload3',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Fail to Modify Existing Sorting',
        'narrative': 'This only runs if changing the sort columns is not supported and should throw an exception',
        'options': {'owner_table': schema + '.' + RDBMS_FACT,
                    'sort_columns_csv': 'channel_id,promo_id,prod_id',
                    'older_than_date': SALES_BASED_FACT_HV_2},
        'prereq': lambda: fact_sorted_table_supported(backend_api) and not backend_api.sorted_table_modify_supported(),
        'expected_exception_string': SORT_COLUMN_NO_MODIFY_EXCEPTION_TEXT},
       {'id': 'offload_sorting_fact_offload4',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Incremental Offload Of Fact With Custom Sorting',
        'narrative': 'This should alter the table to match the new settings',
        'options': {'owner_table': schema + '.' + RDBMS_FACT,
                    'sort_columns_csv': 'channel_id,promo_id,prod_id',
                    'older_than_date': SALES_BASED_FACT_HV_2},
        'prereq': lambda: fact_sorted_table_supported(backend_api, modify=True),
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_FACT, rdbms_fact_be, data_db, options, backend_api,
                                               repo_client, offload_sort_columns='CHANNEL_ID,PROMO_ID,PROD_ID'),
             lambda test: True)
        ]},
       {'id': 'offload_sorting_fact_offload5',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Incremental Offload Of Fact With Default Sorting',
        'narrative': 'Sort columns remain the same as defined in previous offload',
        'options': {'owner_table': schema + '.' + RDBMS_FACT,
                    'sort_columns_csv': SORT_COLUMNS_NO_CHANGE,
                    'older_than_date': SALES_BASED_FACT_HV_3},
        'prereq': lambda: fact_sorted_table_supported(backend_api, modify=True),
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_FACT, rdbms_fact_be, data_db, options, backend_api,
                                               repo_client, offload_sort_columns='CHANNEL_ID,PROMO_ID,PROD_ID'),
             lambda test: True)
        ]},
       {'id': 'offload_sorting_fact_offload6',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Incremental Offload Of Fact With Empty Sorting',
        'options': {'owner_table': schema + '.' + RDBMS_FACT,
                    'sort_columns_csv': '',
                    'older_than_date': SALES_BASED_FACT_HV_4},
        'prereq': lambda: fact_sorted_table_supported(backend_api, modify=True),
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_FACT, rdbms_fact_be, data_db, options, backend_api,
                                               repo_client, offload_sort_columns='NULL'),
             lambda test: True)
        ]},
       {'id': 'offload_sorting_fact_offload7',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'No-op Offload Changing Sorting',
        'narrative': 'No-op offload (same HV as previous) but still expect to change sort columns',
        'options': {'owner_table': schema + '.' + RDBMS_FACT,
                    'sort_columns_csv': 'channel_id,promo_id',
                    'older_than_date': SALES_BASED_FACT_HV_4,
                    'force': True},
        'prereq': lambda: fact_sorted_table_supported(backend_api, modify=True),
        'assertion_pairs': [
            (lambda test: sort_story_assertion(hybrid_schema, RDBMS_FACT, rdbms_fact_be, data_db, options, backend_api,
                                               repo_client, offload_sort_columns='CHANNEL_ID,PROMO_ID'),
             lambda test: True)
        ]},
       {'id': 'offload_sorting_fact_offload8',
        'type': STORY_TYPE_OFFLOAD,
        'title': 'Offload Fact With 5 Sorting Columns',
        'narrative': 'BigQuery has a limit on cluster columns, this test confirms we throw an exception when altering',
        'options': {'owner_table': schema + '.' + RDBMS_FACT,
                    'sort_columns_csv': 'channel_id,promo_id,prod_id,cust_id,quantity_sold',
                    'older_than_date': SALES_BASED_FACT_HV_5},
        'prereq': lambda: fact_sorted_table_supported(backend_api, modify=True) and backend_api.max_sort_columns() < 5,
        'expected_exception_string': SORT_COLUMN_MAX_EXCEEDED_EXCEPTION_TEXT},
    ]
