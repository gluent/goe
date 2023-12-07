from test_sets.stories.story_globals import OFFLOAD_PATTERN_90_10, STORY_SETUP_TYPE_FRONTEND, STORY_SETUP_TYPE_HYBRID, \
    STORY_SETUP_TYPE_PYTHON, STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import drop_backend_test_table, gen_hybrid_drop_ddl,\
    gen_rdbms_dim_create_ddl, gen_sales_based_fact_create_ddl, get_backend_drop_incr_fns, \
    SALES_BASED_FACT_HV_1, SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_4, SALES_BASED_FACT_HV_5,\
    SALES_BASED_FACT_PRE_HV_NUM, SALES_BASED_FACT_HV_2_NUM, SALES_BASED_FACT_HV_3_NUM, SALES_BASED_FACT_HV_7_NUM
from test_sets.stories.story_assertion_functions import backend_column_exists, get_max_column_length,\
    get_distinct_from_column, get_date_offload_granularity, offload_dim_assertion, offload_fact_assertions,\
    synthetic_part_col_name, text_in_warnings

from gluentlib.offload.backend_table import NULL_BACKEND_PARTITION_VALUE_WARNING_TEXT, PARTITION_KEY_OUT_OF_RANGE
from gluentlib.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_ORACLE, DBTYPE_TERADATA, \
    PART_COL_GRANULARITY_DAY, PART_COL_GRANULARITY_MONTH, PART_COL_GRANULARITY_YEAR
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_messages import VERBOSE
from gluentlib.offload.operation.partition_controls import MISSING_PARTITION_GRANULARITY_EXCEPTION_TEXT,\
    PARTITION_BY_STRING_NOT_SUPPORTED_EXCEPTION_TEXT
from gluentlib.util.better_impyla import HADOOP_TYPE_STRING

from testlib.test_framework.test_functions import log


OFFLOAD_DIM = 'STORY_BP_DIM'
OFFLOAD_FACT = 'STORY_BP_FACT'
OFFLOAD_FACT_BY_NUM = 'STORY_BP_NUM_FACT'
OFFLOAD_FACT_BY_DEC = 'STORY_BP_DEC_FACT'
STR_DATE_FACT = 'STORY_STR_DATE'
OFFLOAD_DIM_NULL = 'STORY_BP_NDIM'


def partition_by_string_supported_exception(backend_api, exception_text=None):
    if backend_api and backend_api.partition_by_string_supported():
        return exception_text
    else:
        return PARTITION_BY_STRING_NOT_SUPPORTED_EXCEPTION_TEXT


def dim_null_setup_frontend_ddl(frontend_api, options, schema, table_name) -> list:
    if not options:
        return []
    ddls = gen_rdbms_dim_create_ddl(frontend_api, schema, table_name)
    if options.db_type == DBTYPE_ORACLE:
        ddls.append("UPDATE %s.%s SET prod_eff_from = NULL WHERE ROWNUM = 1" % (schema, table_name))
    else:
        ddls.append(f"""UPDATE {schema}.{table_name} SET prod_eff_from = NULL
                    WHERE prod_id = (SELECT MIN(prod_id) FROM {schema}.{table_name})""")
    return ddls


def offload_story_str_assertion(options, backend_api, data_db, table_name, part_col, granularity):
    if not backend_api:
        return True
    backend_name = convert_backend_identifier_case(options, table_name)
    synth_part_col = synthetic_part_col_name(granularity, part_col)
    if not backend_column_exists(backend_api, data_db, backend_name, synth_part_col):
        return False
    max_len = get_max_column_length(backend_api, data_db, backend_name, synth_part_col)
    if max_len != granularity:
        log('%s != %s' % (max_len, granularity), detail=VERBOSE)
        return False
    return True


def offload_story_date_assertion(options, backend_api, data_db, table_name, part_col, granularity,
                                 expected_values=None, at_least_values=None):
    if not backend_api:
        return True
    backend_name = convert_backend_identifier_case(options, table_name)
    backend_column = backend_api.get_column(data_db, backend_name, part_col)
    if not backend_column:
        log('offload_story_date_assertion: Missing column %s' % part_col)
        return False
    if backend_api.partition_column_requires_synthetic_column(backend_column, granularity):
        check_part_col = synthetic_part_col_name(granularity, part_col)
        if not backend_column_exists(backend_api, data_db, backend_name, check_part_col):
            return False
        synth_column = backend_api.get_column(data_db, backend_name, check_part_col)
        if synth_column.is_string_based():
            # We can check that the synthetic values are of the correct length
            expected_len = {'Y': 4, 'M': 7, 'D': 10}[granularity]
            max_len = get_max_column_length(backend_api, data_db, backend_name, check_part_col)
            if max_len != expected_len:
                log('%s != %s' % (max_len, expected_len))
                return False
    if expected_values or at_least_values:
        num_parts = backend_api.get_table_partition_count(data_db, backend_name)
        if expected_values and num_parts != expected_values:
            log('num_parts != expected_values: %s != %s' % (num_parts, expected_values))
            return False
        if at_least_values and num_parts < at_least_values:
            log('num_parts < at_least_values: %s < %s' % (num_parts, at_least_values))
            return False
    return True


def offload_story_num_assertion(options, backend_api, data_db, table_name, part_col, granularity, digits,
                                expected_values=None):
    if not backend_api:
        return True
    if backend_api.backend_type() == DBTYPE_BIGQUERY:
        digits = None
    backend_name = convert_backend_identifier_case(options, table_name)
    backend_column = backend_api.get_column(data_db, backend_name, part_col)
    if not backend_column:
        log('offload_story_num_assertion: Missing column %s' % part_col)
        return False
    if backend_api.partition_column_requires_synthetic_column(backend_column, granularity):
        synth_part_col = synthetic_part_col_name(granularity, part_col, synthetic_partition_digits=digits)
        if not backend_column_exists(backend_api, data_db, backend_name, synth_part_col):
            log(f'Missing backend column: {synth_part_col}')
            return False
        if backend_api.synthetic_partition_numbers_are_string():
            max_len = get_max_column_length(backend_api, data_db, backend_name, synth_part_col)
            if max_len != digits:
                log('Invalid data max_len: %s != %s' % (max_len, digits))
                return False
        if expected_values is not None:
            dist_vals = get_distinct_from_column(backend_api, data_db, backend_name, synth_part_col)
            if len(dist_vals) != expected_values:
                log('Incorrect distinct synthetic values: %s != %s' % (len(dist_vals), expected_values))
                return False
    if expected_values is not None:
        num_partitions = backend_api.get_table_partition_count(data_db, backend_name)
        if num_partitions != expected_values:
            log('Incorrect num partitions: %s != %s' % (num_partitions, expected_values))
            return False
    return True


def offload_backend_part_fact_as_date_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                            repo_client, table_name, test_id):
    def get_prereq_fn(backend_api, granularity, hybrid_schema_supported_only=False):
        def hybrid_schema_check():
            return bool(not hybrid_schema_check or frontend_api.hybrid_schema_supported())

        if test_id == 'date':
            return lambda: bool(backend_api.canonical_date_supported()
                                and granularity in backend_api.supported_date_based_partition_granularities()
                                and hybrid_schema_check())
        else:
            return lambda: bool(backend_api.backend_type() == DBTYPE_BIGQUERY
                                and hybrid_schema_check())

    assert test_id in ['date', 'tstz']

    if test_id == 'date':
        column_type_option = 'date_columns_csv'
    else:
        column_type_option = 'timestamp_tz_columns_csv'

    return [{'id': 'offload_backend_part_fact_as_%s_year' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'Offload of Fact %s Partitioned by Year' % test_id.upper(),
             'narrative': """OFFLOAD_FACT is partitioned by a date/time data type in the RDBMS. Offload as %s.
                             Only runs on systems that support a relevant data type.""" % test_id.upper(),
             'options': {'owner_table': schema + '.' + table_name,
                         'older_than_date': SALES_BASED_FACT_HV_2,
                         'reset_backend_table': True,
                         'num_buckets': 1,
                         column_type_option: 'time_id',
                         'offload_partition_columns': 'time_id',
                         'offload_partition_granularity': PART_COL_GRANULARITY_YEAR},
             'assertion_pairs':
                 offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                         hybrid_schema, data_db, table_name, SALES_BASED_FACT_HV_2, bucket_check=1) +
                 [(lambda test: offload_story_date_assertion(options, backend_api, data_db, table_name, 'time_id',
                                                             PART_COL_GRANULARITY_YEAR, at_least_values=1),
                   lambda test: True)],
             'prereq': get_prereq_fn(backend_api, PART_COL_GRANULARITY_YEAR)},
            {'id': 'offload_backend_part_fact_as_%s_month' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'Offload of Fact %s Partitioned by Month' % test_id.upper(),
             'narrative': """OFFLOAD_FACT is partitioned by a date/time data type in the RDBMS. Offload as %s.
                             Only runs on systems that support a relevant data type.""" % test_id.upper(),
             'options': {'owner_table': schema + '.' + table_name,
                         'older_than_date': SALES_BASED_FACT_HV_2,
                         'reset_backend_table': True,
                         'num_buckets': 1,
                         column_type_option: 'time_id',
                         'offload_partition_columns': 'time_id',
                         'offload_partition_granularity': PART_COL_GRANULARITY_MONTH},
             'assertion_pairs':
                 offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                         hybrid_schema, data_db, table_name, SALES_BASED_FACT_HV_2, bucket_check=1) +
                 [(lambda test: offload_story_date_assertion(options, backend_api, data_db, table_name, 'time_id',
                                                             PART_COL_GRANULARITY_MONTH, at_least_values=2),
                   lambda test: True)],
             'prereq': get_prereq_fn(backend_api, PART_COL_GRANULARITY_MONTH)},
            {'id': 'offload_backend_part_fact_as_%s_day1' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'Offload of Fact Native %s Partitioned by Day' % test_id.upper(),
             'narrative': """OFFLOAD_FACT is partitioned by a date/time data type in the RDBMS. Offload as %s.
                             Only runs on systems that support a relevant data type.""" % test_id.upper(),
             'options': {'owner_table': schema + '.' + table_name,
                         'older_than_date': SALES_BASED_FACT_HV_2,
                         'reset_backend_table': True,
                         'num_buckets': 1,
                         column_type_option: 'time_id',
                         'offload_partition_columns': 'time_id',
                         'offload_partition_granularity': PART_COL_GRANULARITY_DAY},
             'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                        schema, hybrid_schema, data_db, table_name,
                                                        SALES_BASED_FACT_HV_2, bucket_check=1) +
                                [(lambda test: offload_story_date_assertion(options, backend_api, data_db, table_name,
                                                                            'time_id', PART_COL_GRANULARITY_DAY,
                                                                            at_least_values=5),
                                  lambda test: True)],
             'prereq': get_prereq_fn(backend_api, PART_COL_GRANULARITY_DAY)},
            {'id': 'offload_backend_part_fact_as_%s_day2_verification' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'IPA Offload when Native %s Partitioned' % test_id.upper(),
             'options': {'owner_table': schema + '.' + table_name,
                         'older_than_date': SALES_BASED_FACT_HV_4},
             'config_override': {'execute': False},
             'prereq': get_prereq_fn(backend_api, PART_COL_GRANULARITY_DAY)},
            {'id': 'offload_backend_part_fact_as_%s_day2_run' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'IPA Offload when Native %s Partitioned' % test_id.upper(),
             'options': {'owner_table': schema + '.' + table_name,
                         'older_than_date': SALES_BASED_FACT_HV_4},
             'assertion_pairs': offload_fact_assertions(
                 options, backend_api, frontend_api, repo_client, schema,
                 hybrid_schema, data_db, table_name, SALES_BASED_FACT_HV_4),
             'prereq': get_prereq_fn(backend_api, PART_COL_GRANULARITY_DAY)},
            {'id': 'offload_backend_part_fact_as_%s_day3' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'IPA Offload when Native %s Partitioned' % test_id.upper(),
             'options': {'owner_table': schema + '.' + table_name,
                         'older_than_date': SALES_BASED_FACT_HV_5,
                         'verify_row_count': 'aggregate'},
             'assertion_pairs': offload_fact_assertions(
                 options, backend_api, frontend_api, repo_client, schema,
                 hybrid_schema, data_db, table_name, SALES_BASED_FACT_HV_5),
             'prereq': get_prereq_fn(backend_api, PART_COL_GRANULARITY_DAY)}]


def offload_backend_part_story_tests(schema, hybrid_schema, data_db, load_db, options,
                                     backend_api, frontend_api, repo_client):
    """ Tests checking that controls for backend partition work as intended.
        Some tests are backend specific because we do different things in different cases.
    """
    if backend_api and not backend_api.partition_by_column_supported():
        log('Skipping offload_backend_part_story_tests() because partition_by_column_supported() == False')
        return []

    option_target = options.target if options else None
    offload_dim_be = convert_backend_identifier_case(options, OFFLOAD_DIM)
    str_date_fact_be = convert_backend_identifier_case(options, STR_DATE_FACT)
    part_by_num_type = None
    if frontend_api:
        part_by_num_type = frontend_api.test_type_canonical_decimal()
    return [
        {'id': 'offload_backend_part_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_DIM,
         'narrative': 'Ensure prod_id < 100 so we know we have 1 or 2 digit values for number partitioning',
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, OFFLOAD_DIM,
                                                                               filter_clause='prod_id > 10 and prod_id < 50'),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_DIM),
                                             lambda: drop_backend_test_table(options, backend_api,
                                                                             load_db, OFFLOAD_DIM)]}},
        {'id': 'offload_backend_part_dim_by_str_1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension',
         'narrative': 'Offload the dimension partitioned by a string - substr 1',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'prod_category_desc',
                     'offload_partition_granularity': '1',
                     'purge_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, OFFLOAD_DIM, bucket_check=1, check_aggs=False),
              lambda test: True),
             (lambda test: offload_story_str_assertion(options, backend_api, data_db,
                                                       offload_dim_be, 'prod_category_desc', 1),
              lambda test: True)],
         'prereq': lambda: backend_api.partition_by_string_supported()},
        {'id': 'offload_backend_part_dim_by_str_2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension',
         'narrative': 'Offload the dimension partitioned by a string - substr 2',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'prod_category_desc',
                     'offload_partition_granularity': '2',
                     'purge_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                 data_db, OFFLOAD_DIM, bucket_check=1, check_aggs=False),
              lambda test: True),
             (lambda test: offload_story_str_assertion(options, backend_api, data_db,
                                                       offload_dim_be, 'prod_category_desc', 2),
              lambda test: True)],
         'prereq': lambda: backend_api.partition_by_string_supported()},
        {'id': 'offload_backend_part_dim_by_num_fail',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Partitioning by Insufficient Number Range',
         'narrative': "Offload table partitioned by a number range that doesn't fit in config, should see warning",
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'offload_partition_columns': 'prod_id',
                     'offload_partition_lower_value': 0,
                     'offload_partition_upper_value': 1,
                     'offload_partition_granularity': '100',
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema,
                 hybrid_schema, data_db, OFFLOAD_DIM),
              lambda test: True),
             (lambda test: text_in_warnings(test.offload_messages,
                                            PARTITION_KEY_OUT_OF_RANGE.format(column='PROD_ID', start=0, end=1)),
              lambda test: True)],
         'prereq': lambda: bool(option_target == DBTYPE_BIGQUERY)},
        {'id': 'offload_backend_part_dim_by_num_1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Partitioning by Number',
         'narrative': """Offload the dimension partitioned by a number - granularity 100 should give a single partition.
                         This should be true of all backends.""",
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'prod_id',
                     'offload_partition_granularity': '100',
                     'offload_partition_lower_value': 0,
                     'offload_partition_upper_value': 1000,
                     'synthetic_partition_digits': 5,
                     'purge_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, OFFLOAD_DIM,
                                                 bucket_check=1, check_aggs=False),
              lambda test: True),
             (lambda test: offload_story_num_assertion(options, backend_api, data_db, OFFLOAD_DIM, 'prod_id',
                                                       100, 5, expected_values=1),
              lambda test: True)]},
        {'id': 'offload_backend_part_dim_by_num_2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Partitioning by Number',
         'narrative': 'Offload the dimension partitioned by a number - granularity 10 should give 4 partitions. This should be true of all backends.',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'prod_id',
                     'offload_partition_granularity': '10',
                     'offload_partition_lower_value': 0,
                     'offload_partition_upper_value': 1000,
                     'synthetic_partition_digits': 8,
                     'purge_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client, schema,
                 hybrid_schema, data_db, OFFLOAD_DIM, bucket_check=1, check_aggs=False),
              lambda test: True),
             (lambda test: offload_story_num_assertion(options, backend_api, data_db, OFFLOAD_DIM, 'prod_id',
                                                       10, 8, expected_values=4),
              lambda test: True)]},
        {'id': 'offload_backend_part_dim_by_num_3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Partitioning by Number (Expect to fail)',
         'narrative': 'Offload the dimension partitioned by a number without mandatory lower/upper options',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'prod_id',
                     'offload_partition_granularity': '10',
                     'offload_partition_lower_value': None,
                     'offload_partition_upper_value': None},
         'expected_exception_string': '--partition-lower-value',
         'prereq': lambda: bool(option_target == DBTYPE_BIGQUERY)},
        {'id': 'offload_backend_part_dim_by_num_4',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Partitioning by Fractional Number',
         'narrative': 'Offload the dimension partitioned by a fractional number.',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'PROD_LIST_PRICE',
                     'offload_partition_granularity': '1000',
                     'offload_partition_lower_value': 0,
                     'offload_partition_upper_value': 5000,
                     'synthetic_partition_digits': 8,
                     'purge_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, OFFLOAD_DIM,
                                                 bucket_check=1, check_aggs=False),
              lambda test: True),
             (lambda test: offload_story_num_assertion(options, backend_api, data_db, OFFLOAD_DIM, 'PROD_LIST_PRICE',
                                                       1000, 8, expected_values=2),
              lambda test: True)]},
        {'id': 'offload_backend_part_dim_by_wildcard',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload using Wildcard Matching for Partition Column',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'offload_partition_columns': '*_LIST_PRICE',
                     'offload_partition_granularity': '1000',
                     'offload_partition_lower_value': 0,
                     'offload_partition_upper_value': 5000,
                     'synthetic_partition_digits': 8},
         'assertion_pairs': [
             (lambda test: offload_story_num_assertion(options, backend_api, data_db, OFFLOAD_DIM,
                                                       'PROD_LIST_PRICE', 1000, 8),
              lambda test: True)]},
        {'id': 'offload_backend_part_fact_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_FACT,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, OFFLOAD_FACT,
                                                                                      extra_pred='AND EXTRACT(DAY FROM time_id) <= 10'),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_FACT)]}},
        {'id': 'offload_backend_part_fact_by_year',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of Fact Partitioned by Year',
         'narrative': 'Offloads 2 months of partitions (in same year) from a fact table and checks single backend partition.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'time_id',
                     'offload_partition_granularity': PART_COL_GRANULARITY_YEAR},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_2,
                                     bucket_check=1, check_aggs=False) +
             [(lambda test: offload_story_date_assertion(options, backend_api, data_db, OFFLOAD_FACT, 'time_id',
                                                         PART_COL_GRANULARITY_YEAR, expected_values=1),
               lambda test: True)],
         'prereq': lambda: bool(PART_COL_GRANULARITY_YEAR in backend_api.supported_date_based_partition_granularities())},
        {'id': 'offload_backend_part_fact_by_month',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of Fact Partitioned by Month',
         'narrative': 'Offloads 2 months of partitions from a fact table and checks 2 backend partitions.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'time_id',
                     'offload_partition_granularity': PART_COL_GRANULARITY_MONTH},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_2,
                                     bucket_check=1, check_aggs=False) +
             [(lambda test: offload_story_date_assertion(options, backend_api, data_db, OFFLOAD_FACT, 'time_id',
                                                         PART_COL_GRANULARITY_MONTH, expected_values=2),
               lambda test: True)],
         'prereq': lambda: bool(PART_COL_GRANULARITY_MONTH in backend_api.supported_date_based_partition_granularities())},
        {'id': 'offload_backend_part_fact_by_day',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of Fact Partitioned by Day',
         'narrative': 'Offload 1 monthly partition from a fact table and check backend values.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'time_id',
                     'offload_partition_granularity': PART_COL_GRANULARITY_DAY},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, OFFLOAD_FACT, SALES_BASED_FACT_HV_2,
                                     bucket_check=1, check_aggs=False) +
             [(lambda test: offload_story_date_assertion(options, backend_api, data_db, OFFLOAD_FACT, 'time_id',
                                                         PART_COL_GRANULARITY_DAY, at_least_values=5),
               lambda test: True)]}] + \
        offload_backend_part_fact_as_date_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                                repo_client, OFFLOAD_FACT, 'date') + \
        offload_backend_part_fact_as_date_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                                repo_client, OFFLOAD_FACT, 'tstz') + [ \
        {'id': 'offload_backend_part_fact_by_str_date_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % STR_DATE_FACT,
         'narrative': 'Used to confirm that we can partition the backend table by a date offloaded to string.',
         'setup': {'oracle': gen_sales_based_fact_create_ddl(frontend_api, schema, STR_DATE_FACT,
                                                             enable_row_movement=True),
                   'hybrid': gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, STR_DATE_FACT),
                   'python': get_backend_drop_incr_fns(options, backend_api, data_db, STR_DATE_FACT)}},
        {'id': 'offload_backend_part_fact_by_str_date_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st partition of %s (Expect to Fail)' % STR_DATE_FACT,
         'narrative': 'Offload 1st partition with TIME_ID as a STRING in backend with date based granularity rather than string based',
         'options': {'owner_table': schema + '.' + STR_DATE_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_1,
                     'variable_string_columns_csv': 'time_id',
                     'reset_backend_table': True},
         'expected_exception_string': partition_by_string_supported_exception(backend_api, MISSING_PARTITION_GRANULARITY_EXCEPTION_TEXT)},
        {'id': 'offload_backend_part_fact_by_str_date_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st partition of %s' % STR_DATE_FACT,
         'narrative': 'Offload 1st partition with TIME_ID as a STRING in backend. We have assertion for backends that support this and an expected exception for those that do not',
         'options': {'owner_table': schema + '.' + STR_DATE_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_1,
                     'variable_string_columns_csv': 'time_id',
                     'offload_partition_granularity': '4',
                     'reset_backend_table': True},
         'expected_exception_string': partition_by_string_supported_exception(backend_api),
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, STR_DATE_FACT, SALES_BASED_FACT_HV_1,
                                     offload_pattern=OFFLOAD_PATTERN_90_10) +
             [(lambda test: backend_column_exists(backend_api, data_db, str_date_fact_be,
                                                  'time_id', HADOOP_TYPE_STRING),
               lambda test: True),
              (lambda test: backend_column_exists(backend_api, data_db, str_date_fact_be,
                                                  'gl_part_4_time_id', HADOOP_TYPE_STRING),
               lambda test: True)]},
        {'id': 'offload_backend_part_fact_by_str_date_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 2nd partition of %s' % STR_DATE_FACT,
         'narrative': 'Offload 2nd partition with TIME_ID as a STRING in backend',
         'options': {'owner_table': schema + '.' + STR_DATE_FACT,
                     'older_than_date': SALES_BASED_FACT_HV_2},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, STR_DATE_FACT,
                                                    SALES_BASED_FACT_HV_2, offload_pattern=OFFLOAD_PATTERN_90_10),
         'prereq': backend_api and backend_api.partition_by_string_supported()},
        {'id': 'offload_backend_part_dim_null_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_DIM_NULL,
         'narrative': 'Ensure we have some NULLs in the partition column to check validation works',
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: dim_null_setup_frontend_ddl(frontend_api, options, schema, OFFLOAD_DIM_NULL),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_DIM_NULL),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_DIM_NULL),
                                             lambda: drop_backend_test_table(options, backend_api,
                                                                             load_db, OFFLOAD_DIM_NULL)]}},
        {'id': 'offload_backend_part_dim_null_run',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with NULL Partition Column',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM_NULL,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'prod_eff_from',
                     'offload_partition_granularity': get_date_offload_granularity(backend_api,
                                                                                   PART_COL_GRANULARITY_YEAR)},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, OFFLOAD_DIM),
              lambda test: True),
             (lambda test: text_in_warnings(test.offload_messages, NULL_BACKEND_PARTITION_VALUE_WARNING_TEXT),
              lambda test: True)]},
        {'id': 'offload_backend_part_fact_by_num_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_FACT_BY_NUM,
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, OFFLOAD_FACT_BY_NUM,
                                                               extra_pred='AND EXTRACT(DAY FROM time_id) <= 10',
                                                               part_key_type=part_by_num_type),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_FACT_BY_NUM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_FACT_BY_NUM)]}},
        {'id': 'offload_backend_part_fact_by_num_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of Fact Partitioned by Number',
         'narrative': 'Offloads 2 months of partitions from a fact table and checks 2 backend partitions.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT_BY_NUM,
                     'less_than_value': SALES_BASED_FACT_HV_2_NUM,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'offload_partition_columns': 'time_id',
                     'offload_partition_granularity': '100',
                     'synthetic_partition_digits': 10,
                     'offload_partition_lower_value': SALES_BASED_FACT_PRE_HV_NUM,
                     'offload_partition_upper_value': SALES_BASED_FACT_HV_7_NUM},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, OFFLOAD_FACT_BY_NUM, SALES_BASED_FACT_HV_2_NUM,
                                     bucket_check=1, check_aggs=False, incremental_key_type=part_by_num_type) +
             [(lambda test: offload_story_num_assertion(options, backend_api, data_db, OFFLOAD_FACT_BY_NUM,
                                                        'time_id', 100, 10, expected_values=2),
               lambda test: True)],
         # TODO In Teradata MVP we don't yet support numeric frontend partitioning
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_backend_part_fact_by_num_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Partition Append Offload Partitioned by Number',
         'narrative': 'Offload next partition from a fact table and checks 3 backend partitions.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT_BY_NUM,
                     'less_than_value': SALES_BASED_FACT_HV_3_NUM},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, OFFLOAD_FACT_BY_NUM, SALES_BASED_FACT_HV_3_NUM,
                                     bucket_check=1, check_aggs=False, incremental_key_type=part_by_num_type) +
             [(lambda test: offload_story_num_assertion(options, backend_api, data_db, OFFLOAD_FACT_BY_NUM,
                                                        'time_id', 100, 10, expected_values=3),
               lambda test: True)],
         # TODO In Teradata MVP we don't yet support numeric frontend partitioning
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_backend_part_fact_by_dec_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_FACT_BY_DEC,
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, OFFLOAD_FACT_BY_DEC,
                                                               extra_pred='AND EXTRACT(DAY FROM time_id) <= 10',
                                                               part_key_type=part_by_num_type),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_FACT_BY_DEC),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_FACT_BY_DEC)]},
         # TODO In Teradata MVP we don't yet support numeric frontend partitioning
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_backend_part_fact_by_dec_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload of Fact Partitioned by Decimal',
         'narrative': 'Offloads 2 months of partitions from a fact table and checks 2 backend partitions.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT_BY_DEC,
                     'less_than_value': SALES_BASED_FACT_HV_2_NUM,
                     'reset_backend_table': True,
                     'num_buckets': 1,
                     'decimal_columns_csv_list': ['time_id'],
                     'decimal_columns_type_list': ['10,2'],
                     'offload_partition_columns': 'time_id',
                     'offload_partition_granularity': '100',
                     'synthetic_partition_digits': 10,
                     'offload_partition_lower_value': SALES_BASED_FACT_PRE_HV_NUM,
                     'offload_partition_upper_value': SALES_BASED_FACT_HV_7_NUM},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, OFFLOAD_FACT_BY_DEC, SALES_BASED_FACT_HV_2_NUM,
                                     bucket_check=1, check_aggs=False, incremental_key_type=part_by_num_type) +
             [(lambda test: offload_story_num_assertion(options, backend_api, data_db, OFFLOAD_FACT_BY_DEC,
                                                        'time_id', 100, 10, expected_values=2),
               lambda test: True)],
         # TODO In Teradata MVP we don't yet support numeric frontend partitioning
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_backend_part_fact_by_dec_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Partition Append Offload Partitioned by Number',
         'narrative': 'Offload next partition from a fact table and checks 3 backend partitions.',
         'options': {'owner_table': schema + '.' + OFFLOAD_FACT_BY_DEC,
                     'less_than_value': SALES_BASED_FACT_HV_3_NUM},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, OFFLOAD_FACT_BY_DEC, SALES_BASED_FACT_HV_3_NUM,
                                     bucket_check=1, check_aggs=False, incremental_key_type=part_by_num_type) +
             [(lambda test: offload_story_num_assertion(options, backend_api, data_db, OFFLOAD_FACT_BY_DEC,
                                                        'time_id', 100, 10, expected_values=3),
               lambda test: True)],
         # TODO In Teradata MVP we don't yet support numeric frontend partitioning
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
    ]
