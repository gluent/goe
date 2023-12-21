from test_sets.stories.story_globals import STORY_SETUP_TYPE_FRONTEND, STORY_SETUP_TYPE_HYBRID, \
    STORY_SETUP_TYPE_PYTHON, STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import drop_backend_test_table, gen_hybrid_drop_ddl,\
    gen_rdbms_dim_create_ddl
from test_sets.stories.story_assertion_functions import backend_column_exists, offload_dim_assertion,\
    synthetic_part_col_name

from goe.offload.backend_table import PARTITION_FUNCTION_ARG_COUNT_EXCEPTION_TEXT,\
    PARTITION_FUNCTION_DOES_NOT_EXIST_EXCEPTION_TEXT, PARTITION_FUNCTION_ARG_TYPE_EXCEPTION_TEXT
from goe.offload.bigquery.bigquery_column import BIGQUERY_TYPE_FLOAT64,\
    BIGQUERY_TYPE_INT64
from goe.offload.offload_constants import DBTYPE_IMPALA
from goe.offload.offload_functions import convert_backend_identifier_case
from goe.offload.offload_messages import VERBOSE
from goe.offload.operation.partition_controls import PARTITION_FUNCTIONS_ELEMENT_EXCEPTION_TEXT
from testlib.test_framework.test_constants import PARTITION_FUNCTION_TEST_FROM_INT8,\
    PARTITION_FUNCTION_TEST_FROM_DEC1, PARTITION_FUNCTION_TEST_FROM_DEC2, PARTITION_FUNCTION_TEST_FROM_STRING
from testlib.test_framework.test_functions import log


DIM_NUM = 'story_pfn_dim_n'
DIM_DEC = 'story_pfn_dim_d1'
DIM_DEC2 = 'story_pfn_dim_d2'
DIM_STR = 'story_pfn_dim_s'

# UDF names
INT8_UDF = PARTITION_FUNCTION_TEST_FROM_INT8
INT38_UDF = PARTITION_FUNCTION_TEST_FROM_DEC1
DEC19_UDF = PARTITION_FUNCTION_TEST_FROM_DEC2
STRING_UDF = PARTITION_FUNCTION_TEST_FROM_STRING
NO_ARG_UDF = 'TEST_NO_ARG_UDF'
TWO_ARG_UDF = 'TEST_TWO_ARG_UDF'
DOUBLE_UDF = 'TEST_INT_TO_FLOAT64'


def create_extra_test_bigquery_udf_fns(backend_api, data_db):
    """ In addition to the std UDFs created in "test --setup" we want a few extras for testing exceptions """
    if not backend_api:
        return []
    udf_fns = [
        # Incompatible UDFs
        lambda: backend_api.create_udf(data_db, NO_ARG_UDF, BIGQUERY_TYPE_INT64,
                                       [],
                                       "CAST(12345 AS INT64)",
                                       or_replace=True),
        lambda: backend_api.create_udf(data_db, TWO_ARG_UDF, BIGQUERY_TYPE_INT64,
                                       [('p_arg1', BIGQUERY_TYPE_INT64),
                                        ('p_arg2', BIGQUERY_TYPE_INT64)],
                                       'p_arg1+p_arg2',
                                       or_replace=True),
        lambda: backend_api.create_udf(data_db, DOUBLE_UDF, BIGQUERY_TYPE_FLOAT64,
                                       [('p_arg', BIGQUERY_TYPE_INT64)],
                                       "CAST(p_arg AS FLOAT64)",
                                       or_replace=True),
    ]
    return udf_fns


def offload_dim_options(schema, table_name, option_overrides):
    opts = {'owner_table': schema + '.' + table_name,
            'integer_8_columns_csv': 'prod_id',
            'offload_partition_columns': 'prod_id',
            'offload_partition_granularity': '10',
            'offload_partition_lower_value': 0,
            'offload_partition_upper_value': 5000,
            'reset_backend_table': True}
    opts.update(option_overrides or {})
    return opts


def expected_udf_metadata(options, data_db, udf_name_option):
    if '.' in udf_name_option:
        return udf_name_option
    elif options.udf_db:
        return options.udf_db + '.' + udf_name_option
    elif options.target == DBTYPE_IMPALA:
        return 'default.' + udf_name_option
    else:
        return data_db + '.' + udf_name_option


def backend_column_assertion(options, backend_api, data_db, table_name, column_name):
    backend_name = convert_backend_identifier_case(options, table_name)
    if not backend_column_exists(backend_api, data_db, backend_name, column_name):
        log(f'Column {column_name} not found in backend table {data_db}.{backend_name}')
        return False
    return True


def offload_part_fn_assertion(options, backend_api, data_db, table_name, source_column='prod_id',
                              synth_position=0, udf=None):
    if not options:
        return True
    synth_col = synthetic_part_col_name(f'U{synth_position}', source_column)
    synth_col = convert_backend_identifier_case(options, synth_col)
    if not backend_column_assertion(options, backend_api, data_db, table_name, synth_col):
        return False
    if udf:
        backend_name = convert_backend_identifier_case(options, table_name)
        check_sql = 'SELECT COUNT(*) FROM {} WHERE {}({}) != {}'.format(
            backend_api.enclose_object_reference(data_db, backend_name),
            backend_api.enclose_object_reference(data_db, udf), source_column, synth_col
        )
        mismatch_row = backend_api.execute_query_fetch_one(check_sql, log_level=VERBOSE)
        if not mismatch_row:
            log('No data in backend_table')
            return False
        if mismatch_row[0] != 0:
            log('Unexpected values in backend_table synthetic column for %s rows' % mismatch_row[0])
            return False
    return True


def offload_part_fn_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                hybrid_cursor, repo_client):
    """ This story is testing functionality around requesting and validating partition functions.
        The testing is done on a simple dimension table.
        Partition functions are also tested in the assorted partition append stories:
            $ grep -l "'offload_partition_functions'" test_stories/*.py
            test_stories/offload_esoteric.py
            test_stories/offload_list_rpa.py
            test_stories/offload_lpa.py
            test_stories/offload_part_fn.py
            test_stories/offload_rpa.py
    """

    # TODO Add this test when convert to scenario:
    #{'id': 'offload_story_dim_no_part_funcs',
    #     'type': STORY_TYPE_OFFLOAD,
    #     'title': 'Offload With Partition Functions When Not Supported',
    #     'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
    #                 'reset_backend_table': True,
    #                 'offload_partition_functions': 'anything'},
    #     'expected_exception_string': PARTITION_FUNCTIONS_NOT_SUPPORTED_EXCEPTION_TEXT,
    #     'prereq': lambda: not backend_api.gluent_partition_functions_supported()},

    if backend_api and not backend_api.gluent_partition_functions_supported():
        log('Skipping offload_part_fn_story_tests() due to gluent_partition_functions_supported() == False')
        return []

    return [
        {'id': 'offload_part_fn_dim_n_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % DIM_NUM,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, DIM_NUM,
                                                                               extra_col_tuples=[("'-123'", 'STR_PROD_ID')]),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DIM_NUM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db, DIM_NUM)]}},
        {'id': 'offload_part_fn_dim_d1_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % DIM_DEC,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, DIM_DEC,
                                                                               extra_col_tuples=[("'-123'", 'STR_PROD_ID')]),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DIM_DEC),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db, DIM_DEC)]}},
        {'id': 'offload_part_fn_dim_d2_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % DIM_DEC2,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, DIM_DEC2,
                                                                               extra_col_tuples=[("'-123'", 'STR_PROD_ID')]),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DIM_DEC2),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db, DIM_DEC2)]}},
        {'id': 'offload_part_fn_dim_s_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % DIM_STR,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, DIM_STR,
                                                                               extra_col_tuples=[("'-123'", 'STR_PROD_ID')]),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DIM_STR),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api, data_db, DIM_STR)]}},
        {'id': 'offload_part_fn_setup_udfs',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Test UDFs',
         'narrative': 'Create a series of UDFs, some incompatible with GDP, for use throughout this story',
         'setup': {STORY_SETUP_TYPE_PYTHON: create_extra_test_bigquery_udf_fns(backend_api, data_db)}},
        {'id': 'offload_part_fn_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with unsupported partition function UDF',
         'options': offload_dim_options(schema, DIM_NUM, {'offload_partition_functions': NO_ARG_UDF}),
         'expected_exception_string': PARTITION_FUNCTION_ARG_COUNT_EXCEPTION_TEXT},
        {'id': 'offload_part_fn_fail2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with unsupported partition function UDF',
         'options': offload_dim_options(schema, DIM_NUM, {'offload_partition_functions': TWO_ARG_UDF}),
         'expected_exception_string': PARTITION_FUNCTION_ARG_COUNT_EXCEPTION_TEXT},
        {'id': 'offload_part_fn_fail3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with non-existent partition function UDF',
         'options': offload_dim_options(schema, DIM_NUM, {'offload_partition_functions': INT8_UDF + '-not-real'}),
         'expected_exception_string': PARTITION_FUNCTION_DOES_NOT_EXIST_EXCEPTION_TEXT},
        {'id': 'offload_part_fn_fail4',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with STRING input to INT8 partition function',
         'options': offload_dim_options(schema, DIM_NUM, {'offload_partition_columns': 'PROD_CATEGORY_DESC',
                                                          'offload_partition_functions': INT8_UDF}),
         'expected_exception_string': PARTITION_FUNCTION_ARG_TYPE_EXCEPTION_TEXT},
        {'id': 'offload_part_fn_fail5',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with DATE input to partition function',
         'options': offload_dim_options(schema, DIM_NUM, {'offload_partition_columns': 'PROD_EFF_FROM',
                                                          'offload_partition_functions': INT8_UDF,
                                                          'offload_partition_granularity': 'M'}),
         'expected_exception_string': PARTITION_FUNCTION_ARG_TYPE_EXCEPTION_TEXT},
        {'id': 'offload_part_fn_fail6',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with too many partition functions',
         'options': offload_dim_options(schema, DIM_NUM, {'offload_partition_columns': 'PROD_EFF_FROM',
                                                          'offload_partition_functions': INT8_UDF + ',' + INT38_UDF,
                                                          'offload_partition_granularity': 'M'}),
         'expected_exception_string': PARTITION_FUNCTIONS_ELEMENT_EXCEPTION_TEXT},
        {'id': 'offload_part_fn_dim_n_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_NUM,
         'narrative': 'Offload with db prefixed partition function UDF',
         'options': offload_dim_options(schema, DIM_NUM,
                                        {'offload_partition_functions': data_db + '.' + INT8_UDF}),
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client,
                 schema, hybrid_schema, data_db, DIM_NUM,
                 partition_functions=expected_udf_metadata(options, data_db, data_db + '.' + INT8_UDF)),
              lambda test: True),
             (lambda test: offload_part_fn_assertion(options, backend_api, data_db, DIM_NUM, udf=INT8_UDF),
              lambda test: True)]},
        {'id': 'offload_part_fn_dim_n_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_NUM,
         'narrative': 'Offload with non-prefixed INT64 partition function UDF',
         'options': offload_dim_options(schema, DIM_NUM, {'offload_partition_functions': INT8_UDF}),
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client,
                 schema, hybrid_schema, data_db, DIM_NUM,
                 partition_functions=expected_udf_metadata(options, data_db, INT8_UDF)),
              lambda test: True),
             (lambda test: offload_part_fn_assertion(options, backend_api, data_db, DIM_NUM, udf=INT8_UDF),
              lambda test: True)]},
        {'id': 'offload_part_fn_dim_d_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_DEC,
         'narrative': 'Offload with a BIGNUMERIC partition function UDF',
         'options': offload_dim_options(schema, DIM_DEC, {'integer_38_columns_csv': 'prod_id',
                                                          'integer_8_columns_csv': None,
                                                          'offload_partition_functions': INT38_UDF}),
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client,
                 schema, hybrid_schema, data_db, DIM_DEC,
                 partition_functions=expected_udf_metadata(options, data_db, INT38_UDF)),
              lambda test: True),
             (lambda test: offload_part_fn_assertion(options, backend_api, data_db, DIM_DEC, udf=INT38_UDF),
              lambda test: True)]},
        {'id': 'offload_part_fn_dim_d2_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_DEC2,
         'narrative': 'Offload with a BIGNUMERIC partition function UDF',
         'options': offload_dim_options(schema, DIM_DEC2, {'decimal_columns_csv_list': ['prod_id'],
                                                           'decimal_columns_type_list': ['19,0'],
                                                           'integer_8_columns_csv': None,
                                                           'offload_partition_functions': DEC19_UDF}),
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client,
                 schema, hybrid_schema, data_db, DIM_DEC2,
                 partition_functions=expected_udf_metadata(options, data_db, DEC19_UDF)),
              lambda test: True),
             (lambda test: offload_part_fn_assertion(options, backend_api, data_db, DIM_DEC2, udf=DEC19_UDF),
              lambda test: True)]},
        {'id': 'offload_part_fn_dim_s_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_STR,
         'narrative': 'Offload with a STRING partition function UDF',
         'options': offload_dim_options(schema, DIM_STR, {'offload_partition_columns': 'str_prod_id',
                                                          'offload_partition_functions': STRING_UDF}),
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(
                 test, options, backend_api, frontend_api, repo_client,
                 schema, hybrid_schema, data_db, DIM_STR,
                 partition_functions=expected_udf_metadata(options, data_db, STRING_UDF)),
              lambda test: True),
             (lambda test: offload_part_fn_assertion(options, backend_api, data_db, DIM_STR,
                                                     source_column='str_prod_id', udf=STRING_UDF),
              lambda test: True)]},
        ]
