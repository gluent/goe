from random import randint
from textwrap import dedent

from numpy import datetime64

from test_sets.stories.story_globals import STORY_SETUP_TYPE_FRONTEND, STORY_SETUP_TYPE_HYBRID, \
    STORY_SETUP_TYPE_PYTHON, STORY_TYPE_LINK_ID, STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import dbms_stats_gather_string, dbms_stats_delete_string, \
    drop_backend_test_table, drop_backend_test_load_table, gen_hybrid_drop_ddl, gen_rdbms_dim_create_ddl, \
    no_query_import_transport_method
from test_sets.stories.story_assertion_functions import backend_column_exists, frontend_column_exists, \
    table_minus_row_count, text_in_messages
from test_sets.stories.test_stories.offload_esoteric import hint_text_in_log

from gluent import DECIMAL_COL_TYPE_SYNTAX_TEMPLATE, verbose
from gluentlib.offload.backend_table import CAST_VALIDATION_EXCEPTION_TEXT, DATA_VALIDATION_SCALE_EXCEPTION_TEXT
from gluentlib.offload.column_metadata import match_table_column,\
    GLUENT_TYPE_FIXED_STRING, GLUENT_TYPE_VARIABLE_STRING, GLUENT_TYPE_INTEGER_1,\
    GLUENT_TYPE_INTEGER_2, GLUENT_TYPE_INTEGER_4, GLUENT_TYPE_INTEGER_8, GLUENT_TYPE_DECIMAL,\
    GLUENT_TYPE_DOUBLE, GLUENT_TYPE_DATE
from gluentlib.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_IMPALA, DBTYPE_ORACLE, DBTYPE_TERADATA,\
    INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT, OFFLOAD_BUCKET_NAME
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_source_table import DATA_SAMPLE_SIZE_AUTO, DATETIME_STATS_SAMPLING_OPT_ACTION_TEXT, \
    COLUMNS_FAILED_SAMPLING_EXCEPTION_TEXT
from gluentlib.offload.operation.data_type_controls import CONFLICTING_DATA_TYPE_OPTIONS_EXCEPTION_TEXT
from testlib.test_framework.backend_testing_api import STORY_TEST_OFFLOAD_NUMS_BARE_NUM,\
    STORY_TEST_OFFLOAD_NUMS_BARE_FLT, \
    STORY_TEST_OFFLOAD_NUMS_NUM_4, STORY_TEST_OFFLOAD_NUMS_NUM_18, STORY_TEST_OFFLOAD_NUMS_NUM_19, \
    STORY_TEST_OFFLOAD_NUMS_NUM_3_2, STORY_TEST_OFFLOAD_NUMS_NUM_13_3, STORY_TEST_OFFLOAD_NUMS_NUM_16_1, \
    STORY_TEST_OFFLOAD_NUMS_NUM_20_5, STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4, STORY_TEST_OFFLOAD_NUMS_NUM_3_5, \
    STORY_TEST_OFFLOAD_NUMS_NUM_10_M5, STORY_TEST_OFFLOAD_NUMS_DEC_10_0, STORY_TEST_OFFLOAD_NUMS_DEC_13_9, \
    STORY_TEST_OFFLOAD_NUMS_DEC_15_9, STORY_TEST_OFFLOAD_NUMS_DEC_36_3, STORY_TEST_OFFLOAD_NUMS_DEC_37_3, \
    STORY_TEST_OFFLOAD_NUMS_DEC_38_3
from testlib.test_framework.test_functions import log


OFFLOAD_DIM = 'STORY_DC_DIM'
DATE_DIM = 'STORY_DATES'
DATE_SDIM = 'STORY_SDATES'
NUMS_DIM = 'STORY_NUMS'
NUM_TOO_BIG_DIM = 'STORY_NUM_TB'
WILDCARD_DIM = 'STORY_DC_COLS'

# TODO nj@2020-03-31 when GOE-1528 is fixed we should change bad years below from 0001 to -1000
BAD_DT = '0001-01-01'


def cast_validation_exception_text(backend_api):
    """ Get expected exception text when cast validation fails """
    if backend_api:
        if backend_api.backend_type() == DBTYPE_IMPALA:
            return CAST_VALIDATION_EXCEPTION_TEXT
    return DATA_VALIDATION_SCALE_EXCEPTION_TEXT


def num_of_size(digits):
    return ''.join([str(randint(1, 9)) for _ in range(digits)])


def nums_setup_frontend_ddl(frontend_api, backend_api, options, schema, table_name) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        setup_casts = {STORY_TEST_OFFLOAD_NUMS_BARE_NUM: 'CAST(1.101 AS NUMBER)',
                       STORY_TEST_OFFLOAD_NUMS_BARE_FLT: 'CAST(1 AS FLOAT)',
                       STORY_TEST_OFFLOAD_NUMS_NUM_4: 'CAST(1234 AS NUMBER(4))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_18: 'CAST(1 AS NUMBER(18))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_19: 'CAST(1 AS NUMBER(19))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_3_2: 'CAST(1.12 AS NUMBER(3,2))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_13_3: 'CAST(1.123 AS NUMBER(13,3))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_16_1: 'CAST(1.1 AS NUMBER(16,1))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_20_5: 'CAST(1.12345 AS NUMBER(20,5))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4: 'CAST(1.1234 AS NUMBER(*,4))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_3_5: 'CAST(0.001 AS NUMBER(3,5))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_10_M5: 'CAST(1 AS NUMBER(10,-5))',
                       STORY_TEST_OFFLOAD_NUMS_DEC_10_0: 'CAST(1234567890 AS NUMBER)',
                       STORY_TEST_OFFLOAD_NUMS_DEC_13_9: 'CAST(1234.123456789 AS NUMBER)',
                       STORY_TEST_OFFLOAD_NUMS_DEC_15_9: 'CAST(123456.123456789 AS NUMBER)',
                       STORY_TEST_OFFLOAD_NUMS_DEC_36_3: 'CAST(%s.123 AS NUMBER)' % num_of_size(20),
                       STORY_TEST_OFFLOAD_NUMS_DEC_37_3: 'CAST(%s.123 AS NUMBER)' % num_of_size(20),
                       STORY_TEST_OFFLOAD_NUMS_DEC_38_3: 'CAST(%s.123 AS NUMBER)' % num_of_size(20)}
        select_template = 'SELECT {} FROM dual'
    elif options.db_type == DBTYPE_TERADATA:
        setup_casts = {STORY_TEST_OFFLOAD_NUMS_BARE_NUM: 'CAST(1.101 AS NUMBER)',
                       STORY_TEST_OFFLOAD_NUMS_NUM_4: 'CAST(1234 AS NUMBER(4))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_18: 'CAST(1 AS NUMBER(18))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_19: 'CAST(1 AS NUMBER(19))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_3_2: 'CAST(1.12 AS NUMBER(3,2))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_13_3: 'CAST(1.123 AS NUMBER(13,3))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_16_1: 'CAST(1.1 AS NUMBER(16,1))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_20_5: 'CAST(1.12345 AS NUMBER(20,5))',
                       STORY_TEST_OFFLOAD_NUMS_NUM_STAR_4: 'CAST(1.1234 AS NUMBER(*,4))',
                       STORY_TEST_OFFLOAD_NUMS_DEC_10_0: 'CAST(1234567890 AS NUMBER)',
                       STORY_TEST_OFFLOAD_NUMS_DEC_13_9: 'CAST(1234.123456789 AS NUMBER)',
                       STORY_TEST_OFFLOAD_NUMS_DEC_15_9: 'CAST(123456.123456789 AS NUMBER)',
                       STORY_TEST_OFFLOAD_NUMS_DEC_36_3: 'CAST(%s.123 AS NUMBER)' % num_of_size(20),
                       STORY_TEST_OFFLOAD_NUMS_DEC_37_3: 'CAST(%s.123 AS NUMBER)' % num_of_size(20),
                       STORY_TEST_OFFLOAD_NUMS_DEC_38_3: 'CAST(%s.123 AS NUMBER)' % num_of_size(20)}
        select_template = 'SELECT {}'
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    subquery = select_template.format(
        '\n,      '.join('{} AS {}'.format(setup_casts[_], _)
                         for _ in setup_casts
                         if _ in backend_api.story_test_offload_nums_expected_backend_types())
    )
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery, with_stats_collection=True)


def num_too_big_prec_setup_frontend_ddl(frontend_api, options, schema, table_name, with_stats=True) -> list:
    if not options:
        return []
    if options.db_type in DBTYPE_ORACLE:
        subquery = 'SELECT 1 AS id, CAST(%s AS NUMBER(38)) AS num FROM dual' % ('123'.ljust(38, '0'))
    elif options.db_type == DBTYPE_TERADATA:
        subquery = 'SELECT 1 AS id, CAST(%s AS NUMBER(38)) AS num' % (schema, table_name, '123'.ljust(38, '0'))
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery, with_stats_collection=with_stats)


def num_too_big_scale_setup_frontend_ddl(frontend_api, options, schema, table_name) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        subquery = 'SELECT CAST(1 AS NUMBER(4)) AS id, CAST(12.0123456789 AS NUMBER(20,10)) AS num FROM dual'
    elif options.db_type == DBTYPE_TERADATA:
        subquery = 'SELECT CAST(1 AS NUMBER(4)) AS id, CAST(12.0123456789 AS NUMBER(20,10)) AS num'
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery, with_stats_collection=True)


def wildcard_setup_frontend_ddl(frontend_api, options, schema, table_name) -> list:
    if not options:
        return []
    subquery = dedent("""\
        SELECT 1                          AS table_id
        ,      2                          AS cust_id
        ,      3                          AS prod_id
        ,      2012                       AS txn_year
        ,      201209                     AS txn_month
        ,      20120931                   AS txn_day
        ,      4                          AS txn_qtr
        ,      DATE'2012-10-31'           AS txn_date
        ,      17.5                       AS txn_rate
        ,      CAST('ABC' AS VARCHAR(5)) AS txn_desc
        ,      2012                       AS sale_year
        ,      201209                     AS sale_month
        ,      20120931                   AS sale_day
        ,      DATE'2012-10-31'           AS sale_date
        ,      4                          AS sale_qtr
        ,      17.5                       AS sale_rate
        ,      123.55                     AS sale_amt
        ,      CAST('ABC' AS CHAR(3))     AS sale_desc""")
    if options.db_type == DBTYPE_ORACLE:
        subquery += ' FROM dual'
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery, with_stats_collection=True)


def story_dates_setup_frontend_ddl(frontend_api, options, schema, table_name) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        subquery = dedent("""\
        SELECT TRUNC(SYSDATE) AS dt
        ,      CAST(TRUNC(SYSDATE) AS TIMESTAMP(0)) AS ts0
        ,      CAST(TRUNC(SYSDATE) AS TIMESTAMP(6)) AS ts6
        ,      CAST(TIMESTAMP'2001-10-31 01:00:00 -5:00' AS TIMESTAMP(0) WITH TIME ZONE) AS ts0tz
        ,      CAST(TIMESTAMP'2001-10-31 01:00:00 -5:00' AS TIMESTAMP(6) WITH TIME ZONE) AS ts6tz
        FROM   dual""")
    elif options.db_type == DBTYPE_TERADATA:
        subquery = dedent("""\
        SELECT CURRENT_DATE AS dt
        ,      CAST(CURRENT_DATE AS TIMESTAMP(0)) AS ts0
        ,      CAST(CURRENT_DATE AS TIMESTAMP(6)) AS ts6
        ,      CAST(TRUNC(TO_TIMESTAMP_TZ('2001-10-31 01:00:00 -5:00','YYYY-MM-DD HH24:MI:SS TZH:TZM')) AS TIMESTAMP(0) WITH TIME ZONE) AS ts0tz
        ,      CAST(TO_TIMESTAMP_TZ('2001-10-31 01:00:00 -5:00','YYYY-MM-DD HH24:MI:SS TZH:TZM') AS TIMESTAMP(6) WITH TIME ZONE) AS ts6tz""")
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery, with_stats_collection=True)



def story_samp_dates_setup_frontend_ddl(frontend_api, options, schema, table_name) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        subquery = dedent("""\
        SELECT DATE' %(bad_dt)s'                             AS bad_date
        ,      TIMESTAMP'%(bad_dt)s 00:00:00.000000'         AS bad_ts
        --,      TIMESTAMP'1400-01-01 01:00:00 +5:00'          AS bad_tstz
        ,      SYSDATE                                       AS good_date
        ,      CAST(SYSDATE AS TIMESTAMP(6))                 AS good_ts
        ,      TIMESTAMP'1400-01-01 01:00:00 -5:00'          AS good_tstz
        FROM   dual""") % {'bad_dt': BAD_DT}
    elif options.db_type == DBTYPE_TERADATA:
        subquery = dedent("""\
        SELECT DATE '%(bad_dt)s'                             AS bad_date
        ,      TIMESTAMP '%(bad_dt)s 00:00:00.000000'         AS bad_ts
        --,      TIMESTAMP'1400-01-01 01:00:00 +5:00'           AS bad_tstz
        ,      CURRENT_DATE                                  AS good_date
        ,      CAST(CURRENT_DATE AS TIMESTAMP(6))            AS good_ts
        ,      TO_TIMESTAMP_TZ('1400-01-01 01:00:00 -5:00','YYYY-MM-DD HH24:MI:SS TZH:TZM') AS good_tstz""") \
            % {'bad_dt': BAD_DT}
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery, with_stats_collection=True)


def offload_story_nums_assertion(backend_api, frontend_api, test, schema, hybrid_schema, data_db, table_name,
                                 backend_name, detect=True, check_dec_10_0=True):
    if not backend_api:
        return True
    for col_name, expected_type in backend_api.story_test_offload_nums_expected_backend_types(sampling_enabled=detect).items():
        if col_name == STORY_TEST_OFFLOAD_NUMS_DEC_10_0 and not check_dec_10_0:
            # This test only makes sense when we force it to a decimal via options (nums_on test)
            continue
        if not frontend_column_exists(frontend_api, data_db, table_name, col_name):
            # Not all test columns are valid in all frontends
            log('Skipping {col_name} for frontend system', detail=verbose)
            continue
        if not backend_column_exists(backend_api, data_db, backend_name, col_name, search_type=expected_type):
            return False
    if frontend_api.hybrid_schema_supported():
        if table_minus_row_count(test, test.name, schema + '.' + table_name, hybrid_schema + '.' + table_name) != 0:
            return False
    return True


def offload_story_unicode_assertion(backend_api, data_db, backend_name, asserted_unicode_columns):
    assert isinstance(asserted_unicode_columns, dict)
    unicode_column_names = [_.upper() for _ in asserted_unicode_columns.keys()]
    backend_columns = [_ for _ in backend_api.get_columns(data_db, backend_name)
                       if _.name.upper() in unicode_column_names]
    expected_backend_types = backend_api.expected_canonical_to_backend_type_map(
        override_used=['unicode_string_columns_csv'])
    for column_name, expected_canonical_type in asserted_unicode_columns.items():
        backend_column = match_table_column(column_name, backend_columns)
        if backend_column.data_type != expected_backend_types[expected_canonical_type]:
            log('Column %s offloaded to wrong backend type: %s != %s'
                % (backend_column.name.upper(), backend_column.data_type,
                   expected_backend_types[expected_canonical_type]))
            return False
    return True


def offload_story_wildcard_assertion(backend_api, data_db, backend_name):
    if not backend_api:
        return True

    def check_data_type(backend_column, expected_data_type):
        if backend_column.data_type != expected_data_type:
            log('Column %s offloaded to wrong backend type: %s != %s'
                % (backend_column.name.upper(), backend_column.data_type, expected_data_type))
            return False
        return True
    for backend_column in backend_api.get_columns(data_db, backend_name):
        if backend_column.name == OFFLOAD_BUCKET_NAME:
            continue
        if backend_column.name.lower().endswith('_id'):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={'integer_1_columns_csv': backend_column.name})[GLUENT_TYPE_INTEGER_1]
            if not check_data_type(backend_column, expected_backend_type):
                return False
        if backend_column.name.lower().endswith('_year') or backend_column.name.lower().endswith('_qtr'):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={'integer_2_columns_csv': backend_column.name})[GLUENT_TYPE_INTEGER_2]
            if not check_data_type(backend_column, expected_backend_type):
                return False
        if backend_column.name.lower().endswith('_month'):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={'integer_4_columns_csv': backend_column.name})[GLUENT_TYPE_INTEGER_4]
            if not check_data_type(backend_column, expected_backend_type):
                return False
        if backend_column.name.lower().endswith('_day'):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={'integer_8_columns_csv': backend_column.name})[GLUENT_TYPE_INTEGER_8]
            if not check_data_type(backend_column, expected_backend_type):
                return False
        if backend_column.name.lower().endswith('_date'):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={'date_columns_csv': backend_column.name})[GLUENT_TYPE_DATE]
            if not check_data_type(backend_column, expected_backend_type):
                return False
        if backend_column.name.lower().endswith('_rate'):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={'double_columns_csv': backend_column.name})[GLUENT_TYPE_DOUBLE]
            if not check_data_type(backend_column, expected_backend_type):
                return False
        if backend_column.name.lower().endswith('_amt'):
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={'decimal_columns_csv_list': backend_column.name})[GLUENT_TYPE_DECIMAL]
            if not check_data_type(backend_column, expected_backend_type):
                return False
        if backend_column.name.lower().endswith('_desc'):
            input_type = GLUENT_TYPE_FIXED_STRING if backend_column.name.lower() == 'sale_desc'\
                else GLUENT_TYPE_VARIABLE_STRING
            expected_backend_type = backend_api.expected_canonical_to_backend_type_map(
                override_used={'unicode_string_columns_csv': backend_column.name})[input_type]
            if not check_data_type(backend_column, expected_backend_type):
                return False
    return True


def offload_date_assertion(test, frontend_api, backend_api, data_db, table_name,
                           forced_to_date=False, forced_to_tstz=False):
    """ Check outcome of DATE_DIM offloads are correct.
        Has hardcoded Hadoop data types which is necessary because we need to confirm the correct outcome, we
        can't use canonical translation because that's what we are trying to check.
    """
    if not backend_api:
        return True
    if forced_to_date:
        expected_dt_data_type = backend_api.backend_test_type_canonical_date()
        expected_ts_data_type = backend_api.backend_test_type_canonical_date()
    elif forced_to_tstz:
        expected_dt_data_type = backend_api.backend_test_type_canonical_timestamp_tz()
        expected_ts_data_type = backend_api.backend_test_type_canonical_timestamp_tz()
    else:
        if frontend_api.canonical_date_supported():
            expected_dt_data_type = backend_api.backend_test_type_canonical_date()
        else:
            expected_dt_data_type = backend_api.backend_test_type_canonical_timestamp()
        expected_ts_data_type = backend_api.backend_test_type_canonical_timestamp()
    expected_tstz_data_type = backend_api.backend_test_type_canonical_timestamp_tz()
    if not backend_column_exists(backend_api, data_db, table_name, 'dt', search_type=expected_dt_data_type):
        test.offload_messages.log('Backend column does not exist: %s (%s)' % ('dt', expected_dt_data_type))
        return False
    if not backend_column_exists(backend_api, data_db, table_name, 'ts0', search_type=expected_ts_data_type):
        test.offload_messages.log('Backend column does not exist: %s (%s)' % ('ts0', expected_ts_data_type))
        return False
    if not backend_column_exists(backend_api, data_db, table_name, 'ts0tz', search_type=expected_tstz_data_type):
        test.offload_messages.log('Backend column does not exist: %s (%s)' % ('ts0tz', expected_tstz_data_type))
        return False
    return True


def offload_samp_date_assertion(test, backend_api, frontend_api, data_db, backend_name,
                                from_stats=True, good_as_date=False):
    """ Check outcome of DATE_SDIM offloads are correct.
    """
    if not backend_api:
        return True

    if frontend_api.canonical_date_supported():
        expected_good_data_type = backend_api.backend_test_type_canonical_date()
    else:
        expected_good_data_type = backend_api.backend_test_type_canonical_timestamp()
    if backend_api.min_datetime_value() > datetime64(BAD_DT):
        expected_bad_data_type = backend_api.backend_test_type_canonical_string()
    else:
        expected_bad_data_type = expected_good_data_type

    if good_as_date:
        if backend_api.canonical_date_supported():
            expected_good_data_type = backend_api.backend_test_type_canonical_date()

    if not backend_column_exists(backend_api, data_db, backend_name, 'bad_date', search_type=expected_bad_data_type):
        test.offload_messages.log('Backend column does not exist: %s (%s)' % ('bad_date', expected_bad_data_type))
        return False
    if not backend_column_exists(backend_api, data_db, backend_name, 'bad_ts', search_type=expected_bad_data_type):
        test.offload_messages.log('Backend column does not exist: %s (%s)' % ('bad_ts', expected_bad_data_type))
        return False
    if not backend_column_exists(backend_api, data_db, backend_name, 'good_date', search_type=expected_good_data_type):
        test.offload_messages.log('Backend column does not exist: %s (%s)' % ('good_date', expected_good_data_type))
        return False
    if not backend_column_exists(backend_api, data_db, backend_name, 'good_ts', search_type=expected_good_data_type):
        test.offload_messages.log('Backend column does not exist: %s (%s)' % ('good_ts', expected_good_data_type))
        return False
    if expected_bad_data_type != expected_good_data_type:
        text_match = text_in_messages(test.offload_messages, DATETIME_STATS_SAMPLING_OPT_ACTION_TEXT)
        if text_match != from_stats:
            test.offload_messages.log('text_match != from_stats: %s != %s' % (text_match, from_stats))
            return False
    return True


def offload_data_type_controls_story_tests(schema, hybrid_schema, data_db, load_db, options, backend_api, frontend_api):
    max_decimal_precision = backend_api.max_decimal_precision() if backend_api else None
    max_decimal_scale = backend_api.max_decimal_scale() if backend_api else None
    offload_dim_be = convert_backend_identifier_case(options, OFFLOAD_DIM)
    nums_dim_be = convert_backend_identifier_case(options, NUMS_DIM)
    wildcard_dim_be = convert_backend_identifier_case(options, WILDCARD_DIM)
    date_dim_be = convert_backend_identifier_case(options, DATE_DIM)
    date_sdim_be = convert_backend_identifier_case(options, DATE_SDIM)
    return [
        {'id': 'offload_data_type_story_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % OFFLOAD_DIM,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, OFFLOAD_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_DIM),
                                             lambda: drop_backend_test_load_table(options, backend_api,
                                                                                  load_db, OFFLOAD_DIM)]}},
        {'id': 'offload_data_type_story_nums_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Dimension containing all sorts of numbers',
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: nums_setup_frontend_ddl(frontend_api, backend_api,
                                                                              options, schema, NUMS_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, NUMS_DIM),
                   STORY_SETUP_TYPE_PYTHON: [
                       lambda: drop_backend_test_table(options, backend_api, data_db, NUMS_DIM)]}},
        {'id': 'offload_data_type_story_nums_off',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload table containing all sorts of numbers, no detection',
         'narrative': 'Offload table with assorted number columns with number detection disabled, offloads to defaults',
         'options': {'owner_table': schema + '.' + NUMS_DIM,
                     'data_sample_pct': 0,
                     'reset_backend_table': True,
                     'decimal_padding_digits': 2},
         'assertion_pairs': [
             (lambda test: offload_story_nums_assertion(backend_api, frontend_api, test, schema, hybrid_schema,
                                                        data_db, NUMS_DIM, nums_dim_be, detect=False),
              lambda test: True)]},
        {'id': 'offload_data_type_story_nums_on1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload All Sorts of Numbers',
         'narrative': 'Offload table with assorted number columns with number detection enabled and type overrides',
         'options': {'owner_table': schema + '.' + NUMS_DIM,
                     'reset_backend_table': True,
                     'decimal_columns_csv_list': [STORY_TEST_OFFLOAD_NUMS_DEC_10_0, STORY_TEST_OFFLOAD_NUMS_DEC_13_9,
                                                  STORY_TEST_OFFLOAD_NUMS_DEC_15_9, STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
                                                  STORY_TEST_OFFLOAD_NUMS_DEC_37_3, STORY_TEST_OFFLOAD_NUMS_DEC_38_3],
                     'decimal_columns_type_list': ['10,0', '13,9', '15,9', '36,3', '37,3', '38,3'],
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'decimal_padding_digits': 2},
         'assertion_pairs': [
             (lambda test: offload_story_nums_assertion(backend_api, frontend_api, test, schema, hybrid_schema,
                                                        data_db, NUMS_DIM, nums_dim_be, detect=True),
              lambda test: True)]},
        {'id': 'offload_data_type_story_nums_on2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload All Sorts of Numbers No Query Import',
         'narrative': 'Offload table with assorted number columns with number detection enabled and type overrides',
         'options': {'owner_table': schema + '.' + NUMS_DIM,
                     'offload_transport_method': no_query_import_transport_method(options),
                     'reset_backend_table': True,
                     'decimal_columns_csv_list': [STORY_TEST_OFFLOAD_NUMS_DEC_10_0, STORY_TEST_OFFLOAD_NUMS_DEC_13_9,
                                                  STORY_TEST_OFFLOAD_NUMS_DEC_15_9, STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
                                                  STORY_TEST_OFFLOAD_NUMS_DEC_37_3, STORY_TEST_OFFLOAD_NUMS_DEC_38_3],
                     'decimal_columns_type_list': ['10,0', '13,9', '15,9', '36,3', '37,3', '38,3'],
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'decimal_padding_digits': 2},
         'assertion_pairs': [
             (lambda test: offload_story_nums_assertion(backend_api, frontend_api, test, schema, hybrid_schema,
                                                        data_db, NUMS_DIM, nums_dim_be, detect=True),
              lambda test: True)]},
        {'id': 'offload_data_type_story_nums_samp1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension containing all sorts of numbers',
         'narrative': """Offload table with assorted number columns with number detection for sampling.
                         Check the DEC_ columns have correct precision/scale.
                         Still use options for dec_36_3, dec_37_3, dec_38_3 just so the test passes""",
         'options': {'owner_table': schema + '.' + NUMS_DIM,
                     'reset_backend_table': True,
                     'decimal_columns_csv_list': [STORY_TEST_OFFLOAD_NUMS_DEC_36_3, STORY_TEST_OFFLOAD_NUMS_DEC_37_3,
                                                  STORY_TEST_OFFLOAD_NUMS_DEC_38_3],
                     'decimal_columns_type_list': ['36,3', '37,3', '38,3'],
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'decimal_padding_digits': 2},
         'assertion_pairs': [
             (lambda test: offload_story_nums_assertion(backend_api, frontend_api, test, schema, hybrid_schema, data_db,
                                                        NUMS_DIM, nums_dim_be, detect=True, check_dec_10_0=False),
              lambda test: True)]},
        {'id': 'offload_data_type_story_nums_samp2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension With Parallel Sampling=0',
         'narrative': 'Runs with --no-verify to remove risk of verification having a PARALLEL hint',
         'options': {'owner_table': schema + '.' + NUMS_DIM,
                     'data_sample_parallelism': 0,
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'reset_backend_table': True,
                     'verify_row_count': False},
         'assertion_pairs': [
             (lambda test: hint_text_in_log(options, 0, '(offload_data_type_story_nums_samp2)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ]},
        {'id': 'offload_data_type_story_nums_samp3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension With Parallel Sampling=3',
         'narrative': 'Runs with --no-verify to remove risk of verification having a PARALLEL hint',
         'options': {'owner_table': schema + '.' + NUMS_DIM,
                     'data_sample_parallelism': 3,
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'reset_backend_table': True,
                     'verify_row_count': False},
         'assertion_pairs': [
             (lambda test: hint_text_in_log(options, 3, '(offload_data_type_story_nums_samp3)'),
              lambda test: bool(options.db_type == DBTYPE_ORACLE))
         ]},
        {'id': 'offload_data_type_story_nums_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with number overflow (expect to fail)',
         'options': {'owner_table': schema + '.' + NUMS_DIM,
                     'reset_backend_table': True,
                     'decimal_columns_csv_list': [','.join([STORY_TEST_OFFLOAD_NUMS_DEC_36_3,
                                                            STORY_TEST_OFFLOAD_NUMS_DEC_37_3,
                                                            STORY_TEST_OFFLOAD_NUMS_DEC_38_3])],
                     'decimal_columns_type_list': ['10,2'],
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO},
         'expected_exception_string': cast_validation_exception_text(backend_api),
         'prereq': lambda: bool(options.target not in [DBTYPE_BIGQUERY])},
        {'id': 'offload_data_type_story_nums_fail2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload table with bad precision (expect to fail)',
         'options': {'owner_table': schema + '.' + NUMS_DIM,
                     'decimal_columns_csv_list': [','.join([STORY_TEST_OFFLOAD_NUMS_DEC_36_3])],
                     'decimal_columns_type_list': ['100,10'],
                     'reset_backend_table': True},
         'expected_exception_string': DECIMAL_COL_TYPE_SYNTAX_TEMPLATE.format(p=max_decimal_precision,
                                                                              s=max_decimal_scale)},
        {'id': 'offload_data_type_story_nums_fail3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload table with bad scale (expect to fail)',
         'options': {'owner_table': schema + '.' + NUMS_DIM,
                     'decimal_columns_csv_list': [','.join([STORY_TEST_OFFLOAD_NUMS_DEC_36_3])],
                     'decimal_columns_type_list': ['10,100'],
                     'reset_backend_table': True},
         'expected_exception_string': DECIMAL_COL_TYPE_SYNTAX_TEMPLATE.format(p=max_decimal_precision,
                                                                              s=max_decimal_scale)},
        {'id': 'offload_data_type_story_num_too_big_prec_setup1',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % NUM_TOO_BIG_DIM,
         'narrative': """Create a table with data that is too big for the backend and no dbms_stats call.
                         Without optimizer stats sampling we should still catch the bad value.""",
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: num_too_big_prec_setup_frontend_ddl(frontend_api, options,
                                                                                          schema, NUM_TOO_BIG_DIM,
                                                                                          with_stats=False),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, NUM_TOO_BIG_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, NUM_TOO_BIG_DIM)]},
         'prereq': lambda: backend_api.max_decimal_integral_magnitude() < 38},
        {'id': 'offload_data_type_story_num_too_big_prec_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload NUM_TOO_BIG_DIM with number overflow (expect to fail)',
         'options': {'owner_table': schema + '.' + NUM_TOO_BIG_DIM,
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'reset_backend_table': True},
         'expected_exception_string': COLUMNS_FAILED_SAMPLING_EXCEPTION_TEXT,
         'prereq': lambda: backend_api.max_decimal_integral_magnitude() < 38},
        {'id': 'offload_data_type_story_num_too_big_prec_setup2',
         'type': STORY_TYPE_SETUP,
         'title': 'Collect Stats On %s' % NUM_TOO_BIG_DIM,
         'setup': {STORY_SETUP_TYPE_FRONTEND: [dbms_stats_gather_string(schema, NUM_TOO_BIG_DIM,
                                                                        frontend_api=frontend_api)]},
         'prereq': lambda: backend_api.max_decimal_integral_magnitude() < 38},
        {'id': 'offload_data_type_story_num_too_big_prec_fail2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload NUM_TOO_BIG_DIM with number overflow (expect to fail)',
         'options': {'owner_table': schema + '.' + NUM_TOO_BIG_DIM,
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'reset_backend_table': True},
         'expected_exception_string': COLUMNS_FAILED_SAMPLING_EXCEPTION_TEXT,
         'prereq': lambda: backend_api.max_decimal_integral_magnitude() < 38},
        {'id': 'offload_data_type_story_num_too_big_prec_fail3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload NUM_TOO_BIG_DIM with number overflow (expect to fail)',
         'narrative': 'Disable sampling, we will see CAST validation catch the problem data instead',
         'options': {'owner_table': schema + '.' + NUM_TOO_BIG_DIM,
                     'data_sample_pct': 0,
                     'reset_backend_table': True},
         'expected_exception_string': CAST_VALIDATION_EXCEPTION_TEXT,
         'prereq': lambda: backend_api.max_decimal_integral_magnitude() < 38},
        {'id': 'offload_data_type_story_num_too_big_scale_setup1',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % NUM_TOO_BIG_DIM,
         'narrative': 'Create the table with scale that is too big for backend table.',
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: num_too_big_scale_setup_frontend_ddl(frontend_api, options,
                                                                                           schema, NUM_TOO_BIG_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, NUM_TOO_BIG_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, NUM_TOO_BIG_DIM)]}},
        {'id': 'offload_data_type_story_num_too_big_scale_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload NUM_TOO_BIG_DIM with scale overflow based on backend column spec (expect to fail)',
         'options': {'owner_table': schema + '.' + NUM_TOO_BIG_DIM,
                     'decimal_columns_csv_list': ['num'],
                     'decimal_columns_type_list': ['20,5'],
                     'reset_backend_table': True,
                     'decimal_padding_digits': 0},
         'expected_exception_string': DATA_VALIDATION_SCALE_EXCEPTION_TEXT},
        {'id': 'offload_data_type_story_num_too_big_scale_round1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload NUM_TOO_BIG_DIM with scale overflow based on backend column spec',
         'options': {'owner_table': schema + '.' + NUM_TOO_BIG_DIM,
                     'decimal_columns_csv_list': ['num'],
                     'decimal_columns_type_list': ['20,5'],
                     'allow_decimal_scale_rounding': True,
                     'reset_backend_table': True,
                     'decimal_padding_digits': 0}},
        {'id': 'offload_data_type_story_wildcard_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % WILDCARD_DIM,
         'narrative': """Create table with column name patterns as discussed in GOE-1670""",
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: wildcard_setup_frontend_ddl(frontend_api, options,
                                                                                  schema, WILDCARD_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, WILDCARD_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, WILDCARD_DIM)]}},
        {'id': 'offload_data_type_story_wildcard_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with wildcards in data type controls',
         'options': {'owner_table': schema + '.' + WILDCARD_DIM,
                     'reset_backend_table': True,
                     'integer_1_columns_csv': '*_id',
                     'integer_2_columns_csv': '*_year,*_QTR',
                     'integer_4_columns_csv': '*_month',
                     'integer_8_columns_csv': '*_day',
                     'date_columns_csv': '*_date',
                     'double_columns_csv': '*_rate',
                     'decimal_columns_csv_list': ['*amt'],
                     'decimal_columns_type_list': ['12,3'],
                     'unicode_string_columns_csv': '*_desc',
                     'decimal_padding_digits': 0},
         'assertion_pairs': [
             (lambda test: offload_story_wildcard_assertion(backend_api, data_db, wildcard_dim_be),
              lambda test: True)]},
        {'id': 'offload_data_type_story_wildcard_fail',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with overlapping wildcards in data type controls (expect to fail)',
         'options': {'owner_table': schema + '.' + WILDCARD_DIM,
                     'reset_backend_table': True,
                     'integer_1_columns_csv': '*_id',
                     'integer_2_columns_csv': '*id'},
         'expected_exception_string': CONFLICTING_DATA_TYPE_OPTIONS_EXCEPTION_TEXT},
        {'id': 'offload_data_type_story_data_type_control_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with number column as string (expect to fail)',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'variable_string_columns_csv': 'prod_id'},
         'expected_exception_string': INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT},
        {'id': 'offload_data_type_story_data_type_control_fail2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with number column as date (expect to fail)',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'date_columns_csv': 'prod_id'},
         'expected_exception_string': INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT},
        {'id': 'offload_data_type_story_data_type_control_fail3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with date column as number (expect to fail)',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'integer_8_columns_csv': 'PROD_EFF_FROM'},
         'expected_exception_string': INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT},
        {'id': 'offload_data_type_story_data_type_control_fail4',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with string column as date (expect to fail)',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'date_columns_csv': 'PROD_NAME'},
         'expected_exception_string': INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT},
        {'id': 'offload_data_type_story_data_type_control_fail5',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with string column as time zoned date (expect to fail)',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'timestamp_tz_columns_csv': 'PROD_NAME'},
         'expected_exception_string': INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT},
        {'id': 'offload_data_type_story_data_type_control_fail6',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with string column as number (expect to fail)',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'integer_4_columns_csv': 'PROD_NAME'},
         'expected_exception_string': INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT},
        {'id': 'offload_data_type_story_data_type_control_fail7',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with number column as unicode string (expect to fail)',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'unicode_string_columns_csv': 'PROD_ID'},
         'expected_exception_string': INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT},
        {'id': 'offload_data_type_story_string_as_unicode',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension with string column as unicode string',
         'options': {'owner_table': schema + '.' + OFFLOAD_DIM,
                     'reset_backend_table': True,
                     'unicode_string_columns_csv': 'PROD_NAME',
                     'skip': ['verify_exported_data']},
         'assertion_pairs': [
             (lambda test: offload_story_unicode_assertion(backend_api, data_db, offload_dim_be,
                                                           {'PROD_NAME': GLUENT_TYPE_VARIABLE_STRING}),
              lambda test: True)]},
        {'id': 'offload_story_dates_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Dimension containing date based columns',
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: story_dates_setup_frontend_ddl(frontend_api, options,
                                                                                     schema, DATE_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DATE_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, date_dim_be)]}},
        {'id': 'offload_story_dates_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DATE_DIM,
         'narrative': 'Offload dimension with dates to defaults',
         'options': {'owner_table': schema + '.' + DATE_DIM,
                     'data_sample_pct': 0,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_date_assertion(test, frontend_api, backend_api, data_db, date_dim_be),
              lambda test: True)]},
        {'id': 'offload_story_dates_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DATE_DIM,
         'narrative': 'Offload dimension with dates forced to canonical DATE',
         'options': {'owner_table': schema + '.' + DATE_DIM,
                     'data_sample_pct': 0,
                     'date_columns_csv': 'dt,ts0,ts6',
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_date_assertion(test, frontend_api, backend_api, data_db, date_dim_be,
                                                  forced_to_date=True),
              lambda test: True)],
         'prereq': lambda: backend_api.canonical_date_supported()},
        {'id': 'offload_story_dates_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DATE_DIM,
         'narrative': 'Offload dimension with dates forced to canonical TIMESTAMP_TZ',
         'options': {'owner_table': schema + '.' + DATE_DIM,
                     'data_sample_pct': 0,
                     'timestamp_tz_columns_csv': 'dt,ts0,ts6,ts0tz,ts6tz',
                     'reset_backend_table': True},
         'assertion_pairs': [(lambda test: offload_date_assertion(test, frontend_api, backend_api, data_db, date_dim_be,
                                                                  forced_to_tstz=True),
                              lambda test: True)]},
        {'id': 'offload_story_samp_dates_setup1',
         'type': STORY_TYPE_SETUP,
         'title': 'Create Dimension containing dates that need sampling',
         # TODO nj@2018-06-13 cannot test bad TZ values due to GOE-1102, uncomment bad_tstz/bad_tsltz during GOE-1102
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: story_samp_dates_setup_frontend_ddl(frontend_api, options,
                                                                                          schema, DATE_SDIM),

                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DATE_SDIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, date_sdim_be)]}},
        {'id': 'offload_story_samp_dates_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension containing bad dates',
         'narrative': 'Offload Dimension containing bad dates with stats, detection should be done from stats',
         'options': {'owner_table': schema + '.' + DATE_SDIM,
                     'allow_nanosecond_timestamp_columns': True,
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_samp_date_assertion(test, backend_api, frontend_api, data_db, date_sdim_be),
              lambda test: True)]},
        {'id': 'offload_story_samp_dates_setup2',
         'type': STORY_TYPE_SETUP,
         'title': 'Remove stats from %s' % DATE_SDIM,
         'setup': {STORY_SETUP_TYPE_FRONTEND: [dbms_stats_delete_string(schema, DATE_SDIM, frontend_api)]}},
        {'id': 'offload_story_samp_dates_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension containing bad dates',
         'narrative': 'Offload Dimension containing bad dates without stats, detection should be done using SQL',
         'options': {'owner_table': schema + '.' + DATE_SDIM,
                     'allow_nanosecond_timestamp_columns': True,
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_samp_date_assertion(test, backend_api, frontend_api, data_db,
                                                       date_sdim_be, from_stats=False),
              lambda test: True)]},
        # We've had cases where stats appear between prior test and next one, so drop stats again here to be sure.
        {'id': 'offload_story_samp_dates_setup3',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'offload_story_samp_dates_setup2'},
        {'id': 'offload_story_samp_dates_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Dimension containing dates',
         'narrative': 'Offload Dimension containing dates and influence canonical type using --date-columns',
         'options': {'owner_table': schema + '.' + DATE_SDIM,
                     'allow_nanosecond_timestamp_columns': True,
                     'data_sample_pct': DATA_SAMPLE_SIZE_AUTO,
                     'date_columns_csv': 'good_date,good_ts',
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_samp_date_assertion(test, backend_api, frontend_api, data_db, date_sdim_be,
                                                       from_stats=False, good_as_date=True),
              lambda test: True)]},
    ]
