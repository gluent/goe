import os
import re
from pyarrow import parquet

from gluent import verbose, vverbose, \
    num_location_files_enabled, \
    TOTAL_ROWS_OFFLOADED_LOG_TEXT
from gluentlib.offload.factory.backend_table_factory import backend_table_factory
from gluentlib.offload.column_metadata import SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE
from gluentlib.offload.frontend_api import QueryParameter
from gluentlib.offload.offload_constants import DBTYPE_BIGQUERY, PART_COL_GRANULARITY_DAY, PART_COL_GRANULARITY_MONTH
from gluentlib.offload.offload_functions import convert_backend_identifier_case, data_db_name, prefix_column_with_alias
from gluentlib.offload.offload_metadata_functions import incremental_hv_list_from_csv, incremental_hv_csv_from_list, \
    flatten_lpa_individual_high_values, split_metadata_incremental_high_values, \
    HYBRID_VIEW_IPA_HWM_TOKEN_RDBMS_BEGIN, HYBRID_VIEW_IPA_HWM_TOKEN_RDBMS_END, \
    HYBRID_VIEW_IPA_HWM_TOKEN_REMOTE_BEGIN, HYBRID_VIEW_IPA_HWM_TOKEN_REMOTE_END, \
    OFFLOAD_TYPE_FULL, OFFLOAD_TYPE_INCREMENTAL
from gluentlib.offload.oracle.oracle_column import ORACLE_TYPE_TIMESTAMP
from gluentlib.offload.offload_transport import MISSING_ROWS_IMPORTED_WARNING
from gluentlib.orchestration import command_steps
from gluentlib.persistence.orchestration_metadata import INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE, \
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE, INCREMENTAL_PREDICATE_TYPE_LIST, \
    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE, INCREMENTAL_PREDICATE_TYPE_RANGE
from gluentlib.util.misc_functions import add_suffix_in_same_case, get_temp_path, remove_chars, \
    trunc_with_hash
from testlib.test_framework import test_functions
from testlib.test_framework.oracle.oracle_frontend_testing_api import lob_to_hash
from testlib.test_framework.test_functions import log

from test_sets.stories.story_globals import OFFLOAD_PATTERN_100_0, OFFLOAD_PATTERN_100_10, OFFLOAD_PATTERN_90_10


# helper functions
def get_backend_testing_api(options, list_only=False, no_caching=False):
    """ Get a BackendTestingApi object.
        If options is unset or list_only is True then we're getting a story list
        for purposes other than actually testing things, therefore return None and
        don't create any backend connections.
    """
    if options and not list_only:
        return test_functions.get_backend_testing_api(options, no_caching=no_caching)
    else:
        return None


def get_date_offload_granularity(backend_api, preferred_value=None):
    if not backend_api:
        return preferred_value or PART_COL_GRANULARITY_MONTH
    if not backend_api.partition_by_column_supported():
        return None
    if preferred_value and preferred_value in backend_api.supported_date_based_partition_granularities():
        return preferred_value
    return backend_api.default_date_based_partition_granularity()


def ora_col(c, table_name, column_name, data_type=None, data_length=None, data_precision=None,
            data_scale=None, char_length=None, column_name_is_wildcard=False):
    log('ora_col details for %s.%s: data_type=%s, data_length=%s, data_precision=%s, data_scale=%s, char_length=%s'
        % (table_name, column_name, data_type, data_length, data_precision, data_scale, char_length), verbose)
    type_pred, length_pred = '', ''
    precision_pred, scale_pred = '', ''
    owner, table = table_name.split('.')
    params = {'o': owner, 't': table, 'c': column_name}
    if data_type:
        params['dt'] = data_type
        type_pred = ' AND data_type LIKE :dt'
    if data_length:
        params['dl'] = data_length
        length_pred = ' AND data_length = :dl'
    if char_length:
        params['cl'] = char_length
        length_pred += ' AND char_length = :cl'
    if data_precision:
        params['dp'] = data_precision
        precision_pred = ' AND data_precision = :dp'
    if data_scale is not None:
        params['ds'] = data_scale
        scale_pred = ' AND data_scale = :ds'
    column_name_op = 'LIKE' if column_name_is_wildcard else '='
    q = """SELECT column_name, data_type, data_length, data_precision, data_scale
FROM dba_tab_columns
WHERE owner = UPPER(:o) AND table_name = UPPER(:t) AND column_name %s UPPER(:c)%s%s%s%s""" \
        % (column_name_op, type_pred, length_pred, precision_pred, scale_pred)
    log('SQL: %s' % q, detail=vverbose)
    log('Params: %s' % str(params), detail=vverbose)
    return c.execute(q, params).fetchall()


def desc_columns(test_cursor, table_name, data_type_like=None, data_precision=None):
    log('desc_columns for %s' % table_name, verbose)
    owner, table = table_name.split('.')
    params = {'o': owner, 't': table}
    q = """SELECT column_name, data_type, data_length, data_scale
FROM dba_tab_columns
WHERE owner = UPPER(:o) AND table_name = UPPER(:t)"""
    if data_type_like:
        q += ' AND data_type LIKE :dt'
        params['dt'] = data_type_like.upper()
    if data_precision is not None:
        q += ' AND data_precision = :dp'
        params['dp'] = data_precision
    return test_cursor.execute(q, params).fetchall()


def minus_column_spec_count(test, desc, owner_a, table_name_a, owner_b, table_name_b, frontend_api=None):
    """ Check that all columns are of the same spec when comparing source table to hybrid view.
    """
    log('minus_column_spec_count: %s.%s minus %s.%s'
        % (owner_a.upper(), table_name_a.upper(), owner_b.upper(), table_name_b.upper()), verbose)
    use_frontend_api = None
    use_cursor = None
    if frontend_api:
        use_frontend_api = frontend_api
    else:
        use_cursor = test.cursor
    return test_functions.minus_column_spec_count(owner_a, table_name_a, owner_b, table_name_b,
                                                  desc, cursor=use_cursor, frontend_api=use_frontend_api,
                                                  log_diffs_if_non_zero=True)


def clob_safe_columns(test_cursor, owner_table):
    columns = desc_columns(test_cursor, owner_table)
    lob_sensitive = bool(col for col in columns if 'LOB' in col[1])
    if lob_sensitive:
        return ','.join(lob_to_hash(col[1], col[0]) if 'LOB' in col[1] else col[0] for col in columns)


def table_minus_row_count(test, desc, table_name_a, table_name_b, column=None, parallel=0, where_clause=None):
    column = column or clob_safe_columns(test.cursor, table_name_a) or '*'
    where_clause = where_clause or ''
    log('table_minus_row_count: %s vs %s' % (table_name_a, table_name_b), verbose)

    q1 = 'SELECT %s FROM %s %s' % (column, table_name_a.upper(), where_clause)
    q2 = 'SELECT %s FROM %s %s' % (column, table_name_b.upper(), where_clause)
    q = 'SELECT COUNT(*) FROM (%s MINUS %s)' % (q1, q2)
    log('table_minus_row_count qry: %s' % q, detail=vverbose)
    return_count = test.cursor.execute(q).fetchone()[0]
    return return_count


def table_row_count(c, table_name, where_clause=None):
    q = 'SELECT COUNT(*) FROM %s %s' % (table_name, where_clause or '')
    log('table_row_count: %s' % q, detail=verbose)
    count = c.execute(q).fetchone()[0]
    log('count: %s' % count, detail=verbose)
    return count


def get_offload_row_count_from_log(test_name):
    """ Search test log forwards from the "test_name" we are processing and find the offload row count
    """
    log('get_offload_row_count_from_log(%s)' % test_name, detail=verbose)
    matched_line = test_functions.get_line_from_log(TOTAL_ROWS_OFFLOADED_LOG_TEXT, search_from_text=test_name)
    log('matched_line: %s' % matched_line)
    rows = int(matched_line.split()[-1]) if matched_line else None
    return rows


def rdbms_count_minus_backend_count(rdbms_schema, rdbms_table, backend_db, backend_table,
                                    test_cursor, backend_api):
    log('rdbms_count_minus_backend_count(%s.%s - %s.%s)' % (rdbms_schema, rdbms_table, backend_db, backend_table),
        detail=verbose)
    return (table_row_count(test_cursor, rdbms_schema + '.' + rdbms_table)
            - backend_table_count(backend_api, backend_db, backend_table))


def string_in_ora_view_text(frontend_api, owner, view, search_string):
    log('string_in_ora_view_text: %s.%s %s' % (owner, view, search_string), detail=verbose)
    view_text = frontend_api.get_view_ddl(owner, view)
    return bool(search_string in view_text)


def strings_in_ora_view_text(frontend_api, owner, view, search_string_list):
    """ Handy function if we are going to search multiple times in the view text, avoids
        multiple look-ups on the DDL
    """
    log('strings_in_ora_view_text: %s.%s %s' % (owner, view, str(search_string_list)), detail=verbose)
    view_text = frontend_api.get_view_ddl(owner, view)
    for search_string in search_string_list:
        if search_string not in view_text:
            log('View does not contain string: %s' % search_string, detail=verbose)
            log('View text:\n%s' % view_text, detail=vverbose)
            return False
    return True


def ora_rewrite_equivalence(frontend_api, hybrid_schema, hybrid_view):
    # return tuple of (source_stmt, destination_stmt)
    log('ora_rewrite_equivalence: %s.%s' % (hybrid_schema, hybrid_view), verbose)
    q = """SELECT source_stmt, destination_stmt
           FROM dba_rewrite_equivalences
           WHERE owner = :o AND name = :t"""
    row = frontend_api.execute_query_fetch_one(q, query_params=[QueryParameter('o', hybrid_schema.upper()),
                                                                QueryParameter('t', hybrid_view.upper())])
    return (str(row[0]), str(row[1])) if row else row


def get_ext_table_access_parameters(frontend_api, hybrid_schema, ext_table_name):
    log('get_ext_table_access_parameters: %s.%s' % (hybrid_schema, ext_table_name), detail=verbose)
    q = 'SELECT access_parameters FROM dba_external_tables WHERE owner=:owner AND table_name=:table_name'
    row = frontend_api.execute_query_fetch_one(q, query_params=[QueryParameter('owner', hybrid_schema.upper()),
                                                                QueryParameter('table_name', ext_table_name.upper())])
    if not row or not row[0]:
        log('No access_parameters found', detail=verbose)
        return None
    params = row[0]
    return params


def check_ext_table_backend_types(frontend_api, hybrid_schema, ext_table_name, col_name_type_tuples):
    """ Validates external table access_parameters against col_name_type_tuples, checking that columns and
        types are as expected.
        It's a bit hacky, chopping up string etc, but is fine for test validation
    """
    log('check_ext_table_backend_types: %s.%s' % (hybrid_schema, ext_table_name), detail=verbose)
    assert hybrid_schema and hybrid_schema and col_name_type_tuples
    assert isinstance(col_name_type_tuples, list)
    access_parameter_string = get_ext_table_access_parameters(frontend_api, hybrid_schema, ext_table_name)
    lines = [remove_chars(_, ',"').strip() for _ in access_parameter_string.split('\n')]
    # we only want the lines between parentheses
    backend_columns = [_.split() for _ in lines[lines.index('(')+1:lines.index(')')]]
    backend_columns = [(_[0].upper(), _[1].upper()) for _ in backend_columns]
    for kv in col_name_type_tuples:
        log('Checking for column/type: %s' % str(kv), detail=verbose)
        if kv not in backend_columns:
            return False
    return True


def select_one_value_from_frontend_table_column(frontend_api, schema, table_name, column_name):
    """ Run a select and return the column value, column_name can be an expression such as SUM(col1)
        This is just a helper function for us in story assertions
    """
    log('select_one_value_from_frontend_table_column: %s, %s, %s' % (schema, table_name, column_name), detail=verbose)
    assert schema and table_name and column_name
    q = 'SELECT %s FROM %s.%s' % (column_name, schema, table_name)
    row = frontend_api.execute_query_fetch_one(q)
    if not row:
        return None
    return row[0]


def synthetic_part_col_name(granularity, source_col_name, synthetic_partition_digits=None, partition_function=None):
    """ Mock up a synthetic partition column name.
        Use synthetic_partition_digits for numeric partitioning in order to have the granularity padded.
        Assumes single partition column for UDFs.
    """
    if synthetic_partition_digits:
        granularity = ('{:0%sd}' % synthetic_partition_digits).format(int(granularity))
    if partition_function:
        granularity = 'U0'
    return (SYNTHETIC_PARTITION_COLUMN_NAME_TEMPLATE % (granularity, source_col_name)).upper()


def enclosed_backend_column_name(backend_api, data_db, table_name, column_name):
    log('enclosed_backend_column_name: %s.%s.%s' % (data_db, table_name, column_name), verbose)
    return backend_api.enclose_identifier(backend_api.get_column(data_db, table_name, column_name).name)


def backend_column_exists(backend_api, owner, table_name, search_column, search_type=None):
    log('backend_column_exists: %s.%s.%s %s' % (owner, table_name, search_column, search_type), verbose)
    if search_type:
        def search_fn(col):
            return bool(col.name.upper() == search_column.upper() and
                        search_type.upper() in col.format_data_type().upper())
    else:
        def search_fn(col):
            return bool(col.name.upper() == search_column.upper())
    matches = len([col for col in backend_api.get_columns(owner, table_name) if search_fn(col)])
    return bool(matches == 1)


def backend_table_exists(backend_api, db_name, table_name) -> bool:
    log('backend_table_exists: %s.%s' % (db_name, table_name), verbose)
    return backend_api.table_exists(db_name, table_name)


def backend_view_exists(backend_api, db_name, view_name) -> bool:
    log('backend_view_exists: %s.%s' % (db_name, view_name), verbose)
    return backend_api.view_exists(db_name, view_name)


def backend_table_count(backend_api, db, table_name, filter_clause=None):
    log('backend_table_count: %s.%s' % (db, table_name), detail=verbose)
    count = backend_api.get_table_row_count(db, table_name, filter_clause=filter_clause)
    log('count: %s' % count, detail=verbose)
    return count


def frontend_column_exists(frontend_api, owner, table_name, search_column, column_list=None,
                           search_type=None, data_length=None, data_precision=None, data_scale=None,
                           column_name_is_prefix=False, char_length=None) -> bool:
    log('frontend_column_exists: %s.%s.%s %s' % (owner, table_name, search_column, search_type), verbose)
    def name_test(c) -> bool:
        if column_name_is_prefix:
            status = bool(c.name.upper().startswith(search_column.upper()))
        else:
            status = bool(c.name.upper() == search_column.upper())
        if not status:
            log(f'name_test({search_column.upper()} != {c.name.upper()})', detail=vverbose)
        return status

    def data_type_test(c) -> bool:
        if search_type is None:
            return True
        if search_type == ORACLE_TYPE_TIMESTAMP:
            status = bool(search_type.upper() in c.data_type.upper())
        else:
            status = bool(c.data_type.upper() == search_type.upper())
        if not status:
            log(f'data_type_test({search_type.upper()} != {c.data_type.upper()})', detail=vverbose)
        return status

    def data_length_test(c) -> bool:
        status = bool(data_length is None or data_length == c.data_length)
        if not status:
            log(f'data_length_test({data_length} != {c.data_length})', detail=verbose)
        return status

    def data_precision_test(c) -> bool:
        status = bool(data_precision is None or data_precision == c.data_precision)
        if not status:
            log(f'data_precision_test({data_precision} != {c.data_precision})', detail=verbose)
        return status

    def data_scale_test(c) -> bool:
        status = bool(data_scale is None or data_scale == c.data_scale)
        if not status:
            log(f'data_scale_test({data_scale} != {c.data_scale})', detail=verbose)
        return status

    def char_length_test(c) -> bool:
        status = bool(char_length is None or char_length == c.char_length)
        if not status:
            log(f'char_length_test({char_length} != {c.char_length})', detail=verbose)
        return status

    def search_fn(col) -> bool:
        return bool(name_test(col)
                    and data_type_test(col)
                    and data_length_test(col)
                    and data_precision_test(col)
                    and data_scale_test(col)
                    and char_length_test(col))

    column_list = column_list or frontend_api.get_columns(owner, table_name.upper())
    matches = len([col for col in column_list if search_fn(col)])
    return bool(matches == 1)


def frontend_table_count(frontend_api, db, table_name, filter_clause=None):
    log('frontend_table_count: %s.%s' % (db.upper(), table_name.upper()), detail=verbose)
    count = frontend_api.get_table_row_count(db.upper(), table_name.upper(), filter_clause=filter_clause)
    log('count: %s' % count, detail=verbose)
    return count


def get_max_bucket_id(backend_api, db_name, table_name):
    log('get_max_bucket_id: %s.%s' % (db_name, table_name), verbose)
    if backend_api.synthetic_bucketing_supported():
        max_val = backend_api.get_max_column_values(db_name, table_name, ['offload_bucket_id'])
        max_val = max_val[0] if max_val else max_val
        return max_val
    else:
        return None


def get_distinct_from_column(backend_api, db_name, table_name, column_name):
    log('get_distinct_from_column: %s, %s, %s' % (db_name, table_name, column_name), detail=verbose)
    dist_vals = backend_api.get_distinct_column_values(db_name, table_name, [column_name])
    log('get_distinct_from_column: %s' % dist_vals, detail=verbose)
    return dist_vals


def get_max_column_length(backend_api, db_name, table_name, column_name) -> int:
    log('get_max_column_length: %s, %s, %s' % (db_name, table_name, column_name), detail=verbose)
    max_len = backend_api.get_max_column_length(db_name, table_name, column_name)
    log('get_max_column_length: %s' % max_len, detail=verbose)
    return max_len


def run_test_query(frontend_api, q, fetchone=False):
    log('run_test_query: %s' % q, verbose)
    if fetchone:
        return frontend_api.execute_query_fetch_one(q)
    else:
        return frontend_api.execute_query_fetch_all(q)


def get_metadata_item(hybrid_schema, hybrid_name, repo_client, metadata_attribute):
    log('get_metadata_item: %s' % metadata_attribute, detail=verbose)
    metadata = repo_client.get_offload_metadata(hybrid_schema, hybrid_name)
    if not metadata:
        log('No metadata for %s.%s' % (hybrid_schema, hybrid_name), verbose)
        return None
    return getattr(metadata, metadata_attribute)


def check_metadata(hybrid_schema, hybrid_name, repo_client, metadata_override=None, hybrid_owner=None, hybrid_view=None,
                   hadoop_owner=None, hadoop_table=None, offload_type=None, incremental_key=None,
                   incremental_high_value=None, offload_bucket_column=None, offload_bucket_method=None,
                   offload_bucket_count=None, offload_sort_columns=None, incremental_range=None,
                   incremental_predicate_type=None, incremental_predicate_value=None,
                   offload_partition_functions=None, iu_key_columns=None, iu_extraction_method=None,
                   iu_extraction_scn=None, iu_extraction_time=None, changelog_table=None, changelog_trigger=None,
                   changelog_sequence=None, updatable_view=None, updatable_trigger=None,
                   check_fn=None):
    """ Run a series of metadata checks using specific values or a custom check_fn() and return True or False """
    def check_item(metadata, check_value, metadata_name, case_sensitive=False):
        if check_value:
            check_value = str(check_value)
            metadata_value = getattr(metadata, metadata_name)
            if check_value == 'NULL' and metadata_value is None:
                log('%s is None' % metadata_name, verbose)
            elif not case_sensitive and str(metadata_value).upper() != check_value.upper():
                log('%s (%s) != %s' % (metadata_name, str(metadata_value).upper(), check_value.upper()))
                return False
            elif case_sensitive and str(metadata_value) != check_value:
                log('%s (%s) != %s' % (metadata_name, metadata_value, check_value))
                return False
            else:
                log('%s == %s' % (metadata_name, check_value), verbose)
        return True

    metadata = metadata_override or repo_client.get_offload_metadata(hybrid_schema, hybrid_name)
    if not metadata:
        log('No metadata for %s.%s' % (hybrid_schema, hybrid_name))
        return False
    if not check_item(metadata, hybrid_owner, 'hybrid_owner'):
        return False
    if not check_item(metadata, hybrid_view, 'hybrid_view'):
        return False
    if not check_item(metadata, hadoop_owner, 'backend_owner'):
        return False
    if not check_item(metadata, hadoop_table, 'backend_table'):
        return False
    if not check_item(metadata, offload_type, 'offload_type'):
        return False
    if not check_item(metadata, incremental_key, 'incremental_key'):
        return False
    if not check_item(metadata, incremental_high_value, 'incremental_high_value'):
        return False
    if not check_item(metadata, offload_bucket_column, 'offload_bucket_column'):
        return False
    if not check_item(metadata, offload_bucket_method, 'offload_bucket_method'):
        return False
    if not check_item(metadata, offload_bucket_count, 'offload_bucket_count'):
        return False
    if not check_item(metadata, offload_sort_columns, 'offload_sort_columns'):
        return False
    if not check_item(metadata, offload_partition_functions, 'offload_partition_functions'):
        return False
    if not check_item(metadata, incremental_range, 'incremental_range'):
        return False
    if not check_item(metadata, incremental_predicate_type, 'incremental_predicate_type'):
        return False
    if not check_item(metadata, incremental_predicate_value, 'incremental_predicate_value'):
        return False
    if not check_item(metadata, iu_key_columns, 'iu_key_columns'):
        return False
    if not check_item(metadata, iu_extraction_method, 'iu_extraction_method'):
        return False
    if not check_item(metadata, iu_extraction_scn, 'iu_extraction_scn'):
        return False
    if not check_item(metadata, iu_extraction_time, 'iu_extraction_time'):
        return False
    if not check_item(metadata, changelog_table, 'changelog_table'):
        return False
    if not check_item(metadata, changelog_trigger, 'changelog_trigger'):
        return False
    if not check_item(metadata, changelog_sequence, 'changelog_sequence'):
        return False
    if not check_item(metadata, updatable_view, 'updatable_view'):
        return False
    if not check_item(metadata, updatable_trigger, 'updatable_trigger'):
        return False
    if not getattr(metadata, 'offload_version'):
        log('OFFLOAD_VERSION missing from metadata')
        return False
    if check_fn:
        log('Checking metadata by fn', verbose)
        if not check_fn(metadata):
            log('check_fn() != True')
            return False
    return True


def messages_step_executions(messages, step_text) -> int:
    """Return the number of times step "step_text" was executed."""
    assert step_text
    log('messages_step_executions: %s' % step_text, verbose)
    if messages and step_text in messages.steps:
        return messages.steps[step_text]['count']
    return 0


def text_in_log(search_text, search_from_text='') -> bool:
    # Do not log search_text below because that is a sure fire way to break the check.
    log(f'text_in_log(">8 snip 8<", "{search_from_text}")', detail=verbose)
    return test_functions.text_in_log(search_text, search_from_text=search_from_text)


def text_in_events(messages, message_token) -> bool:
    log(f'text_in_events({message_token})', detail=verbose)
    return test_functions.text_in_events(messages, message_token)


def text_in_messages(messages, log_text) -> bool:
    log(f'text_in_messages({log_text})', detail=verbose)
    return test_functions.text_in_messages(messages, log_text)


def text_in_warnings(messages, log_text):
    log(f'text_in_warnings({log_text})', detail=verbose)
    return test_functions.text_in_warnings(messages, log_text)


def data_gov_error_reported(test) -> bool:
    """ Data Gov calls are optional but we can check for warnings to find failures
        If data gov is not enabled then this will find no errors and pass
    """
    if getattr(test, 'offload_messages', None):
        return text_in_messages(test.offload_messages,
                                'Error in "%s"' % command_steps.step_title(command_steps.STEP_REGISTER_DATA_GOV))
    else:
        return False


def hybrid_query_rewrites(hybrid_cursor, query, ext_table_name):
    """ Runs explain plan on a query and searches resulting plan for an external table name. """
    log('hybrid_query_rewrites qry: %s' % query, detail=verbose)
    explain_sql = 'EXPLAIN PLAN FOR %s' % query
    hybrid_cursor.execute(explain_sql)
    plan_sql = 'SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY)'
    rows = hybrid_cursor.execute(plan_sql).fetchall()
    plan_str = ('\n'.join(_[0] for _ in rows)).upper()
    if ext_table_name.upper() not in plan_str:
        log('Ext table %s not in plan:\n%s' % (ext_table_name, plan_str))
        return False
    return True


#
# functions that return assertion lists
#

def offload_rowsource_split_type_assertions(story_id, split_type=''):
    return (lambda test: text_in_log('Transport rowsource split type: %s' % split_type, '(%s)' % (story_id)),
            lambda test: bool(split_type))


def present_backend_only_assertion(test, hybrid_schema, data_db, table, options, backend_api, frontend_api, repo_client,
                                   date_col_type=None, backend_table=None, expect_hybrid_stats=True,
                                   num_loc_files=None, conv_view_exists=None):
    """ A single set of checks to run after a number of present story tests.
        Returns True or False.
    """
    if not options:
        return True

    backend_table = backend_table or table
    hybrid_view = table.upper()
    agg_name = add_suffix_in_same_case(hybrid_view, '_agg')
    agg_ext = add_suffix_in_same_case(hybrid_view, '_ext')
    cnt_agg_name = add_suffix_in_same_case(hybrid_view, '_cnt_agg')
    gen_agg_columns = frontend_api.get_columns(hybrid_schema, agg_name)
    cnt_agg_columns = frontend_api.get_columns(hybrid_schema, cnt_agg_name)
    # Don't expect min/max by default on string columns
    if frontend_column_exists(frontend_api, hybrid_schema, agg_name, 'min_cust_first_name',
                              column_list=gen_agg_columns):
        log('min_cust_first_name exists')
        return False
    # Do expect count by default on string columns
    if not frontend_column_exists(frontend_api, hybrid_schema, agg_name, 'count_cust_first_name',
                                  column_list=gen_agg_columns):
        log(f'Column {hybrid_schema}.{agg_name}.count_cust_first_name does not exist')
        return False
    # Only one column in cnt agg and named count_star
    if len(cnt_agg_columns) != 1:
        log('len(cnt_agg_columns) != 1: {}'.format(str(cnt_agg_columns)))
        return False
    if not frontend_column_exists(frontend_api, hybrid_schema, cnt_agg_name, 'count_star',
                                  column_list=cnt_agg_columns):
        log(f'Column {hybrid_schema}.{cnt_agg_name}.count_star does not exist')
        return False
    metadata = repo_client.get_offload_metadata(hybrid_schema, hybrid_view)
    if not check_metadata(hybrid_schema, hybrid_view, repo_client, metadata_override=metadata,
                          hybrid_owner=hybrid_schema, hybrid_view=table,
                          hadoop_owner=data_db, hadoop_table=backend_table, offload_bucket_column='NULL',
                          offload_bucket_method='NULL', offload_bucket_count='NULL',
                          check_fn=lambda mt: bool(mt.offload_version)):
        return False

    if date_col_type:
        if '(' in date_col_type:
            date_col_type, ts_scale = date_col_type.strip(')').split('(')
            ts_scale = int(ts_scale)
        else:
            ts_scale = None
        hybrid_view_columns = frontend_api.get_columns(hybrid_schema, hybrid_view)
        if not frontend_column_exists(frontend_api, hybrid_schema, hybrid_view, 'cust_eff_from',
                                      search_type=date_col_type, data_scale=ts_scale, column_list=hybrid_view_columns):
            log(f'Column {hybrid_schema}.{hybrid_view}.cust_eff_from({date_col_type}) does not exist')
            return False
        if not frontend_column_exists(frontend_api, hybrid_schema, hybrid_view, 'cust_eff_to',
                                      search_type=date_col_type, data_scale=ts_scale, column_list=hybrid_view_columns):
            log(f'Column {hybrid_schema}.{hybrid_view}.cust_eff_to({date_col_type}) does not exist')
            return False
        if not frontend_column_exists(frontend_api, hybrid_schema, agg_name, 'cust_eff_from',
                                      search_type=date_col_type, data_scale=ts_scale, column_list=gen_agg_columns):
            log(f'Column {hybrid_schema}.{agg_name}.cust_eff_from({date_col_type}) does not exist')
            return False
        if not frontend_column_exists(frontend_api, hybrid_schema, agg_name, 'cust_eff_to',
                                      search_type=date_col_type, data_scale=ts_scale, column_list=gen_agg_columns):
            log(f'Column {hybrid_schema}.{agg_name}.cust_eff_to({date_col_type}) does not exist')
            return False
        if not frontend_column_exists(frontend_api, hybrid_schema, agg_name, 'max_cust_eff_to',
                                      search_type=date_col_type, data_scale=ts_scale, column_list=gen_agg_columns):
            log(f'Column {hybrid_schema}.{agg_name}.max_cust_eff_to({date_col_type}) does not exist')
            return False
        if not frontend_column_exists(frontend_api, hybrid_schema, agg_name, 'min_cust_eff_to',
                                      search_type=date_col_type, data_scale=ts_scale, column_list=gen_agg_columns):
            log(f'Column {hybrid_schema}.{agg_name}.min_cust_eff_to({date_col_type}) does not exist')
            return False

    ext_table_name = metadata.external_table
    # Check stats are present (or not) depending on expect_hybrid_stats
    if bool(frontend_api.table_row_count_from_stats(hybrid_schema, ext_table_name) is not None) != expect_hybrid_stats:
        log(f'table_row_count_from_stats({ext_table_name}) != {expect_hybrid_stats}')
        return False
    if bool(frontend_api.table_row_count_from_stats(hybrid_schema, agg_ext) is not None) != expect_hybrid_stats:
        log(f'table_row_count_from_stats({ext_table_name}_agg_ext) != {expect_hybrid_stats}')
        return False

    if conv_view_exists is not None:
        conv_view_name = convert_backend_identifier_case(options, table + '_conv')
        if backend_api.view_exists(data_db, conv_view_name) != conv_view_exists:
            log('view_exists(%s) != %s' % (conv_view_name, str(conv_view_exists)))
            return False

    if text_in_warnings(test.offload_messages, 'Exception: '):
        # Some steps are optional and therefore a present can continue even if we encounter an exception.
        # This check is to catch those and fail the test even though the present itself completed.
        log('text_in_warnings() found an Exception')
        return False

    return True


def date_gl_part_column_name(backend_api, source_col_name, granularity_override=None):
    """ Return a synthetic partition column name based on backend defaults.
        I feel this is a bit short term because we have some data types that result
        in native partitioning rather than synthetic columns but at least the
        function is a single place to hold this short-sighted logic.
    """
    if backend_api and backend_api.backend_type() == DBTYPE_BIGQUERY:
        return synthetic_part_col_name(granularity_override or PART_COL_GRANULARITY_DAY, source_col_name)
    else:
        return synthetic_part_col_name(granularity_override or PART_COL_GRANULARITY_MONTH, source_col_name)


def avro_file_is_compressed(dfs_client, staged_file):
    """ Return True or False as appropriate
        Beware, this function:
          1) pulls the entire file into memory so only use on small test tables
          2) Should really decode Avro and find a way to parse metadata in the file -
             I cheated and just look for the codec name as a string inside another string
    """
    staged_content = dfs_client.read(staged_file)
    m = re.search(rb'avro.codec.*(snappy|deflate)', staged_content)
    if m:
        log('Detected load table codec: %s' % m.group(1).decode(), verbose)
    return bool(m)


def parquet_file_is_compressed(dfs_client, staged_file):
    """ Return True or False as appropriate
        Beware, this function:
          1) Pulls the entire file across to local storage so only use on small test tables
    """
    tmp_file = get_temp_path(suffix='.parquet')
    dfs_client.copy_to_local(staged_file, tmp_file)
    try:
        parquet_file = parquet.ParquetFile(tmp_file)
        metadata = parquet_file.metadata.to_dict()
        compression = [_['compression'].upper() for _ in metadata['row_groups'].pop()['columns']]
        return bool('SNAPPY' in compression)
    finally:
        os.remove(tmp_file)


def load_table_is_compressed(db_name, table_name, options, dfs_client, messages):
    """ Return True or False as appropriate
        Beware, this function:
          1) picks the first file in the HDFS directory so don't mix and match codecs
          2) pulls the entire file into memory so only use on small test tables
          3) Should really decode Avro and find a way to parse metadata in the file -
             I cheated and just look for the codec name as a string inside another string
    """
    log('load_table_is_compressed(%s, %s)' % (db_name, table_name), detail=verbose)
    backend_table = backend_table_factory(db_name, table_name, options.target, options, messages)
    path = backend_table.get_staging_table_location()
    files = [_ for _ in dfs_client.list_dir(path) if _ and _[0] != '.']
    log('files: %s' % str(files), detail=verbose)

    # Filter files for format extension
    new_files = [_ for _ in files if _.endswith(options.offload_staging_format.lower())]
    if not new_files:
        # No files matching by extension so instead throw out known non-transport files. This is because Spark
        # on CDH does not append a file extension, so we assume Avro because that's all we support
        new_files = [_ for _ in files if not _.endswith('_SUCCESS')]
    assert new_files

    staged_file = new_files.pop()
    if staged_file.endswith('.parquet') or staged_file.endswith('.parq'):
        return parquet_file_is_compressed(dfs_client, staged_file)
    else:
        return avro_file_is_compressed(dfs_client, staged_file)


def get_query_engine():
    return os.getenv('QUERY_ENGINE').lower()


def table_has_stats(backend_api, data_db_schema, table):
    """ If the table has stats return True else return False
    """
    assert data_db_schema
    assert table
    if backend_api.table_stats_get_supported():
        # Only Hadoop backends will return part_stats and col_stats
        # All backends should return tab_stats: [num_rows, num_bytes, avg_row_len]
        tab_stats, part_stats, col_stats = backend_api.get_table_and_partition_stats(data_db_schema, table, as_dict=False)
        log('table_has_stats: %s.%s: %s' % (data_db_schema, table, tab_stats), detail=verbose)
        return bool(tab_stats[0] > -1)
    else:
        raise NotImplementedError('table_stats_get_supported not supported for %s' % backend_api._backend_type)


def partition_has_stats(backend_api, data_db_schema, table, partition_tuples, colstats=False):
    """ If the partition has stats (or column stats if colstats=True) return True else return False
    """
    assert data_db_schema
    assert table
    log('partition_has_stats: %s.%s: %s' % (data_db_schema, table, str(partition_tuples)), detail=verbose)
    if colstats:
        log('colstats=%s' % colstats, detail=verbose)
    return backend_api.partition_has_stats(data_db_schema, table, partition_tuples, colstats=colstats)


def offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                          schema, hybrid_schema, data_db_schema, table_name,
                          bucket_check=None, check_aggs=False, bucket_col=None, bucket_method=None,
                          check_hybrid_stats=True, hadoop_db=None, hadoop_table=None, num_loc_files=None,
                          run_minus_row_count=True, story_id='', split_type=None, partition_functions=None):
    if not options:
        return True

    data_db = hadoop_db or data_db_schema
    hadoop_table = hadoop_table or table_name
    data_db, hadoop_table = convert_backend_identifier_case(options, data_db, hadoop_table)
    if not backend_api.synthetic_bucketing_supported():
        bucket_check = None
        bucket_col = None
        bucket_method = None
    num_loc_files = num_loc_files if num_location_files_enabled(options.target) else bucket_check

    metadata = repo_client.get_offload_metadata(hybrid_schema, table_name)

    if not check_metadata(hybrid_schema, table_name, repo_client, metadata_override=metadata,
                          hybrid_owner=hybrid_schema, hybrid_view=table_name,
                          hadoop_owner=data_db, hadoop_table=hadoop_table,
                          offload_bucket_column=bucket_col, offload_bucket_method=bucket_method,
                          offload_bucket_count=bucket_check, offload_partition_functions=partition_functions,
                          check_fn=lambda mt: bool(mt.offload_version)):
        log('check_metadata(%s.%s) == False' % (hybrid_schema, table_name))
        return False
    if not backend_table_exists(backend_api, data_db, hadoop_table):
        log('backend_table_exists() == False')
        return False

    if bucket_check:
        if get_max_bucket_id(backend_api, data_db, hadoop_table) != bucket_check-1:
            log('get_max_bucket_id() != %s (bucket_check-1)' % (bucket_check-1))
            return False

    if getattr(test, 'offload_messages', None):
        # Some test types, such as setup tests, do not have a messages object.
        if data_gov_error_reported(test):
            log('data_gov_error_reported()')
            return False

        if text_in_messages(test.offload_messages, MISSING_ROWS_IMPORTED_WARNING):
            log('text_in_messages(%s)' % MISSING_ROWS_IMPORTED_WARNING)
            return False

    if split_type:
        for i, (asrt_left, asrt_right) in enumerate(offload_rowsource_split_type_assertions(story_id, split_type)):
            left_result = asrt_left()
            right_result = asrt_right()
            if left_result != right_result:
                log('offload_rowsource_split_type_assertions assertion %s failed: %s != %s'
                    % (i, left_result, right_result))
                return False

    if frontend_api.hybrid_schema_supported():
        # Checks only required when we have a hybrid schema
        if minus_column_spec_count(test, test.name, schema, table_name, hybrid_schema, table_name,
                                   frontend_api=frontend_api) != 0:
            log('minus_column_spec_count() != 0')
            return False
        if not frontend_api.select_grant_exists(hybrid_schema, table_name, schema, grantable=True):
            log('select_grant_exists() == False')
            return False
        if run_minus_row_count:
            # NOSSH tests fail because metad cannot cope with most tables having SSH user gluent but one as oracle
            if frontend_api.table_minus_row_count(schema, table_name, hybrid_schema) != 0:
                log('table_minus_row_count() != 0')
                return False
        if check_hybrid_stats:
            ext_stats = frontend_api.table_row_count_from_stats(hybrid_schema, metadata.external_table)
            vw_stats = frontend_api.table_row_count_from_stats(schema, table_name)
            if ext_stats != vw_stats:
                log('ext_stats %s != vw_stats %s' % (ext_stats, vw_stats))
                return False

    return True


def get_range_hv_clause_from_metadata(metadata):
    if not metadata:
        return None
    incremental_key = metadata.incremental_key
    if not incremental_key:
        return None
    if ',' in incremental_key:
        # multi-column partition boundary conditions are beyond the complexity supported by test assertions
        return None
    if metadata.incremental_predicate_type in (INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                               INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE):
        range_op = '<='
    else:
        range_op = '<'
    hv_where_clause = "%s %s %s" % (incremental_key, range_op, metadata.incremental_high_value)
    return hv_where_clause


def offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                            schema, hybrid_schema, data_db_schema, table_name, hwm_literal,
                            check_aggs=True, hadoop_schema=None, hadoop_table=None, check_rowcount=True,
                            offload_pattern=OFFLOAD_PATTERN_90_10, incremental_key='TIME_ID', incremental_range=None,
                            bucket_check=None, num_loc_files=None, story_id='', split_type=None,
                            check_hwm_in_hybrid_view=True, ipa_predicate_type='RANGE', incremental_key_type=None,
                            incremental_predicate_value=None, max_hybrid_length_override=None,
                            partition_functions=None, synthetic_partition_column_name=None,
                            check_backend_rowcount=False) -> list:
    if not options:
        return []

    # TODO We need to convert this to a True/False assertion function like the dimension equivalent

    if hadoop_schema:
        data_db = data_db_name(hadoop_schema, options)
    else:
        data_db = data_db_schema
    backend_name = hadoop_table or table_name
    data_db, backend_name = convert_backend_identifier_case(options, data_db, backend_name)
    max_hybrid_length = max_hybrid_length_override or frontend_api.max_table_name_length()

    if not backend_api.synthetic_bucketing_supported():
        bucket_check = None

    if not incremental_key_type and incremental_key == 'TIME_ID':
        incremental_key_type = frontend_api.test_type_canonical_date()

    hwm_literal, meta_check_literal, sql_hwm_literal = frontend_api.sales_based_fact_hwm_literal(hwm_literal, incremental_key_type)

    hwm_in_hybrid_view = False
    asrt_part_key_in_cnt_agg = False
    if offload_pattern == OFFLOAD_PATTERN_90_10:
        offload_type = OFFLOAD_TYPE_INCREMENTAL
        def check_fn(mt):
            match = meta_check_literal in mt.incremental_high_value
            if not match:
                log('Checking %s in INCREMENTAL_HIGH_VALUE (%s): %s'
                    % (meta_check_literal, mt.incremental_high_value, match))
            return match
        hwm_in_hybrid_view = True
        asrt_part_key_in_cnt_agg = True
    elif offload_pattern == OFFLOAD_PATTERN_100_0:
        offload_type = OFFLOAD_TYPE_FULL
        incremental_key = None
        check_fn = lambda mt: bool(not mt.incremental_key and not mt.incremental_high_value)
    elif offload_pattern == OFFLOAD_PATTERN_100_10:
        offload_type = OFFLOAD_TYPE_FULL
        def check_fn(mt):
            match = meta_check_literal in mt.incremental_high_value
            if not match:
                log('Checking %s in INCREMENTAL_HIGH_VALUE (%s): %s'
                    % (meta_check_literal, mt.incremental_high_value, match))
            return match
        hwm_in_hybrid_view = True
        asrt_part_key_in_cnt_agg = True
    if not check_hwm_in_hybrid_view:
        # If we don't want to check HWM in hybrid view then also don't go looking in metadata
        check_fn = None
    rdbms_union_op = '>' if ipa_predicate_type in (INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                                   INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE) else '>='

    num_loc_files = num_loc_files if num_location_files_enabled(options.target) else bucket_check
    asrt = [
        (lambda test: backend_table_exists(backend_api, data_db, backend_name),
         lambda test: True),
        (lambda test: check_metadata(hybrid_schema, table_name, repo_client, hybrid_owner=hybrid_schema,
                                     hybrid_view=table_name, hadoop_owner=data_db, hadoop_table=backend_name,
                                     incremental_key=incremental_key, offload_type=offload_type,
                                     incremental_range=incremental_range, offload_bucket_count=bucket_check,
                                     incremental_predicate_value=incremental_predicate_value,
                                     offload_partition_functions=partition_functions, check_fn=check_fn),
         lambda test: True)]
    if bucket_check:
        asrt.append((lambda test: get_max_bucket_id(backend_api, data_db, backend_name),
                     lambda test: bucket_check-1))
    if synthetic_partition_column_name and backend_api.synthetic_partitioning_supported():
        asrt.append((lambda test: backend_column_exists(backend_api, data_db, backend_name,
                                                        synthetic_partition_column_name),
                     lambda test: True))

    asrt.append((lambda test: data_gov_error_reported(test),
                 lambda test: False))
    asrt.append((lambda test: bool(getattr(test, 'offload_messages', None)
                                   and text_in_messages(test.offload_messages, MISSING_ROWS_IMPORTED_WARNING)),
                 lambda test: False))
    if split_type:
        asrt.append(offload_rowsource_split_type_assertions(story_id, split_type))

    return asrt


def check_hv_predicate_in_hybrid_view(hybrid_schema, hybrid_view, options, repo_client, frontend_api,
                                      metadata=None, view_text=None):
    """ Look at the text of a hybrid view and check that inclusive/exclusive predicates are in the right place.
    """
    if not options or not frontend_api.hybrid_schema_supported():
        return True
    log('check_hv_predicate_in_hybrid_view: %s.%s' % (hybrid_schema, hybrid_view), detail=verbose)
    metadata = metadata or repo_client.get_offload_metadata(hybrid_schema, hybrid_view)
    view_text = view_text or frontend_api.get_view_ddl(hybrid_schema, hybrid_view) or ''

    if not get_hybrid_view_ipa_value_from_metadata(hybrid_schema, hybrid_view, repo_client, metadata) in view_text:
        log('IPA value not in hybrid view', detail=verbose)
        return False
    inc_key = metadata.incremental_key
    pred_type = metadata.incremental_predicate_type
    if pred_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        inclusive, exclusive = 'IN', 'NOT IN'
    elif pred_type in (INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                       INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE):
        inclusive, exclusive = '<=', '>'
    else:
        inclusive, exclusive = '<', '>='
    rdb_hv_pattern = r'%s.*"%s" %s .*%s.*UNION ALL' \
        % (re.escape(HYBRID_VIEW_IPA_HWM_TOKEN_RDBMS_BEGIN), inc_key, exclusive,
           re.escape(HYBRID_VIEW_IPA_HWM_TOKEN_RDBMS_END))
    log('Checking pattern: %s' % rdb_hv_pattern, detail=vverbose)
    if not re.search(rdb_hv_pattern, view_text, re.DOTALL | re.IGNORECASE):
        log('No match for RDBMS pattern in hybrid view: %s' % rdb_hv_pattern, detail=verbose)
        return False
    ext_hv_pattern = r'UNION ALL.*%s.*"%s" %s .*%s' \
        % (re.escape(HYBRID_VIEW_IPA_HWM_TOKEN_REMOTE_BEGIN), inc_key, inclusive,
           re.escape(HYBRID_VIEW_IPA_HWM_TOKEN_REMOTE_END))
    log('Checking pattern: %s' % ext_hv_pattern, detail=vverbose)
    if not re.search(ext_hv_pattern, view_text, re.DOTALL | re.IGNORECASE):
        log('No match for REMOTE pattern in hybrid view: %s' % ext_hv_pattern, detail=verbose)
        return False
    return True


def offload_lpa_fact_assertion(test, schema, hybrid_schema, data_db, table_name, options, backend_api,
                               frontend_api, repo_client, hwm_literals, check_aggs=True, check_rowcount=True,
                               offload_pattern=OFFLOAD_PATTERN_90_10, incremental_key=None,
                               incremental_range='PARTITION', incremental_predicate_type=None,
                               incremental_predicate_value=None, check_hwm_metadata_with_sql=False,
                               backend_table_count_check=None, check_data=False, partition_functions=None,
                               synthetic_partition_column_name=None):

    def get_hv_where_clause(metadata):
        """ This function is required to get metadata during the assertion, not before
            Otherwise we are looking at stale information
        """
        if offload_pattern in (OFFLOAD_PATTERN_90_10, OFFLOAD_PATTERN_100_10) and metadata.incremental_high_value:
            lpa_hvs = split_metadata_incremental_high_values(metadata, frontend_api)
            sql_in_list = ','.join(flatten_lpa_individual_high_values(lpa_hvs))
            return "%s IN (%s)" % (metadata.incremental_key, sql_in_list)
        else:
            return None

    if not options:
        return True

    assert schema
    assert hybrid_schema
    assert data_db
    assert table_name
    assert hwm_literals is None or type(hwm_literals) in (list, tuple)
    log('offload_lpa_fact_assertions: %s' % offload_pattern, detail=vverbose)
    owner_table_h = hybrid_schema + '.' + table_name
    backend_table = convert_backend_identifier_case(options, table_name)

    if offload_pattern == OFFLOAD_PATTERN_90_10:
        offload_type = OFFLOAD_TYPE_INCREMENTAL
        assert hwm_literals, 'hwm_literals required for %s' % offload_pattern
        hwm_in_hybrid_view = True
    elif offload_pattern == OFFLOAD_PATTERN_100_0:
        offload_type = OFFLOAD_TYPE_FULL
        incremental_key = None
        incremental_predicate_type = None
        hwm_in_hybrid_view = False
    elif offload_pattern == OFFLOAD_PATTERN_100_10:
        assert hwm_literals, 'hwm_literals required for %s' % offload_pattern
        offload_type = OFFLOAD_TYPE_FULL
        hwm_in_hybrid_view = True

    log('offload_type: %s' % offload_type, detail=vverbose)
    log('hwm_in_hybrid_view: %s' % hwm_in_hybrid_view, detail=vverbose)

    # GOE-1503, temporarily removed test_cursor from the get_offload_metadata call to ensure an ADM connection in the function
    metadata = repo_client.get_offload_metadata(hybrid_schema, table_name)
    hybrid_view_text = frontend_api.get_view_ddl(hybrid_schema, table_name) or ''

    if frontend_api.hybrid_schema_supported():
        if bool('UNION ALL' in hybrid_view_text) != hwm_in_hybrid_view:
            log('Missing UNION ALL in HV')
            return False

        if minus_column_spec_count(test, test.name, schema, table_name, hybrid_schema, table_name,
                                   frontend_api=frontend_api) > 0:
            return False
    else:
        if check_aggs:
            log('Overriding check_aggs=False when hybrid_schema_supported=False', detail=verbose)
            check_aggs = False

    if hwm_in_hybrid_view:
        if not check_hv_predicate_in_hybrid_view(hybrid_schema, table_name, options, repo_client, frontend_api,
                                                 metadata=metadata, view_text=hybrid_view_text):
            return False

        def check_fn(mt):
            if all(_ in mt.incremental_high_value for _ in hwm_literals):
                return True
            else:
                log('Metadata check_fn False for: %s'
                    % str([_ for _ in hwm_literals if _ not in mt.incremental_high_value]))
                return False
    else:
        check_fn = lambda mt: bool(not mt.incremental_key and not mt.incremental_high_value)

    if not check_metadata(hybrid_schema, table_name, repo_client, hybrid_owner=hybrid_schema, hybrid_view=table_name,
                          hadoop_owner=data_db, hadoop_table=backend_table, incremental_key=incremental_key,
                          offload_type=offload_type, incremental_range=incremental_range,
                          incremental_predicate_type=incremental_predicate_type,
                          incremental_predicate_value=incremental_predicate_value,
                          offload_partition_functions=partition_functions, check_fn=check_fn):
        return False

    if backend_table_count_check is not None:
        if backend_table_count(backend_api, data_db, backend_table) != backend_table_count_check:
            log(f'Backend count != {backend_table_count_check}')
            return False

    if synthetic_partition_column_name and backend_api.synthetic_partitioning_supported():
        if not backend_column_exists(backend_api, data_db, backend_table, synthetic_partition_column_name):
            log(f'Backend column {synthetic_partition_column_name} does not exist')
            return False

    if check_aggs:
        max_hybrid_length = frontend_api.max_table_name_length()
        agg_name = '%s_agg' % table_name.upper()
        cnt_agg_name = '%s_cnt_agg' % table_name.upper()
        if max_hybrid_length:
            agg_name = trunc_with_hash(agg_name, options.hash_chars, max_hybrid_length).upper()
            cnt_agg_name = trunc_with_hash(cnt_agg_name, options.hash_chars, max_hybrid_length).upper()
        metadata = repo_client.get_offload_metadata(hybrid_schema, agg_name)
        gen_agg_view_text = frontend_api.get_view_ddl(hybrid_schema, agg_name) or ''
        if bool('UNION ALL' in gen_agg_view_text) != hwm_in_hybrid_view:
            return False
        if not check_metadata(hybrid_schema, agg_name, repo_client, metadata_override=metadata,
                              hybrid_owner=hybrid_schema, hybrid_view=agg_name,
                              hadoop_owner=data_db, hadoop_table=backend_table, incremental_key=incremental_key,
                              offload_type=offload_type, incremental_range=incremental_range,
                              incremental_predicate_type=incremental_predicate_type,
                              offload_partition_functions=partition_functions, check_fn=check_fn):
            return False
        if hwm_in_hybrid_view:
            if not check_hv_predicate_in_hybrid_view(hybrid_schema, agg_name, options, repo_client, frontend_api,
                                                     metadata=metadata, view_text=gen_agg_view_text):
                return False
        if check_hwm_metadata_with_sql and hwm_literals:
            if not check_char_in_metadata_hv_with_sql(hybrid_schema, agg_name, frontend_api, options, hwm_literals):
                return False

    return True


def get_hybrid_view_ipa_value_from_metadata(hybrid_schema, table_name, repo_client, metadata=None):
    log('get_hybrid_view_ipa_value_from_metadata: %s.%s' % (hybrid_schema, table_name), detail=verbose)
    metadata = metadata or repo_client.get_offload_metadata(hybrid_schema, table_name)
    if not metadata:
        log('No metadata for %s.%s' % (hybrid_schema, table_name), verbose)
        return None
    ipa_predicate_type = metadata.incremental_predicate_type or INCREMENTAL_PREDICATE_TYPE_RANGE
    if ipa_predicate_type in [INCREMENTAL_PREDICATE_TYPE_RANGE,
                              INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
                              INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                              INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE]:
        return metadata.incremental_high_value
    else:
        return incremental_hv_csv_from_list(incremental_hv_list_from_csv(metadata.incremental_high_value, ipa_predicate_type))


def check_char_in_metadata_hv_with_sql(hybrid_schema, table_name, frontend_api, options, ch_list):
    log('check_char_in_metadata_hv_with_sql: %s.%s, %s' % (hybrid_schema, table_name, ch_list), detail=verbose)
    q = "SELECT 1 FROM %s.offload_objects WHERE hybrid_owner = :schema AND hybrid_view = :view_name AND incremental_high_value LIKE '%%'||:ch||'%%'" \
        % options.ora_adm_user
    log('Check SQL: %s' % q, detail=verbose)
    if isinstance(ch_list, str):
        ch_list = [ch_list]
    for ch in ch_list:
        row = frontend_api.execute_query_fetch_one(q, query_params={'schema': hybrid_schema.upper(),
                                                                    'view_name': table_name.upper(),
                                                                    'ch': ch})
        if not row:
            log('Return False for binds: %s.%s: %s' % (hybrid_schema.upper(), table_name.upper(), ch), detail=verbose)
            return False
    return True


def present_custom_agg_assertion(schema, hybrid_schema, data_db_schema, table_name, agg_name, frontend_api, options,
                                 repo_client, offload_pattern=OFFLOAD_PATTERN_90_10):
    assert schema
    assert hybrid_schema
    assert data_db_schema
    assert table_name
    assert agg_name
    if not options:
        return True

    agg_owner_name = hybrid_schema + '.' + agg_name
    backend_table = convert_backend_identifier_case(options, table_name)
    hwm_in_hybrid_view = False
    offload_type = OFFLOAD_TYPE_FULL
    if offload_pattern == OFFLOAD_PATTERN_90_10:
        offload_type = OFFLOAD_TYPE_INCREMENTAL
        hwm_in_hybrid_view = True
    elif offload_pattern in OFFLOAD_PATTERN_100_10:
        hwm_in_hybrid_view = True

    if string_in_ora_view_text(frontend_api, hybrid_schema, agg_name, 'UNION ALL') != hwm_in_hybrid_view:
        return False
    # Agg rules get their stats from base table so there should always be some
    if frontend_api.table_row_count_from_stats(hybrid_schema, '%s_ext' % agg_name) is None:
        return False

    if hwm_in_hybrid_view:
        if bool(get_hybrid_view_ipa_value_from_metadata(hybrid_schema, table_name, repo_client)
                in (frontend_api.get_view_ddl(hybrid_schema, agg_name) or '')) != hwm_in_hybrid_view:
            log('get_hybrid_view_ipa_value_from_metadata() != %s' % hwm_in_hybrid_view)
            return False
    if not check_metadata(hybrid_schema, agg_name, repo_client, hybrid_owner=hybrid_schema, hybrid_view=agg_name,
                          hadoop_owner=data_db_schema, hadoop_table=backend_table, offload_type=offload_type,
                          incremental_high_value=get_metadata_item(hybrid_schema, table_name, repo_client,
                                                                   'incremental_high_value')):
        return False
    return True
