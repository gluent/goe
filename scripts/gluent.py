# LICENSE_TEXT_HEADER

import os
import sys

if not os.environ.get('OFFLOAD_HOME'):
    print('OFFLOAD_HOME environment variable missing')
    print('You should source environment variables first, eg: . ../conf/offload.env')
    sys.exit(1)

from copy import copy
from datetime import datetime, timedelta
import json
import logging
import os.path
import math
from optparse import OptionParser, Option, OptionValueError, SUPPRESS_HELP
import re
from textwrap import dedent
import traceback
from typing import Union

import orjson

from gluentlib.config import config_descriptions, orchestration_defaults
from gluentlib.config.config_validation_functions import normalise_size_option
from gluentlib.filesystem.gluent_dfs import get_scheme_from_location_uri,\
    OFFLOAD_FS_SCHEME_INHERIT, VALID_OFFLOAD_FS_SCHEMES
from gluentlib.filesystem.gluent_dfs_factory import get_dfs_from_options

from gluentlib.offload.backend_api import IMPALA_SHUFFLE_HINT, IMPALA_NOSHUFFLE_HINT
from gluentlib.offload.factory.backend_api_factory import backend_api_factory
from gluentlib.offload.factory.backend_table_factory import backend_table_factory, get_backend_table_from_metadata
from gluentlib.offload.factory.frontend_api_factory import frontend_api_factory_ctx
from gluentlib.offload.factory.offload_source_table_factory import OffloadSourceTable
from gluentlib.offload.column_metadata import ColumnBucketInfo, \
    get_column_names, get_partition_source_column_names,\
    invalid_column_list_message, match_table_column,\
    is_synthetic_partition_column, valid_column_list, \
    GLUENT_TYPE_DECIMAL, GLUENT_TYPE_DATE, GLUENT_TYPE_DOUBLE,\
    GLUENT_TYPE_FLOAT, GLUENT_TYPE_INTEGER_1, GLUENT_TYPE_INTEGER_2, GLUENT_TYPE_INTEGER_4, GLUENT_TYPE_INTEGER_8,\
    GLUENT_TYPE_INTEGER_38, \
    GLUENT_TYPE_VARIABLE_STRING, GLUENT_TYPE_TIMESTAMP_TZ
from gluentlib.offload.hadoop.hadoop_backend_api import format_hive_set_command
from gluentlib.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_IMPALA, DBTYPE_HIVE, \
    DBTYPE_ORACLE, DBTYPE_MSSQL, \
    FILE_STORAGE_COMPRESSION_CODEC_GZIP, FILE_STORAGE_COMPRESSION_CODEC_SNAPPY, FILE_STORAGE_COMPRESSION_CODEC_ZLIB, \
    HYBRID_EXT_TABLE_DEGREE_AUTO, \
    LOG_LEVEL_INFO, LOG_LEVEL_DETAIL, LOG_LEVEL_DEBUG,\
    OFFLOAD_BUCKET_NAME, NUM_BUCKETS_AUTO, \
    OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED, \
    SORT_COLUMNS_NO_CHANGE
from gluentlib.offload.offload_functions import convert_backend_identifier_case, data_db_name
from gluentlib.offload.offload_source_data import offload_source_data_factory, get_offload_type_for_config, \
    OFFLOAD_SOURCE_CLIENT_OFFLOAD
from gluentlib.offload.offload_source_table import OffloadSourceTableInterface, \
    DATA_SAMPLE_SIZE_AUTO, OFFLOAD_PARTITION_TYPE_RANGE, OFFLOAD_PARTITION_TYPE_LIST
from gluentlib.offload.offload_messages import OffloadMessages, VERBOSE, VVERBOSE
from gluentlib.offload.offload_metadata_functions import decode_metadata_incremental_high_values_from_metadata,\
    flatten_lpa_high_values, incremental_key_csv_from_part_keys, \
    gen_and_save_offload_metadata, \
    METADATA_HYBRID_VIEW
from gluentlib.offload.offload_validation import BackendCountValidator, CrossDbValidator,\
    build_verification_clauses
from gluentlib.offload.offload_transport import choose_offload_transport_method, offload_transport_factory, \
    validate_offload_transport_method, \
    VALID_OFFLOAD_TRANSPORT_METHODS
from gluentlib.offload.offload_transport_functions import transport_and_load_offload_chunk
from gluentlib.offload.operation.data_type_controls import canonical_columns_from_columns_csv,\
    char_semantics_override_map
from gluentlib.offload.operation.not_null_columns import apply_not_null_columns_csv
from gluentlib.offload.operation.partition_controls import derive_partition_digits, offload_options_to_partition_info,\
    validate_offload_partition_columns, validate_offload_partition_functions, validate_offload_partition_granularity
from gluentlib.offload.operation.sort_columns import check_and_alter_backend_sort_columns,\
    sort_columns_csv_to_sort_columns
from gluentlib.offload.predicate_offload import GenericPredicate
from gluentlib.offload.oracle.oracle_column import ORACLE_TYPE_BINARY_FLOAT
from gluentlib.orchestration import command_steps
from gluentlib.orchestration.execution_id import ExecutionId
from gluentlib.persistence.factory.orchestration_repo_client_factory import orchestration_repo_client_factory
from gluentlib.persistence.orchestration_metadata import OrchestrationMetadata, hwm_column_names_from_predicates,\
    INCREMENTAL_PREDICATE_TYPE_PREDICATE, INCREMENTAL_PREDICATE_TYPE_LIST, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,\
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE, INCREMENTAL_PREDICATE_TYPE_RANGE,\
    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE, INCREMENTAL_PREDICATE_TYPES_WITH_PREDICATE_IN_HV

from gluentlib.data_governance.hadoop_data_governance import get_hadoop_data_governance_client_from_options,\
    is_valid_data_governance_tag, data_governance_update_metadata_step
from gluentlib.data_governance.hadoop_data_governance_constants import DATA_GOVERNANCE_GLUENT_OBJECT_TYPE_BASE_TABLE

from gluentlib.util.misc_functions import case_insensitive_in, csv_split, bytes_to_human_size,\
    human_size_to_bytes, is_pos_int, format_list_for_logging,\
    standard_log_name
from gluentlib.util.hs2_connection import hs2_connection as hs2_connection_with_opts
from gluentlib.util.ora_query import get_oracle_connection
from gluentlib.util.redis_tools import RedisClient


class OffloadException(Exception):
    pass


dev_logger = logging.getLogger('gluent')

lob_null_special_value = 'X\'00\''

BACKEND_DATA_TYPE_CONTROL_OPTIONS = {GLUENT_TYPE_INTEGER_1: '--integer-1-columns',
                                     GLUENT_TYPE_INTEGER_2: '--integer-2-columns',
                                     GLUENT_TYPE_INTEGER_4: '--integer-4-columns',
                                     GLUENT_TYPE_INTEGER_8: '--integer-8-columns',
                                     GLUENT_TYPE_INTEGER_38: '--integer-38-columns',
                                     GLUENT_TYPE_DOUBLE: '--double-columns',
                                     GLUENT_TYPE_VARIABLE_STRING: '--variable-string-columns'}
DECIMAL_COL_TYPE_SYNTAX_TEMPLATE =\
    'must be of format "precision,scale" where 1<=precision<={p} and 0<=scale<={s} and scale<=precision'

AAPD_VALID_NUMERIC_FNS = ['AVG', 'MIN', 'MAX', 'COUNT', 'SUM']
AAPD_VALID_STRING_FNS = ['MIN', 'MAX', 'COUNT']
AAPD_VALID_DATE_FNS = ['MIN', 'MAX', 'COUNT']

OFFLOAD_PATTERN_100_0, OFFLOAD_PATTERN_90_10, OFFLOAD_PATTERN_100_10 = list(range(3))

HYBRID_EXT_TABLE_DEGREE_DEFAULT = 'DEFAULT'
HYBRID_EXT_TABLE_CHARSET = 'AL32UTF8'
HYBRID_EXT_TABLE_PREPROCESSOR = 'smart_connector.sh'

OFFLOAD_OP_NAME = 'offload'

CONFIG_FILE_NAME = 'offload.env'
LOCATION_FILE_BASE = 'offload.conf'

OFFLOAD_STATS_METHOD_COPY = 'COPY'
OFFLOAD_STATS_METHOD_HISTORY = 'HISTORY'
OFFLOAD_STATS_METHOD_NATIVE = 'NATIVE'
OFFLOAD_STATS_METHOD_NONE = 'NONE'

TOKENISE_STRING_CHAR_1 = '~'
TOKENISE_STRING_CHAR_2 = '^'
TOKENISE_STRING_CHAR_3 = '!'
TOKENISE_STRING_CHARS = [TOKENISE_STRING_CHAR_1, TOKENISE_STRING_CHAR_2, TOKENISE_STRING_CHAR_3]

LEGACY_MAX_HYBRID_IDENTIFIER_LENGTH = 30

# Used in test to identify specific warnings
ADJUSTED_BACKEND_IDENTIFIER_MESSAGE_TEXT = 'Using adjusted backend table name'
CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT = 'Conflicting data identification options'
HYBRID_SCHEMA_STEPS_DUE_TO_HWM_CHANGE_MESSAGE_TEXT = 'Including hybrid schema steps due to HWM change'
IPA_PREDICATE_TYPE_CHANGE_EXCEPTION_TEXT = 'INCREMENTAL_PREDICATE_TYPE cannot be changed for offloaded table'
IPA_PREDICATE_TYPE_EXCEPTION_TEXT = '--offload-predicate-type/RDBMS partition type combination is not valid'
IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT = 'partition identification options are not compatible'
IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT = '--offload-predicate-type is not valid for a first time predicate-based offload'
IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT = 'Missing --offload-predicate option. This option is mandatory to offload tables with an INCREMENTAL_PREDICATE_TYPE configuration of PREDICATE'
JOIN_PUSHDOWN_HWM_EXCEPTION_TEXT = 'High water mark for join can not be influenced by --older-than-date/days, --less-than-value or --offload-predicate options'
JOIN_PUSHDOWN_IGNORE_BUCKET_MESSAGE_TEXT = 'Materialized joins generate synthetic bucket column'
JOIN_PUSHDOWN_INCOMPATIBLE_PREDICATE_EXCEPTION_TEXT = 'Incompatible join INCREMENTAL_PREDICATE_VALUEs'
MISSING_HYBRID_METADATA_EXCEPTION_TEMPLATE = 'Missing hybrid metadata for hybrid view %s.%s, contact Gluent support (or --reset-backend-table to overwrite table data)'
NLS_LANG_MISSING_CHARACTER_SET_EXCEPTION_TEMPLATE = 'NLS_LANG value %s missing character set delimiter (.)'
OFFLOAD_STATS_COPY_EXCEPTION_TEXT = 'Invalid --offload-stats value'
OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT = 'Column mismatch detected between the source and backend table. Resolve before offloading'
OFFLOAD_TYPE_CHANGE_FOR_LIST_EXCEPTION_TEXT = 'Switching to offload type INCREMENTAL for LIST partitioned table requires --equal-to-values/--partition-names'
OFFLOAD_TYPE_CHANGE_FOR_LIST_MESSAGE_TEXT = 'Switching to INCREMENTAL for LIST partitioned table'
OFFLOAD_TYPE_CHANGE_FOR_SUBPART_EXCEPTION_TEXT = 'Switching from offload type FULL to INCREMENTAL is not supported for Subpartition-Based Offload'
RESET_HYBRID_VIEW_EXCEPTION_TEXT = 'Offload data identification options required with --reset-hybrid-view'
RETAINING_PARTITITON_FUNCTIONS_MESSAGE_TEXT = 'Retaining partition functions from backend target'
TOTAL_ROWS_OFFLOADED_LOG_TEXT = 'Total rows offloaded'

# Config that you would expect to be different from one offload to the next
EXPECTED_OFFLOAD_ARGS = [
    'allow_decimal_scale_rounding', 'allow_floating_point_conversions', 'allow_nanosecond_timestamp_columns',
    'bucket_hash_col',
    'column_transformation_list', 'compress_load_table', 'compute_load_table_stats', 'create_backend_db',
    'data_governance_custom_tags_csv', 'data_governance_custom_properties',
    'data_sample_parallelism', 'data_sample_pct', 'date_columns_csv', 'date_fns',
    'decimal_columns_csv_list', 'decimal_columns_type_list', 'decimal_padding_digits', 'double_columns_csv',
    'equal_to_values', 'error_before_step', 'error_after_step', 'ext_readsize', 'ext_table_name',
    'force', 'generate_dependent_views',
    'hive_column_stats', 'hybrid_ext_table_degree',
    'impala_insert_hint',
    'integer_1_columns_csv', 'integer_2_columns_csv', 'integer_4_columns_csv', 'integer_8_columns_csv',
    'integer_38_columns_csv', 'ipa_predicate_type',
    'less_than_value', 'lob_data_length',
    'max_offload_chunk_count', 'max_offload_chunk_size', 'not_null_columns_csv',
    'num_buckets', 'num_location_files', 'numeric_fns',
    'offload_by_subpartition', 'offload_chunk_column', 'offload_distribute_enabled',
    'offload_fs_container', 'offload_fs_prefix', 'offload_fs_scheme',
    'offload_partition_columns', 'offload_partition_functions', 'offload_partition_granularity',
    'offload_partition_lower_value', 'offload_partition_upper_value',
    'offload_predicate', 'offload_predicate_modify_hybrid_view',
    'offload_stats_method', 'offload_transport_method', 'offload_type',
    'offload_transport_consistent_read', 'offload_transport_dsn', 'offload_transport_fetch_size',
    'offload_transport_jvm_overrides',
    'offload_transport_queue_name', 'offload_transport_parallelism', 'offload_transport_small_table_threshold',
    'offload_transport_spark_properties', 'offload_transport_validation_polling_interval',
    'older_than_date', 'older_than_days', 'operation_name', 'owner_table',
    'partition_names_csv', 'preserve_load_table', 'purge_backend_table',
    'reset_backend_table', 'reset_hybrid_view',
    'sort_columns_csv', 'sqoop_additional_options', 'sqoop_mapreduce_map_memory_mb', 'sqoop_mapreduce_map_java_opts',
    'skip', 'storage_compression', 'storage_format', 'string_fns', 'synthetic_partition_digits', 'suppress_stdout',
    'target_owner_name', 'timestamp_tz_columns_csv', 'unicode_string_columns_csv',
    'variable_string_columns_csv', 'ver_check', 'verify_parallelism', 'verify_row_count'
]

normal, verbose, vverbose = list(range(3))
options = None
log_fh = None
suppress_stdout_override = False
execution_id = ""


redis_execution_id = None
redis_in_error = False

def hs2_connection(opts=None):
    """ Wrapper for gluentlib hs2_connection() for backward compatibility
    """
    return hs2_connection_with_opts(opts or options)


def ansi(line, ansi_code):
    return OffloadMessages.ansi_wrap(line, ansi_code, options.ansi)


def serialize_object(obj) -> str:
    """
    Encodes json with the optimized ORJSON package

    orjson.dumps returns bytearray, so you can't pass it directly as json_serializer
    """
    return orjson.dumps(
        obj,
        option=orjson.OPT_NAIVE_UTC | orjson.OPT_SERIALIZE_NUMPY,
    ).decode()


def log(line, detail=normal, ansi_code=None, redis_publish=True):
    global log_fh
    global redis_in_error
    global redis_execution_id

    def fh_log(line):
        log_fh.write((line or '') + '\n')
        log_fh.flush()

    def stdout_log(line):
        sys.stdout.write((line or '') + '\n')
        sys.stdout.flush()

    if redis_publish and orchestration_defaults.cache_enabled() and not redis_in_error and redis_execution_id:
        try:
            cache = RedisClient.connect()
            msg = {
                "message": line,
            }
            cache.rpush(
                f"gluent:run:{redis_execution_id}", serialize_object(msg), ttl=timedelta(hours=48)
            )
        except Exception as exc:
            fh_log('Disabling Redis integration due to: {}'.format(str(exc)))
            redis_in_error = True

    if not log_fh:
        log_fh = sys.stderr
    fh_log(line)
    if not options:
        # it has been known for exceptions to be thrown before options is defined
        stdout_log(line)
    elif options.quiet:
        stdout_log('.')
    elif ((detail == normal or (detail <= verbose and options.verbose) or (detail <= vverbose and options.vverbose))
          and not suppress_stdout_override):
        line = ansi(line, ansi_code)
        stdout_log(line)


def get_log_fh():
    return log_fh


def log_command_line(detail=vverbose):
    log('Command line:', detail=detail, redis_publish=False)
    log(' '.join(sys.argv), detail=detail, redis_publish=False)
    log('', detail=detail, redis_publish=False)


def ora_single_item_query(opts, qry, ora_conn=None, params={}):
    use_conn = ora_conn or oracle_connection(opts)
    try:
        c = use_conn.cursor()
        row = c.execute(qry, params).fetchone()
        return row[0] if row else row
    finally:
        c.close()
        if not ora_conn:
            use_conn.close()


def get_db_unique_name(opts):
  if opts.db_type == DBTYPE_ORACLE:
    sql = """
SELECT SYS_CONTEXT('USERENV', 'DB_UNIQUE_NAME') ||
       CASE
          WHEN version >= 12
          THEN CASE
                  WHEN SYS_CONTEXT('USERENV', 'CDB_NAME') IS NOT NULL
                  THEN '_' || SYS_CONTEXT('USERENV', 'CON_NAME')
               END
       END
FROM  (
       SELECT TO_NUMBER(REGEXP_SUBSTR(version, '[0-9]+')) AS version
       FROM   v$instance
      )
"""
    return ora_single_item_query(opts, sql)
  elif opts.db_type == DBTYPE_MSSQL:
    try:
      return opts.rdbms_dsn.split('=')[1]
    except:
      return ""


def get_rdbms_db_name(opts, ora_conn=None):
  if opts.db_type == DBTYPE_ORACLE:
    sql = """
SELECT CASE
          WHEN version < 12
          THEN SYS_CONTEXT('USERENV', 'DB_NAME')
          ELSE SYS_CONTEXT('USERENV', 'CON_NAME')
       END
FROM  (
       SELECT TO_NUMBER(REGEXP_SUBSTR(version, '[0-9]+')) AS version
       FROM   v$instance
      )
"""
    return ora_single_item_query(opts, sql, ora_conn)
  elif opts.db_type == DBTYPE_MSSQL:
    try:
      return opts.rdbms_dsn.split('=')[1]
    except:
      return ""


def get_db_charset(opts):
    return ora_single_item_query(opts, "SELECT value FROM nls_database_parameters WHERE parameter = 'NLS_CHARACTERSET'")


def get_max_hybrid_identifier_length(connection_options, messages):
    """ Get the max supported hybrid identifier (table/view/column) length for the frontend RDBMS.
        This is not ideal because it is making a frontend connection just to get this information but at the point
        this is called we don't already have a connection we can use.
    """
    if not connection_options:
        return None
    with frontend_api_factory_ctx(connection_options.db_type, connection_options, messages,
                                  dry_run=bool(not connection_options.execute),
                                  trace_action='get_max_hybrid_identifier_length') as frontend_api:
        return frontend_api.max_table_name_length()


def next_power_of_two(x):
    return int(2**(math.ceil(math.log(x, 2))))


def nls_lang_exists():
    return os.environ.get('NLS_LANG')


def nls_lang_has_charset():
    if nls_lang_exists():
        return '.' in os.environ.get('NLS_LANG')


def set_nls_lang_default(opts):
    os.environ['NLS_LANG'] = '.%s' % get_db_charset(opts)


def check_and_set_nls_lang(opts, messages=None):
  # TODO: We believe that we need to have NLS_LANG set correctly in order for query_import to offload data correctly?
  #       If that is the case if/when we implement query_import for non-Oracle, we need to cater for this.
  if opts.db_type == DBTYPE_ORACLE:
    if not nls_lang_exists():
      set_nls_lang_default(opts)
      if messages:
        messages.warning('NLS_LANG not specified in environment, setting to "%s"' % os.environ['NLS_LANG'],
                         ansi_code='red')
    else:
        if not nls_lang_has_charset():
            raise OffloadException(NLS_LANG_MISSING_CHARACTER_SET_EXCEPTION_TEMPLATE % os.environ['NLS_LANG'])


def silent_close(something_that_closes):
    try:
        log('Silently closing %s' % str(type(something_that_closes)), vverbose)
        something_that_closes.close()
    except Exception as e:
        log('Exception issuing close() silently:\n%s' % str(e), vverbose)


def all_int_chars(int_str, allow_negative=False):
  """ returns true if all chars in a string are 0-9
  """
  if allow_negative:
    return bool(re.match(r'^-?\d+$', str(int_str)))
  else:
    return bool(re.match(r'^\d+$', str(int_str)))


def get_offload_type(owner, table_name, hybrid_operation, incr_append_capable, partition_type, hybrid_metadata, messages, with_messages=True):
    """ Wrapper for get_offload_type_for_config that caters for speculative retrievals of offload_type
        Used when deciding whether to auto-enable subpartition offloads
    """
    ipa_options_specified = active_data_append_options(hybrid_operation, partition_type=partition_type)
    messages.debug('ipa_options_specified: %s' % str(ipa_options_specified))
    offload_type, _ = get_offload_type_for_config(
        owner, table_name, hybrid_operation.offload_type,
        incr_append_capable, ipa_options_specified, hybrid_metadata,
        messages, with_messages=with_messages
    )
    return offload_type


def offload_bucket_name():
    return OFFLOAD_BUCKET_NAME


class OptionError(Exception):
  def __init__(self, detail):
    self.detail = detail

  def __str__(self):
    return repr(self.detail)


global ts
ts = None


def log_timestamp(ansi_code='grey'):
  if options and options.execute:
    global ts
    ts = datetime.now()
    ts = ts.replace(microsecond=0)
    log(ts.strftime('%c'), detail=verbose, ansi_code=ansi_code)


def log_timedelta(ansi_code='grey', hybrid_options=None):
  use_opts = hybrid_options or options
  if use_opts.execute:
    ts2 = datetime.now()
    ts2 = ts2.replace(microsecond=0)
    log('Step time: %s' % (ts2 - ts), detail=verbose, ansi_code=ansi_code)
    return ts2 - ts


def enter_or_cancel(msg, allowed=('', 'S')):
  try:
    i = None
    while i not in allowed:
      i = input(msg).upper()
    return i
  except KeyboardInterrupt as exc:
    sys.exit(1)


# TODO Should really be named oracle_adm_connection
def oracle_connection(opts, proxy_user=None):
  return get_oracle_connection(opts.ora_adm_user, opts.ora_adm_pass, opts.rdbms_dsn, opts.use_oracle_wallet, proxy_user)


def oracle_offload_transport_connection(config_options):
  return get_oracle_connection(config_options.rdbms_app_user, config_options.rdbms_app_pass,
                               config_options.offload_transport_dsn, config_options.use_oracle_wallet)


def incremental_offload_partition_overrides(offload_operation, existing_part_digits, messages):
  if existing_part_digits and existing_part_digits != offload_operation.synthetic_partition_digits:
    offload_operation.synthetic_partition_digits = existing_part_digits
    messages.notice('Retaining partition digits from backend target (ignoring --partition-digits)')
  if offload_operation.offload_partition_columns:
    messages.notice('Retaining partition column scheme from backend target (ignoring --partition-columns)')
  if offload_operation.offload_partition_functions:
    messages.notice(f'{RETAINING_PARTITITON_FUNCTIONS_MESSAGE_TEXT} (ignoring --partition-functions)')


def parse_size_expr(size, binary_sizes=False):
  """ Converts a size string, such as 10M or 64G, into bytes """
  return human_size_to_bytes(size, binary_sizes=binary_sizes)


def create_final_backend_table(offload_target_table, offload_operation, offload_options, rdbms_columns,
                               gluent_object_type=DATA_GOVERNANCE_GLUENT_OBJECT_TYPE_BASE_TABLE):
  """ Create the final backend table """
  if not offload_target_table.table_exists() or offload_operation.reset_backend_table:
    offload_target_table.create_backend_table_step(gluent_object_type)
  else:
    check_and_alter_backend_sort_columns(offload_target_table, offload_operation)


def hybrid_owner(target_owner):
  return OffloadOperation.hybrid_owner(target_owner)


def extract_marker(i, token_char=TOKENISE_STRING_CHAR_1):
  return str(token_char + str(i) + token_char)


def verify_offload_by_backend_count(offload_source_table, offload_target_table, ipa_predicate_type, offload_options,
                                    messages, verification_hvs, prior_hvs, verify_parallelism,
                                    inflight_offload_predicate=None):
  """ Verify (by row counts) the data offloaded in the current operation.
      For partitioned tables the partition columns and verification_hvs and prior_hvs are used to limit
      scanning to the relevant data in both frontend abd backend.
  """
  validator = BackendCountValidator(offload_source_table, offload_target_table, messages,
                                    dry_run=bool(not offload_options.execute))
  bind_predicates = bool(ipa_predicate_type in [INCREMENTAL_PREDICATE_TYPE_RANGE,
                                                INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE])
  frontend_filters, query_binds = build_verification_clauses(offload_source_table, ipa_predicate_type,
                                                             verification_hvs, prior_hvs, inflight_offload_predicate,
                                                             with_binds=bind_predicates)
  # TODO NJ@2017-03-15 We need a second set of backend filters for synthetic partition keys (GOE-790)
  backend_filters, _ = build_verification_clauses(offload_source_table, ipa_predicate_type,
                                                  verification_hvs, prior_hvs, inflight_offload_predicate,
                                                  with_binds=False, backend_table=offload_target_table)
  query_hint_block = offload_source_table.enclose_query_hints(
      offload_source_table.parallel_query_hint(verify_parallelism))
  num_diff, source_rows, hybrid_rows = validator.validate(frontend_filters, query_binds, backend_filters,
                                                          frontend_hint_block=query_hint_block)
  return num_diff, source_rows, hybrid_rows


def verify_row_count_by_aggs(offload_source_table, offload_target_table, ipa_predicate_type, options,
                             messages, verification_hvs, prior_hvs, verify_parallelism,
                             inflight_offload_predicate=None):
  """ Light verification by running aggregate queries in both Oracle and backend
      and comparing their results
  """
  validator = CrossDbValidator(
    db_name=offload_source_table.owner,
    table_name=offload_source_table.table_name,
    connection_options=options,
    backend_obj=offload_target_table.get_backend_api(),
    messages=messages,
    backend_db=offload_target_table.db_name,
    backend_table=offload_target_table.table_name
  )

  bind_predicates = bool(ipa_predicate_type in [INCREMENTAL_PREDICATE_TYPE_RANGE, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE])
  frontend_filters, query_binds = build_verification_clauses(offload_source_table, ipa_predicate_type,
                                                             verification_hvs, prior_hvs, inflight_offload_predicate,
                                                             with_binds=bind_predicates)
  backend_filters, _ = build_verification_clauses(offload_source_table, ipa_predicate_type,
                                                  verification_hvs, prior_hvs, inflight_offload_predicate,
                                                  with_binds=False, backend_table=offload_target_table)
  # TODO NJ@2017-03-15 We need a second set of backend filters for Hadoop partition keys (GOE-790)
  status, _ = validator.validate(safe=False, filters=backend_filters, execute=options.execute,
                                 frontend_filters=frontend_filters, frontend_query_params=query_binds,
                                 frontend_parallelism=verify_parallelism)
  return status


def offload_data_verification(offload_source_table, offload_target_table, offload_operation, offload_options,
                              messages, source_data_client):
  """ Verify offloaded data by either rowcount or sampling aggregation functions.
      Boundary conditions used to verify only those partitions offloaded by the current operation.
  """
  new_hvs = None
  prior_hvs = None
  if source_data_client.is_partition_append_capable():
    # Let's add query boundary conditions
    offloading_open_ended_partition = source_data_client.offloading_open_ended_partition()
    if source_data_client.get_partition_append_predicate_type() in [INCREMENTAL_PREDICATE_TYPE_RANGE,
                                                                    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE]:
      # Leave prior_hvs unset for LIST because we don't have a lower bound
      if source_data_client.partitions_to_offload.count() > 0:
        prior_hv_tuple = get_prior_offloaded_hv(offload_source_table, source_data_client, offload_operation=offload_operation)
        if prior_hv_tuple:
            prior_hvs = prior_hv_tuple[1]

    if offloading_open_ended_partition:
      log('MAXVALUE/DEFAULT partition was offloaded therefore cannot add upper bound to verification query', detail=vverbose)
    else:
      new_hv_tuple = get_current_offload_hv(offload_source_table, source_data_client, offload_operation)
      if new_hv_tuple:
        new_hvs = new_hv_tuple[1]

  if offload_operation.verify_row_count == 'minus':
    hybrid_name = 'Hybrid'
    hybrid_label = 'hybrid_rows'
    if offload_source_table.hybrid_schema_supported():
      raise OffloadException('Hybrid Schema is no longer supported')
    else:
      hybrid_name = offload_target_table.backend_db_name()
      hybrid_label = 'backend_rows'
      verify_fn = lambda: verify_offload_by_backend_count(offload_source_table, offload_target_table,
                                                          offload_operation.ipa_predicate_type,
                                                          offload_options, messages, new_hvs, prior_hvs,
                                                          offload_operation.verify_parallelism,
                                                          inflight_offload_predicate=source_data_client.get_inflight_offload_predicate())
    verify_by_count_results = messages.offload_step(command_steps.STEP_VERIFY_EXPORTED_DATA,
                                                    verify_fn, execute=offload_options.execute)
    if offload_options.execute and verify_by_count_results:
        num_diff, source_rows, hybrid_rows = verify_by_count_results
        if num_diff == 0:
            log(f'Source and {hybrid_name} table data matches: offload successful'
                + (' (with warnings)' if messages.get_warnings() else ''),
                ansi_code='green')
            log('%s origin_rows, %s %s' % (source_rows, hybrid_rows, hybrid_label), verbose)
        else:
            raise OffloadException('Source and Hybrid mismatch: %s differences, %s origin_rows, %s %s'
                                   % (num_diff, source_rows, hybrid_rows, hybrid_label))
  else:
    verify_fn = lambda: verify_row_count_by_aggs(offload_source_table, offload_target_table,
                                                 offload_operation.ipa_predicate_type,
                                                 offload_options, messages,
                                                 new_hvs, prior_hvs, offload_operation.verify_parallelism,
                                                 inflight_offload_predicate=source_data_client.get_inflight_offload_predicate())
    if messages.offload_step(command_steps.STEP_VERIFY_EXPORTED_DATA,
                             verify_fn, execute=offload_options.execute):
      log('Source and Hybrid table data matches: offload successful%s'
          % (' (with warnings)' if messages.get_warnings() else ''), ansi_code='green')
    else:
      raise OffloadException('Source and Hybrid mismatch')


def parse_yyyy_mm_dd(ds):
  return datetime.strptime(ds, '%Y-%m-%d')


def get_prior_offloaded_hv(rdbms_table, source_data_client=None, offload_operation=None):
  """ Identifies the HV for a RANGE offload of the partition prior to the offload
      If there is pre-offload metadata we can use that otherwise we need to go back to the list of partitions
  """
  prior_hvs = None

  if offload_operation and offload_operation.pre_offload_hybrid_metadata and offload_operation.pre_offload_hybrid_metadata.incremental_high_value:
    # use metadata to get the prior high value
    log('Assigning prior_hv from pre-offload metadata', detail=vverbose)
    _, real_hvs, literal_hvs = decode_metadata_incremental_high_values_from_metadata(
        offload_operation.pre_offload_hybrid_metadata, rdbms_table
    )
    log('pre-offload metadata real values: %s' % str(real_hvs), detail=vverbose)
    log('pre-offload metadata rdbms literals: %s' % str(literal_hvs), detail=vverbose)
    prior_hvs = [literal_hvs, real_hvs]

  if not prior_hvs and source_data_client:
    min_offloaded_partition = source_data_client.partitions_to_offload.get_partition_by_index(-1)
    if min_offloaded_partition:
      # we haven't gotten values from metadata so let's look at the partition list for the prior HV
      log('Searching original partitions for partition prior to: %s'
          % str(min_offloaded_partition.partition_values_python), detail=vverbose)
      prior_partition = source_data_client.source_partitions.get_prior_partition(partition=min_offloaded_partition)
      if prior_partition:
        prior_hvs = [prior_partition.partition_values_individual, prior_partition.partition_values_python]

  log('Found prior_hvs: %s' % str(prior_hvs), detail=vverbose)
  return prior_hvs


def get_current_offload_hv(offload_source_table, source_data_client, offload_operation):
  """ Identifies the HV for an IPA offload
      If there are partitions in flight then we can use those otherwise we will fall back to using pre-offload metadata
      Returns a list of length 2 containing:
        [RDBMS hv literals, python hv equivalents]
  """
  new_hvs = None

  if source_data_client.partitions_to_offload.count() > 0:
    if offload_operation.ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
      hvs_individual, hvs_python = [], []
      for p in source_data_client.partitions_to_offload.get_partitions():
          hvs_individual.append(p.partition_values_individual)
          hvs_python.append(p.partition_values_python)
      new_hvs = [hvs_individual, hvs_python]
    else:
      # we are offloading partitions so use the latest one from that list, this works for verification mode too
      max_partition = source_data_client.partitions_to_offload.get_partition_by_index(0)
      new_hvs = [max_partition.partition_values_individual, max_partition.partition_values_python]
    log('Identified new_hvs from partitions_to_offload: %s' % str(new_hvs), detail=vverbose)
  elif offload_operation.offload_type == 'INCREMENTAL' and not offload_operation.reset_backend_table:
    # if no partitions in flight then get metadata in order to understand HWM
    current_metadata = offload_operation.get_hybrid_metadata()
    _, _, partition_literal_hvs = decode_metadata_incremental_high_values_from_metadata(
        current_metadata, offload_source_table
    )
    new_hvs = [partition_literal_hvs, partition_literal_hvs]
    log('Identified new_hvs from metadata: %s' % str(new_hvs), detail=vverbose)

  return new_hvs


def get_offload_data_manager(offload_source_table, offload_target_table, offload_operation, offload_options, messages,
                             existing_metadata, source_client_type, partition_columns=None,
                             include_col_offload_source_table=False):
  """ Return a source data manager object which has methods for slicing and dicing RDBMS partitions and state
      containing which partitions to offload and data to construct hybrid view/verification predicates
  """
  if offload_target_table.exists() and not offload_target_table.is_view() and not offload_operation.reset_backend_table:
    # "not offload_target_table.is_view()" because we pass through here for presented joins too and do not expect previous metadata
    log('Pre-offload metadata: ' + str(existing_metadata), detail=vverbose)
    if not existing_metadata:
      messages.warning('Backend table exists but hybrid metadata is missing, this appears to be recovery from a failed offload')

  if include_col_offload_source_table and existing_metadata:
    col_offload_source_table_override = OffloadSourceTable.create(existing_metadata.hybrid_owner,
                                                                  existing_metadata.hybrid_view,
                                                                  offload_options,
                                                                  messages,
                                                                  offload_by_subpartition=offload_source_table.offload_by_subpartition)
  else:
    col_offload_source_table_override = None

  source_data_client = offload_source_data_factory(offload_source_table, offload_target_table,
                                                   offload_operation, existing_metadata,
                                                   messages, source_client_type,
                                                   rdbms_partition_columns_override=partition_columns,
                                                   col_offload_source_table_override=col_offload_source_table_override)

  source_data_client.offload_data_detection()

  return source_data_client


def normalise_column_transformations(column_transformation_list, offload_cols=None, backend_cols=None):
  # custom_transformations = {transformation: num_params}
  custom_transformations = {'encrypt': 0
                          , 'null': 0
                          , 'suppress': 0
                          , 'regexp_replace': 2
                          , 'tokenize': 0
                          , 'translate': 2}
  param_match = r'[\w$#,-?@^ \.\*\'\[\]\+]*'
  column_transformations = {}

  if not column_transformation_list:
    return column_transformations

  if type(column_transformation_list) == dict:
    # when called on metadata we will be working from a dict
    column_transformation_list = ['%s:%s' % (col, column_transformation_list[col]) for col in column_transformation_list]

  for ct in column_transformation_list:
    if not ':' in ct:
      raise OptionError('Missing transformation for column: %s' % ct)

    m = re.search(r'^([\w$#]+):([\w$#]+)(\(%s\))?$' % param_match, ct)
    if not m or len(m.groups()) != 3:
      raise OptionError('Malformed transformation: %s' % ct)

    cname = m.group(1).lower()
    transformation = m.group(2).lower()
    param_str = m.group(3)

    if not transformation.lower() in custom_transformations:
      raise OptionError('Unknown transformation for column %s: %s' % (cname, transformation))

    if offload_cols:
      match_col = match_table_column(cname, offload_cols)
    else:
      match_col = match_table_column(cname, backend_cols)

    if not match_col:
      raise OptionError('Unknown column in transformation: %s' % cname)

    if transformation in ['translate', 'regexp_replace'] and not match_col.is_string_based():
      raise OptionError('Transformation "%s" not valid for %s column' % (transformation, match_col.data_type.upper()))

    trans_params = []
    if param_str:
      # remove surrounding brackets
      param_str = re.search(r'^\((%s)\)$' % param_match, param_str).group(1)
      trans_params = csv_split(param_str)

    if custom_transformations[transformation] != len(trans_params):
      raise OptionError('Malformed transformation parameters "%s" for column "%s"' % (param_str, cname))

    column_transformations.update({cname: {'transformation': transformation, 'params': trans_params}})

  return column_transformations


def bool_option_from_string(opt_name, opt_val):
    return orchestration_defaults.bool_option_from_string(opt_name, opt_val)


def normalise_owner_table_options(options):
  if not options.owner_table or len(options.owner_table.split('.')) != 2:
    raise OptionError('Option -t or --table required in form SCHEMA.TABLENAME')

  options.owner, options.table_name = options.owner_table.split('.')

  # target/base needed for role separation
  if not hasattr(options, 'target_owner_name') or not options.target_owner_name:
    options.target_owner_name = options.owner_table

  if not hasattr(options, 'base_owner_name') or not options.base_owner_name:
    options.base_owner_name = options.owner_table

  if len(options.target_owner_name.split('.')) != 2:
    raise OptionError('Option --target-name required in form SCHEMA.TABLENAME')
  if len(options.base_owner_name.split('.')) != 2:
    raise OptionError('Option --base-name required in form SCHEMA.TABLENAME')

  options.target_owner, options.target_name = options.target_owner_name.split('.')
  # When presenting, target-name modifies the frontend (hybrid) details.
  # When offloading it modifies the backend (hadoop) details.
  # Need to maintain the upper below for backward compatibility if names pass through trunc_with_hash().
  if hasattr(options, 'operation_name') and options.operation_name == OFFLOAD_OP_NAME:
    options.hybrid_owner, options.hybrid_name = hybrid_owner(options.owner), options.table_name.upper()
  else:
    options.hybrid_owner, options.hybrid_name = hybrid_owner(options.target_owner), options.target_name.upper()
  options.base_owner, options.base_name = options.base_owner_name.upper().split('.')

  if options.base_owner_name.upper() != options.owner_table.upper() and options.target_owner.upper() != options.base_owner.upper():
    raise OptionError('The SCHEMA provided in options --target-name and --base-name must match')


def active_data_append_options(opts, partition_type=None, from_options=False, ignore_partition_names_opt=False, ignore_pbo=False):
  rpa_opts = {'--less-than-value': opts.less_than_value, '--partition-names': opts.partition_names_csv}
  lpa_opts = {'--equal-to-values': opts.equal_to_values, '--partition-names': opts.partition_names_csv}
  ida_opts = {'--offload-predicate': opts.offload_predicate}

  if from_options:
      # options has a couple of synonyms for less_than_value
    rpa_opts.update({'--older-than-days': opts.older_than_days, '--older-than-date': opts.older_than_date})

  if ignore_partition_names_opt:
    del rpa_opts['--partition-names']
    del lpa_opts['--partition-names']

  if partition_type == OFFLOAD_PARTITION_TYPE_RANGE:
    chk_opts = rpa_opts
  elif partition_type == OFFLOAD_PARTITION_TYPE_LIST:
    chk_opts = lpa_opts
  elif not partition_type:
    chk_opts = {} if ignore_pbo else ida_opts.copy()
    chk_opts.update(lpa_opts)
    chk_opts.update(rpa_opts)

  active_pa_opts = [_ for _ in chk_opts if chk_opts[_]]
  return active_pa_opts


def check_ipa_predicate_type_option_conflicts(options, exc_cls=OffloadException, rdbms_table=None):
  ipa_predicate_type = getattr(options, 'ipa_predicate_type', None)
  active_lpa_opts = active_data_append_options(options, partition_type=OFFLOAD_PARTITION_TYPE_LIST, ignore_partition_names_opt=True)
  active_rpa_opts = active_data_append_options(options, partition_type=OFFLOAD_PARTITION_TYPE_RANGE, ignore_partition_names_opt=True)
  if ipa_predicate_type in [INCREMENTAL_PREDICATE_TYPE_RANGE, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE]:
    if active_lpa_opts:
      raise exc_cls('LIST %s with %s: %s' \
          % (IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT, ipa_predicate_type, ', '.join(active_lpa_opts)))
    if rdbms_table and active_rpa_opts:
      # If we have access to an RDBMS table then we can check if the partition column data types are valid for IPA
      unsupported_types = rdbms_table.unsupported_partition_data_types(partition_type_override=OFFLOAD_PARTITION_TYPE_RANGE)
      if unsupported_types:
        raise exc_cls('RANGE %s with partition data types: %s' \
            % (IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT, ', '.join(unsupported_types)))
  elif ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
    if active_rpa_opts:
      raise exc_cls('RANGE %s with %s: %s' \
          % (IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT, ipa_predicate_type, ', '.join(active_rpa_opts)))
  elif ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_PREDICATE:
    if not options.offload_predicate:
      raise exc_cls(IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT)


def normalise_less_than_options(options, exc_cls=OffloadException):
  if not hasattr(options, 'older_than_date'):
    # We mustn't be in offload or present so should just drop out
    return

  active_pa_opts = active_data_append_options(options, from_options=True)
  if len(active_pa_opts) > 1:
      raise exc_cls('%s: %s' % (CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT, ', '.join(active_pa_opts)))

  if options.reset_hybrid_view and len(active_pa_opts) == 0:
    raise exc_cls(RESET_HYBRID_VIEW_EXCEPTION_TEXT)

  if options.older_than_date:
    try:
      # move the value into less_than_value
      options.less_than_value = parse_yyyy_mm_dd(options.older_than_date)
      options.older_than_date = None
    except ValueError as exc:
      raise exc_cls('option --older-than-date: %s' % str(exc))
  elif options.older_than_days:
    options.older_than_days = check_opt_is_posint('--older-than-days', options.older_than_days, allow_zero=True)
    # move the value into less_than_value
    options.less_than_value = datetime.today() - timedelta(options.older_than_days)
    options.older_than_days = None

  check_ipa_predicate_type_option_conflicts(options, exc_cls=exc_cls)


def normalise_offload_predicate_options(options):
  if options.offload_predicate:
    if isinstance(options.offload_predicate, str):
        options.offload_predicate = GenericPredicate(options.offload_predicate)

    if options.less_than_value or options.older_than_date or options.older_than_days:
      raise OptionError('Predicate offload cannot be used with incremental partition offload options: (--less-than-value/--older-than-date/--older-than-days)')

  no_modify_hybrid_view_option_used = not options.offload_predicate_modify_hybrid_view
  if no_modify_hybrid_view_option_used and not options.offload_predicate:
    raise OptionError('--no-modify-hybrid-view can only be used with --offload-predicate')


def normalise_datatype_control_options(opts):
  def upper_or_default(opts, option_name, prior_name=None):
    if hasattr(opts, option_name):
      if getattr(opts, option_name, None):
        setattr(opts, option_name, getattr(opts, option_name).upper())
      elif prior_name and getattr(opts, prior_name, None):
        setattr(opts, option_name, getattr(opts, prior_name).upper())

  upper_or_default(opts, 'integer_1_columns_csv')
  upper_or_default(opts, 'integer_2_columns_csv')
  upper_or_default(opts, 'integer_4_columns_csv')
  upper_or_default(opts, 'integer_8_columns_csv')
  upper_or_default(opts, 'integer_38_columns_csv')
  upper_or_default(opts, 'date_columns_csv')
  upper_or_default(opts, 'double_columns_csv')
  upper_or_default(opts, 'variable_string_columns_csv')
  upper_or_default(opts, 'large_binary_columns_csv')
  upper_or_default(opts, 'large_string_columns_csv')
  upper_or_default(opts, 'binary_columns_csv')
  upper_or_default(opts, 'interval_ds_columns_csv')
  upper_or_default(opts, 'interval_ym_columns_csv')
  upper_or_default(opts, 'timestamp_columns_csv')
  upper_or_default(opts, 'timestamp_tz_columns_csv')
  upper_or_default(opts, 'unicode_string_columns_csv')

  if hasattr(opts, 'decimal_columns_csv_list'):
    if opts.decimal_columns_csv_list is None:
      opts.decimal_columns_csv_list = []
    if opts.decimal_columns_type_list is None:
      opts.decimal_columns_type_list = []


def normalise_insert_select_options(opts):
  if opts.impala_insert_hint:
    opts.impala_insert_hint = opts.impala_insert_hint.upper()
    if opts.impala_insert_hint not in [IMPALA_SHUFFLE_HINT, IMPALA_NOSHUFFLE_HINT]:
      raise OptionError('Invalid value for --impala-insert-hint: %s' % opts.impala_insert_hint)

  if opts.offload_chunk_column:
    opts.offload_chunk_column = opts.offload_chunk_column.upper()


def normalise_offload_transport_method(options, orchestration_config):
  """ offload_transport_method is a hidden option derived from other config and has its own normalisation """
  if options.offload_transport_method:
      options.offload_transport_method = options.offload_transport_method.upper()
      # If an override has been specified then check so we can fail early
      validate_offload_transport_method(options.offload_transport_method, orchestration_config,
                                        exception_class=OptionValueError)


def normalise_offload_transport_user_options(options):
    if not hasattr(options, 'offload_transport_method'):
        # Mustn't be an offload
        return

    if options.offload_transport_small_table_threshold:
        options.offload_transport_small_table_threshold = normalise_size_option(
            options.offload_transport_small_table_threshold,
            strict_name='--offload-transport-small-table-threshold',
            exc_cls=OptionValueError)
    options.offload_transport_consistent_read = bool_option_from_string(
        'OFFLOAD_TRANSPORT_CONSISTENT_READ/--offload-transport-consistent-read',
        options.offload_transport_consistent_read)
    if orchestration_defaults.sqoop_disable_direct_default():
        options.offload_transport_parallelism = 1
    else:
        options.offload_transport_parallelism = check_opt_is_posint(
            'OFFLOAD_TRANSPORT_PARALLELISM/--offload-transport-parallelism',
            options.offload_transport_parallelism)
    options.offload_transport_fetch_size = check_opt_is_posint(
        'OFFLOAD_TRANSPORT_FETCH_SIZE/--offload-transport-fetch-size',
        options.offload_transport_fetch_size)

    verify_json_option('OFFLOAD_TRANSPORT_SPARK_PROPERTIES/--offload-transport-spark-properties',
                       options.offload_transport_spark_properties)

    if hasattr(options, 'offload_transport_validation_polling_interval'):
        if (isinstance(options.offload_transport_validation_polling_interval, str)
                and (re.search('^[\d\.]+$', options.offload_transport_validation_polling_interval)
                     or options.offload_transport_validation_polling_interval == str(
                            OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED))):
            options.offload_transport_validation_polling_interval = float(
                options.offload_transport_validation_polling_interval)
        elif not isinstance(options.offload_transport_validation_polling_interval, (int, float)):
            raise OptionError('Invalid value "%s" for --offload-transport-validation-polling-interval'
                              % options.offload_transport_validation_polling_interval)
    else:
        options.offload_transport_validation_polling_interval = 0


def valid_canonical_decimal_spec(prec, spec, max_decimal_precision, max_decimal_scale):
  if prec < 1 or prec > max_decimal_precision or spec < 0 or spec > max_decimal_scale or spec > prec:
    return False
  return True


def check_bucket_location_combinations(options):
  """ check some parameter combinations and return:
      (valid_combination, message)
  """
  if options.target == DBTYPE_IMPALA:
    return (True, None)

  future_num_location_files = int(options.num_location_files or orchestration_defaults.num_location_files_default())
  if options.num_buckets != 'AUTO' and (int(options.num_buckets) < 1 or int(options.num_buckets) > future_num_location_files):
    return (False, '--num-buckets must be between 1 and --num-location-files/NUM_LOCATION_FILES, please check your environment configuration file')
  if future_num_location_files < 1:
    return (False, '--num-location-files must be greater than 0, please check your environment configuration file')
  return (True, None)


def option_is_in_list(options, option_name, cli_name, validation_list, to_upper=True):
  if hasattr(options, option_name) and getattr(options, option_name):
    opt_val = getattr(options, option_name)
    opt_val = opt_val.upper() if to_upper else opt_val.lower()
    if opt_val not in validation_list:
      raise OptionValueError('Unsupported value for %s: %s' % (cli_name, opt_val))
    return opt_val
  return None


def normalise_options(options, normalise_owner_table=True):
  if options.vverbose:
    options.verbose = True
  elif options.quiet:
    options.vverbose = options.verbose = False

  if hasattr(options, 'log_level') and options.log_level:
    options.log_level = options.log_level.lower()
    if options.log_level not in [LOG_LEVEL_INFO, LOG_LEVEL_DETAIL, LOG_LEVEL_DEBUG]:
      raise OptionValueError('Invalid value for LOG_LEVEL: %s' % options.log_level)

  if options.reset_backend_table and not options.force:
    options.force = True

  if not options.execute:
    options.force = False

  if options.storage_compression:
    options.storage_compression = options.storage_compression.upper()
    if options.storage_compression not in ['NONE', 'HIGH', 'MED', FILE_STORAGE_COMPRESSION_CODEC_GZIP,
                                           FILE_STORAGE_COMPRESSION_CODEC_SNAPPY, FILE_STORAGE_COMPRESSION_CODEC_ZLIB]:
        raise OptionValueError('Invalid value for --storage-compression, valid values: HIGH|MED|NONE|GZIP|ZLIB|SNAPPY')

  if normalise_owner_table:
    normalise_owner_table_options(options)

  normalise_offload_transport_user_options(options)

  if options.target == DBTYPE_IMPALA:
      options.offload_distribute_enabled = False

  normalise_less_than_options(options, exc_cls=OptionError)

  options.offload_type = option_is_in_list(options, 'offload_type', '--offload-type', ['FULL', 'INCREMENTAL'])
  options.ipa_predicate_type = option_is_in_list(options, 'ipa_predicate_type', '--offload-predicate-type', \
      [INCREMENTAL_PREDICATE_TYPE_LIST, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE, INCREMENTAL_PREDICATE_TYPE_RANGE,
       INCREMENTAL_PREDICATE_TYPE_PREDICATE, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
       INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE])
  if options.ipa_predicate_type in [INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
                                    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE] and not options.offload_predicate:
    # User has requested a with-predicate predicate type without providing a predicate
    raise OptionValueError('Option --offload-predicate is required when requesting INCREMENTAL_PREDICATE_TYPE: %s'
                           % options.ipa_predicate_type)

  options.skip = options.skip if type(options.skip) is list else options.skip.lower().split(',')

  if options.hybrid_ext_table_degree != HYBRID_EXT_TABLE_DEGREE_AUTO:
    # if not AUTO then must be integral
    if not all_int_chars(options.hybrid_ext_table_degree):
      raise OptionError('Invalid value for --ext-table-degree, must be %s or integral number'
                        % HYBRID_EXT_TABLE_DEGREE_AUTO)

  valid_combination, message = check_bucket_location_combinations(options)
  if not valid_combination:
    raise OptionError(message)

  if options.offload_partition_lower_value and not all_int_chars(options.offload_partition_lower_value,
                                                                 allow_negative=True):
    raise OptionValueError('Invalid value for --partition-lower-value, must be an integral number: %s'
                           % options.offload_partition_lower_value)

  if options.offload_partition_upper_value and not all_int_chars(options.offload_partition_upper_value,
                                                                 allow_negative=True):
    raise OptionValueError('Invalid value for --partition-upper-value, must be an integral number: %s'
                           % options.offload_partition_upper_value)

  normalise_verify_options(options)
  normalise_data_sampling_options(options)
  normalise_offload_predicate_options(options)
  normalise_datatype_control_options(options)
  normalise_stats_options(options, options.target)
  normalise_data_governance_options(options)


def normalise_verify_options(options):
    if getattr(options, 'verify_parallelism', None):
        options.verify_parallelism = check_opt_is_posint('--verify-parallelism', options.verify_parallelism,
                                                         allow_zero=True)


def normalise_data_sampling_options(options):
    if hasattr(options, 'data_sample_pct'):
        if type(options.data_sample_pct) == str and re.search('^[\d\.]+$', options.data_sample_pct):
            options.data_sample_pct = float(options.data_sample_pct)
        elif options.data_sample_pct == 'AUTO':
            options.data_sample_pct = DATA_SAMPLE_SIZE_AUTO
        elif type(options.data_sample_pct) not in (int, float):
            raise OptionError('Invalid value "%s" for --data-sample-percent' % options.data_sample_pct)
    else:
        options.data_sample_pct = 0

    if hasattr(options, 'data_sample_parallelism'):
        options.data_sample_parallelism = check_opt_is_posint('--data-sample-parallelism',
                                                              options.data_sample_parallelism, allow_zero=True)


def normalise_stats_options(options, target_backend):
  if options.offload_stats_method not in [OFFLOAD_STATS_METHOD_NATIVE, OFFLOAD_STATS_METHOD_HISTORY,
                                          OFFLOAD_STATS_METHOD_COPY, OFFLOAD_STATS_METHOD_NONE]:
    raise OptionError('Unsupported value for --offload-stats: %s' % options.offload_stats_method)

  if options.offload_stats_method not in [OFFLOAD_STATS_METHOD_NATIVE, OFFLOAD_STATS_METHOD_NONE]:
    def raise_conjunction_error(option):
      raise OptionError('Unsupported value for --offload-stats: %s when used with the --%s option'
                        % (options.offload_stats_method, option))

  if options.offload_stats_method == OFFLOAD_STATS_METHOD_HISTORY and target_backend == DBTYPE_IMPALA:
    options.offload_stats_method = OFFLOAD_STATS_METHOD_NATIVE


def verify_json_option(option_name, option_value):
    if option_value:
      try:
        properties = json.loads(option_value)

        invalid_props = [k for k, v in properties.items() if type(v) not in (str, int, float)]
        if invalid_props:
          [log('Invalid property value for key/value pair: %s: %s' % (k, properties[k]), detail=vverbose) for k in invalid_props]
          raise OptionError('Invalid property value in %s for keys: %s' % (option_name, str(invalid_props)))
      except ValueError as ve:
        log(traceback.format_exc(), vverbose)
        raise OptionError('Invalid JSON value for %s: %s' % (option_name, str(ve)))


def normalise_data_governance_options(options):
    tag_list = options.data_governance_custom_tags_csv.split(',') if options.data_governance_custom_tags_csv else []
    invalid_custom_tags = [_ for _ in tag_list if not is_valid_data_governance_tag(_)]
    if invalid_custom_tags:
        raise OptionError('Invalid values for --data-governance-custom-tags: %s' % invalid_custom_tags)

    verify_json_option('--data-governance-custom-properties', options.data_governance_custom_properties)


def version():
  """ Note that this function is modified in the top level Makefile """
  with open(os.path.join(os.environ.get('OFFLOAD_HOME'), 'version')) as version_file:
    return '%s-RC' % version_file.read().strip()


def license():
  """ Note that this function is modified in the top level Makefile """
  return 'LICENSE_TEXT'


def comp_ver_check(frontend_api):
  v_goe = version()
  v_ora = frontend_api.gdp_db_component_version()
  return v_goe == v_ora, v_goe, v_ora


def version_abort(check_version, frontend_api):
  match, v_goe, v_ora = comp_ver_check(frontend_api)
  if check_version and not match and v_goe[-3:] != '-RC':
    return True, v_goe, v_ora
  else:
    return False, v_goe, v_ora


def strict_version_ready(version_string):
  """ Our offload version can have -RC or -DEV (or both!) appended
      In order to use StrictVersion we need to tidy this up
      Chop off anything after (and including) a hyphen
  """
  version_string = version_string.split('-')[0] if version_string else version_string
  return version_string


def init_log(log_name):
  global log_fh
  global redis_in_error

  # Reset Redis status so we attempt to publish messages.
  redis_in_error = False

  current_log_name = standard_log_name(log_name)
  log_path = os.path.join(options.log_path, current_log_name)
  log_fh = open(log_path, 'w')

  if hasattr(options, 'dev_log') and options.dev_log:
    # Set tool (debug instrumentation) logging
    logging_params = {
      'level':   logging.getLevelName(options.dev_log_level.upper()),
      'format':  '%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
      'datefmt': '%Y-%m-%d %H:%M:%S'
    }
    if 'FILE' == options.dev_log.upper():
      dev_log_name = os.path.join(options.log_path, 'dev_%s' % current_log_name)
      logging_params['filename'] = dev_log_name
    logging.basicConfig(**logging_params)


def get_log_fh_name():
  global log_fh
  return log_fh.name if log_fh else None


def init(options_i):
  global options
  options = options_i

  if options.version:
    print(version())
    sys.exit(0)


def init_redis_execution_id(execution_id: Union[str, ExecutionId]):
  global redis_execution_id
  redis_execution_id = str(execution_id)


def get_default_location_fs_scheme(offload_target_table):
  """ Return the fs scheme (e.g. hdfs or s3a) from the tables default location
  """
  if offload_target_table.get_default_location():
    return get_scheme_from_location_uri(offload_target_table.get_default_location())
  else:
    return None


def num_location_files_enabled(offload_target):
  """ --num-location-files is ineffective for Impala offloads where the value is tied to the number of buckets """
  return False
  #return bool(offload_target != DBTYPE_IMPALA)


def normalise_storage_options(options, backend_api):
  options.storage_format = options.storage_format or backend_api.default_storage_format()
  if not backend_api.is_valid_storage_format(options.storage_format):
      raise OffloadException('--storage-format value is not valid with %s: %s' % (backend_api.backend_db_name(),
                                                                                  options.storage_format))
  options.storage_compression = backend_api.default_storage_compression(options.storage_compression,
                                                                        options.storage_format)
  if not backend_api.is_valid_storage_compression(options.storage_compression, options.storage_format):
      raise OffloadException('--storage-format value is not valid with %s: %s' % (backend_api.backend_db_name(),
                                                                                  options.storage_compression))


class BaseOperation(object):
  """ Over time OffloadOperation and PresentOperation are converging. Too risky at the moment
      to completely merge them, using this base class to centralise some code.
  """
  def __init__(self, operation_name, config, messages, repo_client=None, execution_id=None,
               max_hybrid_name_length=None, **kwargs):
    self.operation_name = operation_name
    self.execution_id = execution_id
    self.max_hybrid_name_length = max_hybrid_name_length
    self._orchestration_config = config
    self._messages = messages
    self.column_transformations = {}
    self.hwm_in_hybrid_view = None
    self.inflight_offload_predicate = None
    self.bucket_hash_method = None
    # This is a hidden partition filter we can feed into "find partition" logic. Not exposed to the user
    self.less_or_equal_value = None
    self.goe_version = strict_version_ready(version())
    self._existing_metadata = None

    self.offload_stats_method = self.offload_stats_method or orchestration_defaults.offload_stats_method_default(operation_name=operation_name)
    if self.offload_stats_method:
      self.offload_stats_method = self.offload_stats_method.upper()

    # The sorts of checks we do here do not require a backend connection: do_not_connect=True
    backend_api = backend_api_factory(config.target, config, messages, do_not_connect=True)

    if self.offload_stats_method == OFFLOAD_STATS_METHOD_COPY and not (backend_api.table_stats_get_supported() and
                                                                       backend_api.table_stats_set_supported()):
      raise OptionValueError('%s for %s backend: %s'
                             % (OFFLOAD_STATS_COPY_EXCEPTION_TEXT, backend_api.backend_db_name(),
                                self.offload_stats_method))

    if self.offload_stats_method == OFFLOAD_STATS_METHOD_COPY and self.offload_predicate:
      messages.warning('Offload stats method COPY in incompatible with predicate-based offload')
      self.offload_stats_method = OFFLOAD_STATS_METHOD_NATIVE

    self._num_buckets_max = config.num_buckets_max
    self._num_buckets_threshold = config.num_buckets_threshold

    self.max_offload_chunk_size = normalise_size_option(self.max_offload_chunk_size, binary_sizes=True,
                                                        strict_name='MAX_OFFLOAD_CHUNK_SIZE/--max-offload-chunk-size',
                                                        exc_cls=OptionValueError)

    self.max_offload_chunk_count = check_opt_is_posint('--max-offload-chunk-count', self.max_offload_chunk_count)
    if (self.max_offload_chunk_count < 1) or (self.max_offload_chunk_count > 1000):
        raise OptionValueError('Option MAX_OFFLOAD_CHUNK_COUNT/--max-offload-chunk-count must be between 1 and 1000')

    self.sort_columns_csv = self.sort_columns_csv.upper() if self.sort_columns_csv else None
    self.sort_columns = None

    normalise_less_than_options(self)
    normalise_insert_select_options(self)
    normalise_owner_table_options(self)
    normalise_storage_options(self, backend_api)

    self._repo_client = None
    if repo_client:
        self._repo_client = repo_client

    self.ext_table_name = self.table_name #TODO (GOE-2334) Remove this temporary value once metadata DB code updated.

    self.partition_names_csv = self.partition_names_csv.upper() if self.partition_names_csv else None
    self.partition_names = self.partition_names_csv.split(',') if self.partition_names_csv else []

  ###########################################################################
  # PRIVATE METHODS
  ###########################################################################

  def _gen_base_canonical_overrides(self, backend_table, max_decimal_precision=None, max_decimal_scale=None,
                                    columns_override=None):
    max_decimal_precision = max_decimal_precision or 38
    max_decimal_scale = max_decimal_scale or 38
    reference_columns = columns_override or backend_table.get_columns()
    canonical_columns = []
    if self.integer_1_columns_csv:
      canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_INTEGER_1, self.integer_1_columns_csv,
                                                                  canonical_columns, reference_columns))
    if self.integer_2_columns_csv:
      canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_INTEGER_2, self.integer_2_columns_csv,
                                                                  canonical_columns, reference_columns))
    if self.integer_4_columns_csv:
      canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_INTEGER_4, self.integer_4_columns_csv,
                                                                  canonical_columns, reference_columns))
    if self.integer_8_columns_csv:
      canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_INTEGER_8, self.integer_8_columns_csv,
                                                                  canonical_columns, reference_columns))
    if self.integer_38_columns_csv:
      canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_INTEGER_38, self.integer_38_columns_csv,
                                                                  canonical_columns, reference_columns))
    if self.date_columns_csv:
      canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_DATE, self.date_columns_csv,
                                                                  canonical_columns, reference_columns))
    if self.decimal_columns_csv_list:
      assert type(self.decimal_columns_csv_list) is list, '%s is not list' % type(self.decimal_columns_csv_list)
      assert type(self.decimal_columns_type_list) is list, '%s is not list' % type(self.decimal_columns_type_list)
      if not self.decimal_columns_type_list or len(self.decimal_columns_csv_list) != len(self.decimal_columns_type_list):
        log('--decimal-columns unbalanced list: %s' % str(self.decimal_columns_csv_list), detail=vverbose)
        log('--decimal-columns-type unbalanced list: %s' % str(self.decimal_columns_type_list), detail=vverbose)
        raise OffloadException('Unbalanced --decimal-columns, --decimal-columns-type pairs (--decimal-columns * %d, --decimal-columns-type * %d)' \
            % (len(self.decimal_columns_csv_list), len(self.decimal_columns_type_list or [])))
      for col_csv, spec_csv in zip(self.decimal_columns_csv_list, self.decimal_columns_type_list):
        if len(spec_csv.split(',')) != 2 or not re.match(r'^([1-9][0-9]?)\s*,\s*([0-9][0-9]?)$', spec_csv):
          raise OffloadException('--decimal-columns-type ' +
                                 DECIMAL_COL_TYPE_SYNTAX_TEMPLATE.format(p=max_decimal_precision, s=max_decimal_scale))
        spec = [int(num) for num in spec_csv.split(',')]
        if not valid_canonical_decimal_spec(spec[0], spec[1], max_decimal_precision, max_decimal_scale):
          raise OffloadException('--decimal-columns-type ' +
                                 DECIMAL_COL_TYPE_SYNTAX_TEMPLATE.format(p=max_decimal_precision, s=max_decimal_scale))
        if spec[0] > backend_table.max_decimal_precision():
            raise OffloadException('--decimal-columns-type precision is beyond backend maximum: %s > %s'
                                   % (spec[0], backend_table.max_decimal_precision()))
        if spec[1] > backend_table.max_decimal_scale():
          raise OffloadException('--decimal-columns-type scale is beyond backend maximum: %s > %s'
                                 % (spec[1], backend_table.max_decimal_scale()))
        canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_DECIMAL, col_csv, canonical_columns,
                                                                    reference_columns, precision=spec[0], scale=spec[1]))
    if self.timestamp_tz_columns_csv:
      canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_TIMESTAMP_TZ,
                                                                  self.timestamp_tz_columns_csv,
                                                                  canonical_columns, reference_columns,
                                                                  scale=backend_table.max_datetime_scale()))
    return canonical_columns

  def _setup_offload_step(self, messages):
      assert messages
      messages.setup_offload_step(skip=self.skip,
                                  error_before_step=self.error_before_step,
                                  error_after_step=self.error_after_step,
                                  repo_client=self.repo_client)

  def _vars(self, expected_args):
    """Return a dictionary of official attributes, none of the convenience attributes/methods we also store"""
    return {k: v for k, v in vars(self).items() if k in expected_args}

  ###########################################################################
  # PUBLIC METHODS
  ###########################################################################

  @property
  def repo_client(self):
    if self._repo_client is None:
      self._repo_client = orchestration_repo_client_factory(self._orchestration_config, self._messages,
                                                            dry_run=bool(not self._orchestration_config.execute))
    return self._repo_client

  def vars(self):
    raise NotImplementedError('vars() has not been implemented for this operation')

  def override_bucket_hash_col(self, new_bucket_hash_col, new_bucket_hash_method, messages=None):
    """ Pass messages as None to suppress any warnings/notices
    """
    upper_or_none = lambda x: x.upper() if x else x
    if self.bucket_hash_col and upper_or_none(new_bucket_hash_col) != upper_or_none(self.bucket_hash_col) and messages:
      messages.notice('Retaining bucket hash column from original offload (ignoring --bucket-hash-column)')
    self.bucket_hash_col = upper_or_none(new_bucket_hash_col)
    self.bucket_hash_method = new_bucket_hash_method

  def validate_num_buckets(self, rdbms_table, messages, offload_target, backend_table, no_auto_tuning=False):
    if not backend_table.synthetic_bucketing_supported():
      self.num_buckets = None
      return

    if self.num_buckets is None:
      self.num_buckets = orchestration_defaults.num_buckets_default()

    if type(self.num_buckets) is str:
      self.num_buckets = self.num_buckets.upper()

      if self.num_buckets == NUM_BUCKETS_AUTO:
        if no_auto_tuning:
          self.num_buckets = self.num_buckets_cap(offload_target)
        else:
          self.auto_tune_num_buckets(rdbms_table, messages, offload_target)

      if type(self.num_buckets) is str and re.search('^\d+$', str(self.num_buckets)):
        self.num_buckets = int(self.num_buckets)

    if not self.num_buckets \
    or type(self.num_buckets) is str \
    or abs(int(self.num_buckets)) != self.num_buckets:
      raise OffloadException('Invalid value specified for --num-buckets: %s' % str(self.num_buckets))

    if self.num_buckets and self.num_buckets > self._num_buckets_max and not no_auto_tuning:
      raise OffloadException('--num-buckets cannot be greater than DEFAULT_BUCKETS_MAX (%s > %s)'
                             % (str(self.num_buckets), str(self._num_buckets_max)))

  def override_num_buckets(self, num_buckets_override, messages=None):
    """ Pass messages as None to supress any warnings/notices
    """
    if self.num_buckets and num_buckets_override and self.num_buckets != num_buckets_override and messages:
      messages.notice('Retaining number of hash buckets from offloaded table (ignoring --num-buckets, using %s)' % num_buckets_override)
    self.num_buckets = num_buckets_override

  def num_buckets_cap(self, offload_target):
    """ We have an annoying chicken-and-egg option dependency between --num-buckets and --num-location-files.
        num_buckets_cap() and num_location_files_enabled() help make sense of it
    """
    if num_location_files_enabled(offload_target) and self.num_location_files is not None:
      return min(self._num_buckets_max, self.num_location_files)
    else:
      return self._num_buckets_max

  def auto_tune_num_buckets(self, rdbms_table, messages, offload_target):
    """ Decide whether to downgrade --num-buckets to 1 based on the RDBMS tables size if:
        1) not partitioned then overall size < $DEFAULT_BUCKETS_THRESHOLD
        2) partitioned then largest partition size < $DEFAULT_BUCKETS_THRESHOLD
        Otherwise leave it alone
    """
    auto_tune_reason = None
    if self.num_buckets == NUM_BUCKETS_AUTO:
      self.num_buckets = self.num_buckets_cap(offload_target)
      if num_location_files_enabled(offload_target) and self.num_location_files < self._num_buckets_max:
        auto_tune_reason = 'due to --num-location-files'
    else:
      # no auto tuning
      return

    if rdbms_table.is_partitioned():
      size = rdbms_table.get_max_partition_size()
      size_literal = 'partitions'
    else:
      size = rdbms_table.size_in_bytes
      size_literal = 'size'
    auto_tune = bool(size and (size or 0) < self._num_buckets_threshold)
    log('Checking if --num-buckets should be reduced (%s < %s): %s'
        % (size, self._num_buckets_threshold, auto_tune), vverbose)
    if auto_tune:
      auto_tune_reason = 'for table with small %s (<= %s)' % (size_literal, bytes_to_human_size(size))
      self.num_buckets = 1
    if auto_tune_reason:
      messages.notice('Using --num-buckets %s %s, override available with explicit value for --num-buckets'
                      % (self.num_buckets, auto_tune_reason))

  def validate_bucket_hash_col(self, column_names, rdbms_table, opts, messages, synthetic_bucketing_supported,
                               bucket_hash_column_supported):
    if not synthetic_bucketing_supported and not bucket_hash_column_supported:
      self.bucket_hash_col = None
      self.bucket_hash_method = None
      return

    if self.bucket_hash_col and self.bucket_hash_col.upper() not in column_names:
      raise OffloadException('Column specified for --bucket-hash-column does not exist: %s'
                             % self.bucket_hash_col.upper())

    if not self.bucket_hash_col:
      if rdbms_table:
        size = rdbms_table.get_max_partition_size() if rdbms_table.is_partitioned() else rdbms_table.size_in_bytes
        if synthetic_bucketing_supported or (bucket_hash_column_supported and (size or 0) >= self._num_buckets_threshold):
          self.bucket_hash_col = self.default_bucket_hash_col(rdbms_table, opts, messages)
          if not self.bucket_hash_col:
            raise OffloadException('Unable to select a default bucket hash column, table cannot be offloaded')
    else:
      self.bucket_hash_col = self.bucket_hash_col.upper()

  def get_hybrid_metadata(self, force=False):
    """ Get metadata for current hybrid view and add to state
        force can be used to ensure we read regardless of the reset status of the operation, but not store in state.
    """
    if not self._existing_metadata and not self.reset_backend_table:
      self._existing_metadata = OrchestrationMetadata.from_name(self.hybrid_owner, self.hybrid_name,
                                                                client=self.repo_client)
    elif force:
      return OrchestrationMetadata.from_name(self.hybrid_owner, self.hybrid_name, client=self.repo_client)
    return self._existing_metadata

  def reset_hybrid_metadata(self, execute, new_metadata):
    if execute:
      self._existing_metadata = self.get_hybrid_metadata(force=True)
    else:
      # If we're not in execute mode then we need to re-use the in-flight metadata
      self._existing_metadata = new_metadata
    return self._existing_metadata

  def enable_reset_backend_table(self):
    """ If we need to programmatically enable backend reset then we also need to drop cached hybrid metadata.
    """
    self.reset_backend_table = True
    self._existing_metadata = None

  def set_bucket_info_from_metadata(self, existing_metadata, messages):
    if existing_metadata:
      self.override_bucket_hash_col(existing_metadata.offload_bucket_column,
                                    existing_metadata.offload_bucket_method, messages)
      self.override_num_buckets(existing_metadata.offload_bucket_count, messages=messages)
    else:
      self.num_buckets = None
      self.bucket_hash_col = None
      self.bucket_hash_method = None

  def set_offload_partition_functions_from_metadata(self, existing_metadata):
    if existing_metadata and existing_metadata.offload_partition_functions:
      self.offload_partition_functions = csv_split(existing_metadata.offload_partition_functions)
    else:
      self.offload_partition_functions = None

  def set_offload_partition_functions(self, offload_partition_functions_override):
    if isinstance(offload_partition_functions_override, str):
      self.offload_partition_functions = csv_split(offload_partition_functions_override)
    else:
      self.offload_partition_functions = offload_partition_functions_override

  def defaults_for_fresh_offload(self, offload_source_table, offload_options, messages, offload_target_table):
    self.validate_partition_columns(offload_source_table.partition_columns, offload_source_table.columns,
                                    offload_target_table, messages, offload_source_table.partition_type)
    self.validate_bucket_hash_col(offload_source_table.get_column_names(), offload_source_table, offload_options,
                                  messages, offload_target_table.synthetic_bucketing_supported(),
                                  offload_target_table.bucket_hash_column_supported())
    self.validate_num_buckets(offload_source_table, messages, offload_options.target, offload_target_table)

  def defaults_for_existing_table(self, offload_options, frontend_api, messages=None):
    """ Default bucket hash column and datatype mappings from existing table
        This is required for setting up a pre-existing table and is used by
        incremental partition append and incremental update
        Pass messages as None to suppress any warnings/notices
    """
    existing_metadata = self.get_hybrid_metadata()

    if not existing_metadata:
      raise OffloadException(MISSING_HYBRID_METADATA_EXCEPTION_TEMPLATE % (self.hybrid_owner, self.hybrid_name))

    self.set_bucket_info_from_metadata(existing_metadata, messages)

    if (not self.offload_predicate or (self.offload_predicate
                                       and self.ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST)):
      # Only with --offload-predicate can we transition between INCREMENTAL_PREDICATE_TYPEs
      if self.ipa_predicate_type and self.ipa_predicate_type != existing_metadata.incremental_predicate_type:
        # We are overwriting user input with value from metadata
        raise OffloadException(IPA_PREDICATE_TYPE_CHANGE_EXCEPTION_TEXT)
      self.ipa_predicate_type = existing_metadata.incremental_predicate_type

    self.pre_offload_hybrid_metadata = existing_metadata
    self.offload_by_subpartition = bool(existing_metadata and existing_metadata.is_subpartition_offload())
    self.set_offload_partition_functions_from_metadata(existing_metadata)
    return existing_metadata

  def default_ipa_predicate_type(self, offload_source_table, messages):
    """ ipa_predicate_type and offload_by_subpartition are intertwined, it's hard to
        set one without the other. We do some defaulting here, then subpartition work
        and finally validate ipa_predicate_type makes sense.
    """
    if not self.ipa_predicate_type and not self.offload_predicate and not self.offload_by_subpartition:
      rpa_opts_set = active_data_append_options(self, partition_type=OFFLOAD_PARTITION_TYPE_RANGE)
      lpa_opts_set = active_data_append_options(self, partition_type=OFFLOAD_PARTITION_TYPE_LIST)
      if offload_source_table.partition_type == OFFLOAD_PARTITION_TYPE_LIST and lpa_opts_set:
        self.ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST
        log('Defaulting INCREMENTAL_PREDICATE_TYPE=%s due to options: %s' % (self.ipa_predicate_type, lpa_opts_set), detail=vverbose)
      elif offload_source_table.partition_type == OFFLOAD_PARTITION_TYPE_LIST and rpa_opts_set:
        unsupported_range_types = offload_source_table.unsupported_partition_data_types(partition_type_override=OFFLOAD_PARTITION_TYPE_RANGE)
        if unsupported_range_types:
          messages.debug('default_ipa_predicate_type for LIST has unsupported_range_types: %s' % str(unsupported_range_types))
        if not unsupported_range_types or not offload_source_table.offload_by_subpartition_capable():
          self.ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE
          log('Defaulting INCREMENTAL_PREDICATE_TYPE=%s due to options: %s' % (self.ipa_predicate_type, rpa_opts_set), detail=vverbose)
          # No subpartition complications so use check_ipa_predicate_type_option_conflicts to throw exception
          check_ipa_predicate_type_option_conflicts(self, rdbms_table=offload_source_table)
      elif offload_source_table.partition_type == OFFLOAD_PARTITION_TYPE_RANGE and rpa_opts_set:
        self.ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        log('Defaulting INCREMENTAL_PREDICATE_TYPE=%s due to options: %s' % (self.ipa_predicate_type, rpa_opts_set), detail=vverbose)

  def set_bucket_hash_method_for_backend(self, backend_table, new_backend_columns, messages):
    if not backend_table.synthetic_bucketing_supported():
      self.bucket_hash_method = None
      return
    backend_column = match_table_column(self.bucket_hash_col, new_backend_columns)
    method, _ = backend_table.synthetic_bucket_filter_capable_column(backend_column)
    if method:
      log('Using optimized bucket expression for to enable bucket partition pruning: %s' % method, vverbose)
      self.bucket_hash_method = method
    else:
      messages.warning('Not using optimized bucket expression for %s, consider using a different column via --bucket-hash-column' \
          % self.bucket_hash_col)

  def validate_partition_columns(self, rdbms_partition_columns, rdbms_columns, backend_table, messages,
                                 rdbms_partition_type=None, backend_columns=None):
    """ Validate offload_partition_columns and offload_partition_granularity attributes.
        Applies suitable defaults and ensures corresponding values are compatible with each other.
        backend_columns is used by join pushdown to spot when dates are offloaded as strings.
    """
    self.offload_partition_columns = validate_offload_partition_columns(
        self.offload_partition_columns, rdbms_columns, rdbms_partition_columns, rdbms_partition_type,
        self.offload_partition_functions, backend_table, messages, self.offload_chunk_column
    )

    self.offload_partition_functions = validate_offload_partition_functions(
        self.offload_partition_functions, self.offload_partition_columns, backend_table, messages
    )

    self.offload_partition_granularity = validate_offload_partition_granularity(
        self.offload_partition_granularity, self.offload_partition_columns, self.offload_partition_functions,
        rdbms_columns, rdbms_partition_columns, rdbms_partition_type, backend_table, messages,
        getattr(self, 'variable_string_columns_csv', None), backend_columns=backend_columns
    )

    if self.offload_partition_columns:
      messages.notice('Partitioning backend table by: %s' % ','.join(self.offload_partition_columns), detail=VERBOSE)
      log('Partition granularities: %s' % ','.join(self.offload_partition_granularity), detail=VVERBOSE)

  def set_partition_info_on_canonical_columns(self, canonical_columns, rdbms_columns, backend_table):
    def get_partition_info(rdbms_column):
      return offload_options_to_partition_info(self.offload_partition_columns, self.offload_partition_functions,
                                               self.offload_partition_granularity, self.offload_partition_lower_value,
                                               self.offload_partition_upper_value, self.synthetic_partition_digits,
                                               rdbms_column, backend_table)

    def get_bucket_info(rdbms_column):
      bucket_info = None
      if self.num_buckets and self.bucket_hash_col.upper() == rdbms_column.name.upper() \
      and backend_table.synthetic_bucketing_supported():
        bucket_info = ColumnBucketInfo(self.bucket_hash_col, self.num_buckets, self.bucket_hash_method)
      return bucket_info

    new_columns = []
    for canonical_column in canonical_columns:
      rdbms_column = match_table_column(canonical_column.name, rdbms_columns)
      canonical_column.partition_info = get_partition_info(rdbms_column)
      canonical_column.bucket_info = get_bucket_info(rdbms_column)
      new_columns.append(canonical_column)
    return new_columns

  def validate_ipa_predicate_type(self, offload_source_table):
    """ Check that ipa_predicate_type is valid for the offload source table.
    """
    if not self.ipa_predicate_type:
      return
    valid_combinations = [(OFFLOAD_PARTITION_TYPE_RANGE, INCREMENTAL_PREDICATE_TYPE_RANGE),
                          (OFFLOAD_PARTITION_TYPE_RANGE, INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE),
                          (OFFLOAD_PARTITION_TYPE_LIST, INCREMENTAL_PREDICATE_TYPE_LIST),
                          (OFFLOAD_PARTITION_TYPE_LIST, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE),
                          (OFFLOAD_PARTITION_TYPE_LIST, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE)]
    if self.ipa_predicate_type != INCREMENTAL_PREDICATE_TYPE_PREDICATE \
    and (offload_source_table.partition_type, self.ipa_predicate_type) not in valid_combinations:
      raise OffloadException('%s: %s/%s' % (IPA_PREDICATE_TYPE_EXCEPTION_TEXT, self.ipa_predicate_type,
                                            offload_source_table.partition_type))

    check_ipa_predicate_type_option_conflicts(self, rdbms_table=offload_source_table)

    if self.pre_offload_hybrid_metadata\
    and self.pre_offload_hybrid_metadata.incremental_predicate_type != self.ipa_predicate_type:
      # This is an incremental append offload with a modified user requested predicate type.
      # We can validate the user requested transition is valid, valid_combinations in list of pairs below:
      #   (pre-offload predicate type, newly requested predicate type)
      if self.offload_predicate:
        valid_combinations = [(INCREMENTAL_PREDICATE_TYPE_RANGE, INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE),
                              (INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE)]
      else:
        valid_combinations = [(INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE, INCREMENTAL_PREDICATE_TYPE_RANGE),
                              (INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE)]
      if (self.pre_offload_hybrid_metadata.incremental_predicate_type, self.ipa_predicate_type) not in valid_combinations:
        raise OffloadException('INCREMENTAL_PREDICATE_TYPE %s is not valid for existing %s configuration'
                               % (self.ipa_predicate_type,
                                  self.pre_offload_hybrid_metadata.incremental_predicate_type))

    # First time predicate based offloads can only be PREDICATE
    if not self.pre_offload_hybrid_metadata and self.offload_predicate\
    and self.ipa_predicate_type != INCREMENTAL_PREDICATE_TYPE_PREDICATE:
      raise OffloadException('%s: %s' % (IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT, self.ipa_predicate_type))

  def validate_offload_by_subpartition(self, offload_source_table, messages, hybrid_metadata):
    """ Method to be used for an offload to auto switch on offload_by_subpartition if sensible
        or validate correct use of --offload-by-subpartition when manually enabled.
        For IPA offloads we pickup the value from metadata and ignore user input.
    """
    allow_auto_enable = True
    if hybrid_metadata:
      allow_auto_enable = False
      self.offload_by_subpartition = bool(hybrid_metadata and hybrid_metadata.is_subpartition_offload())
      if self.offload_by_subpartition:
        log('Retaining --offload-by-subpartition from offloaded table', vverbose)

    if allow_auto_enable \
    and offload_source_table.partition_type == OFFLOAD_PARTITION_TYPE_LIST \
    and self.ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE:
        # The user has already indicated they want LIST as RANGE so no auto enabling of offload_by_subpartition
        allow_auto_enable = False

    if self.offload_by_subpartition:
      # Ensure the use of --offload-by-subpartition was valid
      if offload_source_table.offload_by_subpartition_capable():
        offload_source_table.enable_offload_by_subpartition()
        if not hybrid_metadata:
            # If this is a fresh offload then we need to default ipa_predicate_type based on subpartition level
            self.default_ipa_predicate_type(offload_source_table, messages)
      else:
        messages.warning('Ignoring --offload-by-subpartition because partition scheme is unsupported')
        self.offload_by_subpartition = False
    elif allow_auto_enable and offload_source_table.offload_by_subpartition_capable(valid_for_auto_enable=True):
      messages.debug('Auto enable of offload_by_subpartition is True, checking details...')
      # the table is capable of supporting offload_by_subpartition, now we need to check the operation makes sense
      incr_append_capable = True
      offload_type = get_offload_type(self.owner, self.table_name, self, incr_append_capable, offload_source_table.subpartition_type,
                                      hybrid_metadata, messages, with_messages=False)
      if offload_type == 'INCREMENTAL':
        messages.notice('Setting --offload-by-subpartition due to partition scheme: %s/%s'
                        % (offload_source_table.partition_type, offload_source_table.subpartition_type))
        self.offload_by_subpartition = True
        # the fn below tells offload_source_table to return subpartition scheme details in place of partition scheme details
        offload_source_table.enable_offload_by_subpartition()
        # ipa_predicate_type was set based on top level so we need to blank it out
        self.ipa_predicate_type = None
      else:
        log('Leaving --offload-by-subpartition=false due to OFFLOAD_TYPE: %s' % offload_type, vverbose)

  def validate_sort_columns(self, rdbms_column_names, messages, offload_options, backend_cols, hybrid_metadata,
                            backend_api=None, metadata_refresh=False):
    """ Default sort_columns for storage index benefit if not specified by the user.
        sort_columns_csv: The incoming option string which can be a CSV list of column names or the special token
            SORT_COLUMNS_NO_CHANGE which identifies the user has not asked for a change.
        sort_columns: A Python list of column names defined by the user.
        backend_cols: A standalone parameter because this function may be used on tables that do not yet exist.
    """

    created_api = False
    if backend_api is None:
        backend_api = backend_api_factory(offload_options.target, offload_options, messages,
                                          dry_run=bool(not offload_options.execute))
        created_api = False

    try:
        if not backend_api.sorted_table_supported():
            if self.sort_columns_csv and self.sort_columns_csv != SORT_COLUMNS_NO_CHANGE:
                # Only warn the user if they input a specific value
                messages.warning('Ignoring --sort-columns in %s version %s'
                                 % (backend_api.backend_db_name(), backend_api.target_version()))
            self.sort_columns = None
            return

        # Sorting is supported

        if hybrid_metadata == 'NONE':
            hybrid_metadata = self.get_hybrid_metadata(offload_options)

        self.sort_columns = sort_columns_csv_to_sort_columns(self.sort_columns_csv, hybrid_metadata, rdbms_column_names,
                                                             backend_cols, backend_api, metadata_refresh, messages)
    finally:
        if created_api:
            backend_api.close()


class OffloadOperation(BaseOperation):
  """ Logic and values related to the process of offloading from OffloadSourceTable to a backend system
  """

  def __init__(self, config, messages, repo_client=None, execution_id=None, max_hybrid_name_length=None, **kwargs):
    unexpected_keys = [k for k in kwargs if k not in EXPECTED_OFFLOAD_ARGS]
    assert not unexpected_keys, 'Unexpected OffloadOperation keys: %s' % unexpected_keys
    vars(self).update(kwargs)

    BaseOperation.__init__(self, OFFLOAD_OP_NAME, config, messages, repo_client=repo_client, execution_id=execution_id,
                           max_hybrid_name_length=max_hybrid_name_length)

    normalise_stats_options(self, config.target)

    self.pre_offload_hybrid_metadata = None

    normalise_offload_transport_user_options(self)
    normalise_offload_transport_method(self, config)
    normalise_offload_predicate_options(self)
    normalise_verify_options(self)
    normalise_data_sampling_options(self)

    self._setup_offload_step(messages)

  def vars(self):
    """Return a dictionary of official attributes, none of the convenience attributes/methods we also store"""
    return self._vars(EXPECTED_OFFLOAD_ARGS)

  def default_bucket_hash_col(self, source_table, opts, messages):
    """ Choose a default hashing column in order of preference below:
          1) If single column PK use that
          2) Choose most selective column based on optimiser stats
             Calculation for this is take NDV and reduce it proportionally to
             the number of NULLs in the column, then pick most selective
          3) No stats? Take first column from list of columns - and warn
    """
    return_hash_col = None

    if len(source_table.get_primary_key_columns()) == 1:
      return_hash_col = source_table.get_primary_key_columns()[0]
      messages.notice('Using primary key singleton %s as --bucket-hash-column' % return_hash_col.upper())
    elif source_table.stats_num_rows:
      return_hash_col = source_table.get_hash_bucket_candidate()
      if return_hash_col:
        messages.notice('Using selective column %s as --bucket-hash-column, based on optimizer statistics'
                        % return_hash_col.upper())

    if not return_hash_col:
      return_hash_col = source_table.get_hash_bucket_last_resort()
      if return_hash_col:
        messages.warning('Using column %s as --bucket-hash-column, in lieu of primary key and optimizer statistics'
                         % return_hash_col.upper())

    return return_hash_col

  @staticmethod
  def hybrid_owner(target_owner):
    hybrid_suffix = '_H'
    if target_owner.upper()[-2:] == hybrid_suffix:
      return target_owner.upper()
    else:
      return target_owner.upper() + hybrid_suffix

  def gen_canonical_overrides(self, backend_table, columns_override=None):
    """ For Offload """
    reference_columns = columns_override or backend_table.get_columns()
    canonical_columns = self._gen_base_canonical_overrides(backend_table, backend_table.max_decimal_precision(),
                                                           backend_table.max_decimal_scale(),
                                                           columns_override=reference_columns)
    if self.double_columns_csv:
      canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_DOUBLE, self.double_columns_csv,
                                                                  canonical_columns, reference_columns))
    if self.variable_string_columns_csv:
      canonical_columns.extend(canonical_columns_from_columns_csv(GLUENT_TYPE_VARIABLE_STRING,
                                                                  self.variable_string_columns_csv,
                                                                  canonical_columns, reference_columns))
    return canonical_columns

  @staticmethod
  def from_options(options, config, messages, repo_client=None, execution_id=None, max_hybrid_name_length=None):
      return OffloadOperation(
          config,
          messages,
          repo_client=repo_client,
          execution_id=execution_id,
          max_hybrid_name_length=max_hybrid_name_length,
          allow_decimal_scale_rounding=options.allow_decimal_scale_rounding,
          allow_floating_point_conversions=options.allow_floating_point_conversions,
          allow_nanosecond_timestamp_columns=options.allow_nanosecond_timestamp_columns,
          bucket_hash_col=options.bucket_hash_col,
          column_transformation_list=options.column_transformation_list,
          compress_load_table=options.compress_load_table,
          compute_load_table_stats=options.compute_load_table_stats,
          create_backend_db=options.create_backend_db,
          data_governance_custom_tags_csv=options.data_governance_custom_tags_csv,
          data_governance_custom_properties=options.data_governance_custom_properties,
          data_sample_parallelism=options.data_sample_parallelism,
          data_sample_pct=options.data_sample_pct,
          date_columns_csv=options.date_columns_csv,
          date_fns=options.date_fns,
          decimal_columns_csv_list=options.decimal_columns_csv_list,
          decimal_columns_type_list=options.decimal_columns_type_list,
          decimal_padding_digits=options.decimal_padding_digits,
          double_columns_csv=options.double_columns_csv,
          equal_to_values=options.equal_to_values,
          error_after_step=options.error_after_step,
          error_before_step=options.error_before_step,
          ext_readsize=options.ext_readsize,
          ext_table_name=options.ext_table_name,
          force=options.force,
          generate_dependent_views=options.generate_dependent_views,
          hive_column_stats=options.hive_column_stats,
          hybrid_ext_table_degree=options.hybrid_ext_table_degree,
          impala_insert_hint=options.impala_insert_hint,
          integer_1_columns_csv=options.integer_1_columns_csv,
          integer_2_columns_csv=options.integer_2_columns_csv,
          integer_4_columns_csv=options.integer_4_columns_csv,
          integer_8_columns_csv=options.integer_8_columns_csv,
          integer_38_columns_csv=options.integer_38_columns_csv,
          ipa_predicate_type=options.ipa_predicate_type,
          less_than_value=options.less_than_value,
          lob_data_length=options.lob_data_length,
          max_offload_chunk_count=options.max_offload_chunk_count,
          max_offload_chunk_size=options.max_offload_chunk_size,
          not_null_columns_csv=options.not_null_columns_csv,
          num_buckets=options.num_buckets,
          num_location_files=options.num_location_files,
          numeric_fns=options.numeric_fns,
          offload_by_subpartition=options.offload_by_subpartition,
          offload_chunk_column=options.offload_chunk_column,
          offload_distribute_enabled=options.offload_distribute_enabled,
          offload_fs_container=options.offload_fs_container,
          offload_fs_prefix=options.offload_fs_prefix,
          offload_fs_scheme=options.offload_fs_scheme,
          offload_partition_columns=options.offload_partition_columns,
          offload_partition_functions=options.offload_partition_functions,
          offload_partition_granularity=options.offload_partition_granularity,
          offload_partition_lower_value=options.offload_partition_lower_value,
          offload_partition_upper_value=options.offload_partition_upper_value,
          offload_stats_method=options.offload_stats_method,
          offload_transport_consistent_read=options.offload_transport_consistent_read,
          offload_transport_fetch_size=options.offload_transport_fetch_size,
          offload_transport_jvm_overrides=options.offload_transport_jvm_overrides,
          offload_transport_method=options.offload_transport_method,
          offload_transport_queue_name=options.offload_transport_queue_name,
          offload_transport_parallelism=options.offload_transport_parallelism,
          offload_transport_small_table_threshold=options.offload_transport_small_table_threshold,
          offload_transport_spark_properties=options.offload_transport_spark_properties,
          offload_transport_validation_polling_interval=options.offload_transport_validation_polling_interval,
          offload_type=options.offload_type,
          offload_predicate=options.offload_predicate,
          offload_predicate_modify_hybrid_view=options.offload_predicate_modify_hybrid_view,
          older_than_date=options.older_than_date,
          older_than_days=options.older_than_days,
          owner_table=options.owner_table,
          partition_names_csv=options.partition_names_csv,
          preserve_load_table=options.preserve_load_table,
          purge_backend_table=options.purge_backend_table,
          reset_backend_table=options.reset_backend_table,
          reset_hybrid_view=options.reset_hybrid_view,
          skip=options.skip,
          sort_columns_csv=options.sort_columns_csv,
          sqoop_additional_options=options.sqoop_additional_options,
          sqoop_mapreduce_map_memory_mb=options.sqoop_mapreduce_map_memory_mb,
          sqoop_mapreduce_map_java_opts=options.sqoop_mapreduce_map_java_opts,
          storage_format=options.storage_format,
          storage_compression = options.storage_compression,
          string_fns=options.string_fns,
          suppress_stdout=options.suppress_stdout,
          synthetic_partition_digits=options.synthetic_partition_digits,
          target_owner_name=options.target_owner_name,
          timestamp_tz_columns_csv=options.timestamp_tz_columns_csv,
          unicode_string_columns_csv=options.unicode_string_columns_csv,
          variable_string_columns_csv=options.variable_string_columns_csv,
          ver_check=options.ver_check,
          verify_row_count=options.verify_row_count,
          verify_parallelism=options.verify_parallelism,
      )

  @staticmethod
  def from_dict(operation_dict, config, messages, repo_client=None, execution_id=None, max_hybrid_name_length=None):
    unexpected_keys = [k for k in operation_dict if k not in EXPECTED_OFFLOAD_ARGS]
    assert not unexpected_keys, 'Unexpected OffloadOperation keys: %s' % unexpected_keys
    return OffloadOperation(
        config,
        messages,
        repo_client=repo_client,
        execution_id=execution_id,
        max_hybrid_name_length=max_hybrid_name_length,
        allow_decimal_scale_rounding=operation_dict.get('allow_decimal_scale_rounding',
                                                        orchestration_defaults.allow_decimal_scale_rounding_default()),
        allow_floating_point_conversions=operation_dict.get('allow_floating_point_conversions',
                                                            orchestration_defaults.allow_floating_point_conversions_default()),
        allow_nanosecond_timestamp_columns=operation_dict.get('allow_nanosecond_timestamp_columns',
                                                              orchestration_defaults.allow_nanosecond_timestamp_columns_default()),
        bucket_hash_col=operation_dict.get('bucket_hash_col'),
        column_transformation_list=operation_dict.get('column_transformation_list'),
        compress_load_table=operation_dict.get('compress_load_table',
                                               orchestration_defaults.compress_load_table_default()),
        compute_load_table_stats=operation_dict.get('compute_load_table_stats',
                                                    orchestration_defaults.compute_load_table_stats_default()),
        create_backend_db=operation_dict.get('create_backend_db', orchestration_defaults.create_backend_db_default()),
        data_governance_custom_tags_csv=operation_dict.get('data_governance_custom_tags_csv',
                                                           orchestration_defaults.data_governance_custom_tags_default()),
        data_governance_custom_properties=operation_dict.get('data_governance_custom_properties',
                                                             orchestration_defaults.data_governance_custom_properties_default()),
        data_sample_parallelism=operation_dict.get('data_sample_parallelism',
                                                   orchestration_defaults.data_sample_parallelism_default()),
        data_sample_pct=operation_dict.get('data_sample_pct', orchestration_defaults.data_sample_pct_default()),
        date_columns_csv=operation_dict.get('date_columns_csv'),
        date_fns=operation_dict.get('date_fns', orchestration_defaults.date_fns_default()),
        decimal_columns_csv_list=operation_dict.get('decimal_columns_csv_list'),
        decimal_columns_type_list=operation_dict.get('decimal_columns_type_list'),
        decimal_padding_digits=operation_dict.get('decimal_padding_digits',
                                                  orchestration_defaults.decimal_padding_digits_default()),
        double_columns_csv=operation_dict.get('double_columns_csv'),
        equal_to_values=operation_dict.get('equal_to_values'),
        error_after_step=operation_dict.get('error_after_step'),
        error_before_step=operation_dict.get('error_before_step'),
        ext_readsize=operation_dict.get('ext_readsize', orchestration_defaults.ext_readsize_default()),
        ext_table_name=operation_dict.get('ext_table_name'),
        force=operation_dict.get('force', orchestration_defaults.force_default()),
        generate_dependent_views=operation_dict.get('generate_dependent_views',
                                                    orchestration_defaults.generate_dependent_views_default()),
        hive_column_stats=operation_dict.get('hive_column_stats',
                                             orchestration_defaults.hive_column_stats_default()),
        hybrid_ext_table_degree=operation_dict.get('hybrid_ext_table_degree',
                                                   orchestration_defaults.hybrid_ext_table_degree_default()),
        impala_insert_hint=operation_dict.get('impala_insert_hint'),
        integer_1_columns_csv=operation_dict.get('integer_1_columns_csv'),
        integer_2_columns_csv=operation_dict.get('integer_2_columns_csv'),
        integer_4_columns_csv=operation_dict.get('integer_4_columns_csv'),
        integer_8_columns_csv=operation_dict.get('integer_8_columns_csv'),
        integer_38_columns_csv=operation_dict.get('integer_38_columns_csv'),
        ipa_predicate_type=operation_dict.get('ipa_predicate_type'),
        less_than_value=operation_dict.get('less_than_value'),
        lob_data_length=operation_dict.get('lob_data_length', orchestration_defaults.lob_data_length_default()),
        max_offload_chunk_count=operation_dict.get('max_offload_chunk_count',
                                                   orchestration_defaults.max_offload_chunk_count_default()),
        max_offload_chunk_size=operation_dict.get('max_offload_chunk_size',
                                                  orchestration_defaults.max_offload_chunk_size_default()),
        not_null_columns_csv=operation_dict.get('not_null_columns_csv'),
        num_buckets=operation_dict.get('num_buckets', orchestration_defaults.num_buckets_default()),
        num_location_files=operation_dict.get('num_location_files'),
        numeric_fns=operation_dict.get('numeric_fns', orchestration_defaults.numeric_fns_default()),
        offload_by_subpartition=operation_dict.get('offload_by_subpartition'),
        offload_chunk_column=operation_dict.get('offload_chunk_column'),
        offload_distribute_enabled=operation_dict.get('offload_distribute_enabled',
                                                      orchestration_defaults.offload_distribute_enabled_default()),
        # No defaults for offload_fs_ options below as they are overrides for OrchestrationConfig attributes.
        offload_fs_container=operation_dict.get('offload_fs_container'),
        offload_fs_prefix=operation_dict.get('offload_fs_prefix'),
        offload_fs_scheme=operation_dict.get('offload_fs_scheme'),
        offload_partition_columns=operation_dict.get('offload_partition_columns'),
        offload_partition_functions=operation_dict.get('offload_partition_functions'),
        offload_partition_granularity=operation_dict.get('offload_partition_granularity'),
        offload_partition_lower_value=operation_dict.get('offload_partition_lower_value'),
        offload_partition_upper_value=operation_dict.get('offload_partition_upper_value'),
        offload_stats_method=operation_dict.get('offload_stats_method',
                                                orchestration_defaults.offload_stats_method_default(
                                                    operation_name=OFFLOAD_OP_NAME)),
        offload_transport_consistent_read=operation_dict.get('offload_transport_consistent_read',
                                                             bool_option_from_string(
                                                                 'OFFLOAD_TRANSPORT_CONSISTENT_READ',
                                                                 orchestration_defaults.offload_transport_consistent_read_default())),
        offload_transport_fetch_size=operation_dict.get('offload_transport_fetch_size',
                                                        orchestration_defaults.offload_transport_fetch_size_default()),
        offload_transport_jvm_overrides=operation_dict.get('offload_transport_jvm_overrides'),
        offload_transport_queue_name=operation_dict.get('offload_transport_queue_name'),
        offload_transport_parallelism=operation_dict.get('offload_transport_parallelism',
                                                         orchestration_defaults.offload_transport_parallelism_default()),
        offload_transport_method=operation_dict.get('offload_transport_method'),
        offload_transport_small_table_threshold=operation_dict.get('offload_transport_small_table_threshold',
                                                                   orchestration_defaults.offload_transport_small_table_threshold_default()),
        offload_transport_spark_properties=operation_dict.get('offload_transport_spark_properties',
                                                              orchestration_defaults.offload_transport_spark_properties_default()),
        offload_transport_validation_polling_interval=operation_dict.get('offload_transport_validation_polling_interval',
                                                                         orchestration_defaults.offload_transport_validation_polling_interval_default()),
        offload_type=operation_dict.get('offload_type'),
        offload_predicate=operation_dict.get('offload_predicate'),
        offload_predicate_modify_hybrid_view=operation_dict.get('offload_predicate_modify_hybrid_view',
                                                                orchestration_defaults.offload_predicate_modify_hybrid_view_default()),
        older_than_date=operation_dict.get('older_than_date'),
        older_than_days=operation_dict.get('older_than_days'),
        owner_table=operation_dict.get('owner_table'),
        partition_names_csv=operation_dict.get('partition_names_csv'),
        preserve_load_table=operation_dict.get('preserve_load_table',
                                               orchestration_defaults.preserve_load_table_default()),
        purge_backend_table=operation_dict.get('purge_backend_table',
                                               orchestration_defaults.purge_backend_table_default()),
        reset_backend_table=operation_dict.get('reset_backend_table', False),
        reset_hybrid_view=operation_dict.get('reset_hybrid_view', False),
        skip=operation_dict.get('skip', orchestration_defaults.skip_default()),
        sort_columns_csv=operation_dict.get('sort_columns_csv', orchestration_defaults.sort_columns_default()),
        sqoop_additional_options=operation_dict.get('sqoop_additional_options',
                                                    orchestration_defaults.sqoop_additional_options_default()),
        sqoop_mapreduce_map_memory_mb=operation_dict.get('sqoop_mapreduce_map_memory_mb'),
        sqoop_mapreduce_map_java_opts=operation_dict.get('sqoop_mapreduce_map_java_opts'),
        storage_format=operation_dict.get('storage_format', orchestration_defaults.storage_format_default()),
        storage_compression = operation_dict.get('storage_compression',
                                                  orchestration_defaults.storage_compression_default()),
        string_fns=operation_dict.get('string_fns', orchestration_defaults.string_fns_default()),
        suppress_stdout=operation_dict.get('suppress_stdout', orchestration_defaults.suppress_stdout_default()),
        synthetic_partition_digits=operation_dict.get('synthetic_partition_digits',
                                                      orchestration_defaults.synthetic_partition_digits_default()),
        target_owner_name=operation_dict.get('target_owner_name'),
        timestamp_tz_columns_csv=operation_dict.get('timestamp_tz_columns_csv'),
        unicode_string_columns_csv=operation_dict.get('unicode_string_columns_csv'),
        variable_string_columns_csv=operation_dict.get('variable_string_columns_csv'),
        ver_check=operation_dict.get('ver_check', orchestration_defaults.ver_check_default()),
        verify_parallelism=operation_dict.get('verify_parallelism',
                                              orchestration_defaults.verify_parallelism_default()),
        verify_row_count=operation_dict.get('verify_row_count', orchestration_defaults.verify_row_count_default())
    )


def unsupported_backend_data_types(supported_backend_data_types, backend_cols=None, data_type_list=None):
  assert supported_backend_data_types and type(supported_backend_data_types) is list
  assert backend_cols or data_type_list
  if data_type_list:
    source_data_types = set(data_type.split('(')[0] for data_type in data_type_list)
  else:
    source_data_types = set(col.data_type for col in backend_cols)
  return list(source_data_types - set(supported_backend_data_types))


def date_min_max_incompatible(offload_source_table, offload_target_table):
    """ Return True if it's possible for the source data to hold dates that are incompatible
        with the target system
        Currently only looking at the min when checking this. We could add max checks to this
        in the future
    """
    if offload_source_table.min_datetime_value() < offload_target_table.min_datetime_value():
        return True
    return False


def offload_source_to_canonical_mappings(offload_source_table, offload_target_table, offload_operation, not_null_propagation, messages):
  """ Take source/RDBMS columns and translate them into intermediate canonical columns.
      This could be from:
          User specified options: canonical_overrides.
          Source schema attributes: offload_source_table.to_canonical_column().
          Data type sampling: offload_source_table.sample_rdbms_data_types().
  """
  canonical_overrides = offload_operation.gen_canonical_overrides(offload_target_table,
                                                                  columns_override=offload_source_table.columns)
  sample_candidate_columns = []
  log('Canonical overrides: {}'.format(str([(_.name, _.data_type) for _ in canonical_overrides])), detail=vverbose)
  canonical_mappings = {}
  for tab_col in offload_source_table.columns:
    new_col = offload_source_table.to_canonical_column_with_overrides(tab_col, canonical_overrides)
    canonical_mappings[tab_col.name] = new_col

    if not offload_target_table.canonical_float_supported() and (tab_col.data_type == ORACLE_TYPE_BINARY_FLOAT
                                                                 or new_col.data_type == GLUENT_TYPE_FLOAT):
      raise OffloadException('4 byte binary floating point data cannot be offloaded to this backend system: %s'
                             % tab_col.name)

    if (new_col.is_number_based() or new_col.is_date_based()) and new_col.safe_mapping is False:
      log('Sampling column because of unsafe mapping: %s' % tab_col.name, detail=vverbose)
      sample_candidate_columns.append(tab_col)

    elif (new_col.is_number_based() and tab_col.data_precision is not None
          and (tab_col.data_precision - tab_col.data_scale) > offload_target_table.max_decimal_integral_magnitude()):
      # precision - scale allows data beyond that supported by the backend system, fall back on sampling
      log('Sampling column because precision-scale > %s: %s'
          % (offload_target_table.max_decimal_integral_magnitude(), tab_col.name), detail=vverbose)
      sample_candidate_columns.append(tab_col)

    elif (new_col.is_number_based() and new_col.data_scale
          and new_col.data_scale > offload_target_table.max_decimal_scale()):
      # Scale allows data beyond that supported by the backend system, fall back on sampling.
      # Use new_col above so that any user override of the scale is taken into account.
      log('Sampling column because scale > %s: %s'
          % (offload_target_table.max_decimal_scale(), tab_col.name), detail=vverbose)
      sample_candidate_columns.append(tab_col)

    elif new_col.is_date_based() and date_min_max_incompatible(offload_source_table, offload_target_table):
      log('Sampling column because of system date boundary incompatibility: %s' % tab_col.name, detail=vverbose)
      sample_candidate_columns.append(tab_col)

  if sample_candidate_columns and offload_operation.data_sample_pct:
    # Some columns require sampling to determine a data type
    if offload_operation.data_sample_pct == DATA_SAMPLE_SIZE_AUTO:
      data_sample_pct = offload_source_table.get_suitable_sample_size()
      log('Sampling %s%% of table' % data_sample_pct, detail=verbose)
    else:
      data_sample_pct = offload_operation.data_sample_pct
    log('Sampling columns: %s' % str(get_column_names(sample_candidate_columns)), detail=verbose)
    sampled_rdbms_cols = offload_source_table.sample_rdbms_data_types(sample_candidate_columns,
                                                                      data_sample_pct,
                                                                      offload_operation.data_sample_parallelism,
                                                                      offload_target_table.min_datetime_value(),
                                                                      offload_target_table.max_decimal_integral_magnitude(),
                                                                      offload_target_table.max_decimal_scale(),
                                                                      offload_operation.allow_decimal_scale_rounding)
    if sampled_rdbms_cols:
      for tab_col in sampled_rdbms_cols:
        # Overwrite any previous canonical mapping with the post sampled version
        canonical_mappings[tab_col.name] = offload_source_table.to_canonical_column(tab_col)

    report_data_type_control_options_to_simulate_sampling(sampled_rdbms_cols, canonical_mappings, messages)

  # Process any char semantics overrides
  for col_name, char_semantics in char_semantics_override_map(offload_operation.unicode_string_columns_csv,
                                                              offload_source_table.columns).items():
      canonical_mappings[col_name].char_semantics = char_semantics
      canonical_mappings[col_name].from_override = True

  # Get a fresh list of canonical columns in order to maintain column order
  canonical_columns = [canonical_mappings[_.name] for _ in offload_source_table.columns]

  canonical_columns = apply_not_null_columns_csv(canonical_columns, offload_operation.not_null_columns_csv,
                                                 not_null_propagation, offload_source_table.get_column_names(),
                                                 messages)

  log('Canonical columns:', detail=vverbose)
  [log(str(_), detail=vverbose) for _ in canonical_columns]
  return canonical_columns


def report_data_type_control_options_to_simulate_sampling(sampled_rdbms_cols, canonical_mappings, messages):
  """ Users requested the outcome of sampling should be logged to avoid resampling if an offload is re-run
      An example of when this is useful is running an offload in preview mode and then running in execute mode,
      this will run sampling twice and in some cases can be frustrating
  """
  if not sampled_rdbms_cols:
    return

  sampled_canonical_columns = [canonical_mappings[_.name] for _ in sampled_rdbms_cols]
  canonical_types_used = sorted(list(set(_.data_type for _ in sampled_canonical_columns)))

  messages.notice('Data types were identified by sampling data for columns: ' + ', '.join(get_column_names(sampled_canonical_columns)))
  messages.debug('Canonical types used: %s' % str(canonical_types_used))
  messages.log('Sampled column/data type detail, use explicit data type controls (--*-columns) to override these values:')
  for used_type in canonical_types_used:
    if used_type == GLUENT_TYPE_DECIMAL:
      # Need to use a combination of options for each precision/scale combo
      ps_tuples = list(set([(_.data_precision, _.data_scale) for _ in sampled_canonical_columns if _.data_type == used_type]))
      messages.debug('Decimal precision/scale combinations used: %s' % str(ps_tuples))
      for p, s in ps_tuples:
        column_name_csv = ','.join(sorted([_.name for _ in sampled_canonical_columns if _.data_type == used_type and _.data_precision == p and _.data_scale == s]))
        messages.log('--decimal-columns=%s --decimal-columns-type=%s,%s' % (column_name_csv, p, s))
    else:
      column_name_csv = ','.join(sorted([_.name for _ in sampled_canonical_columns if _.data_type == used_type]))
      if used_type in BACKEND_DATA_TYPE_CONTROL_OPTIONS:
        messages.log('%s=%s' % (BACKEND_DATA_TYPE_CONTROL_OPTIONS[used_type], column_name_csv))
      else:
        messages.log('%s: %s' % (used_type, column_name_csv))


def canonical_to_rdbms_mappings(canonical_columns, rdbms_table):
  """ Take intermediate canonical columns and translate them into RDBMS columns
      rdbms_table: An rdbms table object that offers from_canonical_column()
  """
  assert canonical_columns
  assert valid_column_list(canonical_columns), invalid_column_list_message(canonical_columns)
  assert rdbms_table
  assert isinstance(rdbms_table, OffloadSourceTableInterface)

  rdbms_columns = []
  for col in canonical_columns:
    rdbms_column = rdbms_table.from_canonical_column(col)
    rdbms_columns.append(rdbms_column)
  log('Converted RDBMS columns:', detail=vverbose)
  [log(str(_), detail=vverbose) for _ in rdbms_columns]
  return rdbms_columns


def offload_type_force_and_aapd_effects(hybrid_operation, source_data_client, original_metadata, offload_source_table,
                                        hybrid_options, messages):
  if source_data_client.is_incremental_append_capable():
    original_offload_type = original_metadata.offload_type
    new_offload_type = hybrid_operation.offload_type
    if hybrid_operation.offload_by_subpartition and (original_offload_type, new_offload_type) == ('FULL', 'INCREMENTAL'):
      # Once we have switched to FULL we cannot trust the HWM with subpartition offloads,
      # what if another HWM appeared for already offloaded HWM!
      raise OffloadException(OFFLOAD_TYPE_CHANGE_FOR_SUBPART_EXCEPTION_TEXT)

    if hybrid_operation.ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST and (original_offload_type, new_offload_type) == ('FULL', 'INCREMENTAL'):
      # We're switching OFFLOAD_TYPE for a LIST table, this is tricky for the user because we don't know the correct
      # INCREMENTAL_HIGH_VALUE value for metadata/hybrid view. So best we can do is try to alert them.
      active_lpa_opts = active_data_append_options(hybrid_operation, partition_type=OFFLOAD_PARTITION_TYPE_LIST)
      if active_lpa_opts:
        messages.notice('%s, only the partitions identified by %s will be queried from offloaded data' \
            % (OFFLOAD_TYPE_CHANGE_FOR_LIST_MESSAGE_TEXT, active_lpa_opts[0]))
      else:
        raise OffloadException(OFFLOAD_TYPE_CHANGE_FOR_LIST_EXCEPTION_TEXT)

    if not hybrid_operation.force:
      new_inc_key = None
      original_inc_key = original_metadata.incremental_key
      if hybrid_operation.hwm_in_hybrid_view and source_data_client.get_incremental_high_values():
        new_inc_key = incremental_key_csv_from_part_keys(offload_source_table.partition_columns)

      original_pred_cols, new_pred_cols = None, None
      if hybrid_operation.ipa_predicate_type in INCREMENTAL_PREDICATE_TYPES_WITH_PREDICATE_IN_HV:
        original_pred_cols = original_metadata.hwm_column_names()
        new_pred_cols = hwm_column_names_from_predicates(source_data_client.get_post_offload_predicates())

      if original_offload_type != new_offload_type:
        messages.notice('Enabling force option when switching OFFLOAD_TYPE ("%s" -> "%s")'
                        % (original_offload_type, new_offload_type))
        hybrid_operation.force = True
      elif original_inc_key != new_inc_key:
        messages.notice('Enabling force option when switching INCREMENTAL_KEY ("%s" -> "%s")'
                        % (original_inc_key, new_inc_key))
        hybrid_operation.force = True
      elif original_pred_cols != new_pred_cols and not source_data_client.nothing_to_offload():
        messages.notice('Enabling force option when columns in INCREMENTAL_PREDICATE_VALUE change ("%s" -> "%s")'
                        % (original_pred_cols, new_pred_cols))
        hybrid_operation.force = True


def announce_offload_chunk(chunk, offload_operation, messages, materialize=False):
  log('')
  if materialize:
    log('Materializing chunk', ansi_code='underline')
  else:
    log('Offloading chunk', ansi_code='underline')
  offload_by_subpartition = offload_operation.offload_by_subpartition if hasattr(offload_operation, 'offload_by_subpartition') else False
  log_timestamp()
  chunk.report_partitions(offload_by_subpartition, messages)


def offload_data_to_target(data_transport_client, offload_source_table, offload_target_table, offload_operation,
                           offload_options, source_data_client, messages, data_gov_client):
  """ Offloads the data via whatever means is appropriate (including validation steps).
      Returns the number of rows offloaded, None if nothing to do (i.e. non execute mode).
  """
  def progress_message(total_partitions, chunk_partitions, todo_after_chunk):
    done_so_far = total_partitions - todo_after_chunk - chunk_partitions
    perc = float(done_so_far) / total_partitions * 100
    log('Partition progress %d%% (%d/%d)' % (int(perc), done_so_far, total_partitions), verbose)

  rows_offloaded = None
  discarded_all_partitions = False
  if source_data_client.partitions_to_offload.count() > 0:
    source_data_client.discard_partitions_to_offload_by_no_segment()
    if source_data_client.partitions_to_offload.count() == 0:
      discarded_all_partitions = True
    incremental_stats = True
  else:
    incremental_stats = False

  def transport_and_load_offload_chunk_fn(partition_chunk=None, chunk_count=0, sync=True):
    """ In-line function to de-dupe partition chunk logic that follows """
    return transport_and_load_offload_chunk(
        data_transport_client, offload_source_table, offload_target_table,
        offload_operation.execution_id, offload_operation.repo_client, messages,
        partition_chunk=partition_chunk, chunk_count=chunk_count, sync=sync,
        offload_predicate=offload_operation.inflight_offload_predicate,
        dry_run=bool(not offload_options.execute)
    )

  if discarded_all_partitions:
    log('No partitions to offload')
    # exit early, skipping any stats steps (GOE-1300)
    return 0
  elif source_data_client.partitions_to_offload.count() > 0:
    for i, (chunk, remaining) in enumerate(source_data_client.get_partitions_to_offload_chunks()):
      announce_offload_chunk(chunk, offload_operation, messages)
      progress_message(source_data_client.partitions_to_offload.count(), chunk.count(), remaining.count())
      # sync below is True when we are on the final insert (i.e. remaining partitions is empty)
      rows_imported = transport_and_load_offload_chunk_fn(partition_chunk=chunk, chunk_count=i, sync=(not remaining.count()))
      if rows_imported and rows_imported >= 0:
        rows_offloaded = (rows_offloaded or 0) + rows_imported
    progress_message(source_data_client.partitions_to_offload.count(), 0, 0)
  else:
    rows_imported = transport_and_load_offload_chunk_fn()
    if rows_imported and rows_imported >= 0:
      rows_offloaded = rows_imported

  data_governance_update_metadata_step(offload_target_table.db_name, offload_target_table.table_name,
                                       data_gov_client, messages, offload_options)

  if offload_operation.offload_stats_method in [OFFLOAD_STATS_METHOD_NATIVE, OFFLOAD_STATS_METHOD_HISTORY]:
    offload_target_table.compute_final_table_stats_step(incremental_stats)
  elif offload_operation.offload_stats_method == OFFLOAD_STATS_METHOD_COPY:
    if offload_target_table.table_stats_set_supported():
      messages.offload_step(
          command_steps.STEP_COPY_STATS_TO_BACKEND,
          lambda: copy_rdbms_stats_to_backend(offload_source_table, offload_target_table, source_data_client,
                                              offload_operation, offload_options, messages),
          execute=offload_options.execute, optional=True)
  else:
    messages.notice('No backend stats due to --offload-stats: %s' % offload_operation.offload_stats_method)
    if offload_operation.hive_column_stats and offload_options.target == DBTYPE_HIVE:
      messages.notice('Ignoring --hive-column-stats option due to --offload-stats: %s' % offload_operation.offload_stats_method)
  return rows_offloaded


def copy_rdbms_stats_to_backend(offload_source_table, offload_target_table, source_data_client, offload_operation,
                                offload_options, messages):
  """ Copy RDBMS stats from source offload table to target table
      Copying stats from RDBMS table has only been implemented for Impala engine, this can be switched on for other backends using
      BackendApi.table_stats_set_supported()
      General approach to this described in design note: https://gluent.atlassian.net/wiki/display/DEV/Hadoop+Oracle+Stats+Design+Notes
  """

  def comparision_tuple_from_hv(rdbms_hv_list, rdbms_synth_expressions):
    # trusting rdbms_synth_expressions to be in the same order as rdbms_hv_list
    # i.e. the order of the RDBMS partition keys
    if rdbms_hv_list:
      return tuple([conv_fn(hv) for (_, _, conv_fn, _), hv in zip(rdbms_synth_expressions, rdbms_hv_list)])
    else:
      return None

  def filter_for_affected_partitions_range(backend_partitions, synth_part_col_names, lower_hv_tuple, upper_hv_tuple):
    # reduce the list of all hadoop partitions down to only those affected by the offload
    partitions_affected_by_offload = []
    for partition_spec in backend_partitions:
      part_details = backend_partitions[partition_spec]
      literals_by_synth_col = {col_name: col_literal for col_name, col_literal in part_details['partition_spec']}
      part_comparision_tuple = tuple([literals_by_synth_col[synth_name.lower()] for synth_name in synth_part_col_names])
      if lower_hv_tuple and part_comparision_tuple < lower_hv_tuple:
        continue
      if upper_hv_tuple and part_comparision_tuple > upper_hv_tuple:
        continue
      partitions_affected_by_offload.append(partition_spec)
    return partitions_affected_by_offload

  def filter_for_affected_partitions_list(backend_partitions, synth_part_col_names, new_hv_tuples):
    # reduce the list of all hadoop partitions down to only those affected by the offload
    partitions_affected_by_offload = []
    for partition_spec in backend_partitions:
      part_details = backend_partitions[partition_spec]
      literals_by_synth_col = {col_name: col_literal for col_name, col_literal in part_details['partition_spec']}
      part_comparision_tuple = tuple([literals_by_synth_col[synth_name.lower()] for synth_name in synth_part_col_names])
      if part_comparision_tuple in new_hv_tuples:
        partitions_affected_by_offload.append(partition_spec)
    return partitions_affected_by_offload

  if not offload_target_table.table_stats_set_supported():
    raise OffloadException('Copy of stats to backend is not support for %s' % offload_target_table.backend_db_name())

  dry_run = bool(not offload_options.execute)
  rdbms_tab_stats = offload_source_table.table_stats
  rdbms_col_stats = rdbms_tab_stats['column_stats']
  rdbms_part_stats = rdbms_tab_stats['partition_stats']
  tab_stats = {tab_key: rdbms_tab_stats[tab_key] for tab_key in ['num_rows', 'num_bytes', 'avg_row_len']}
  rdbms_part_col_names = set(_.name.upper() for _ in offload_source_table.partition_columns) if offload_source_table.partition_columns else set()

  if dry_run and not offload_target_table.table_exists():
    log('Skipping copy stats in preview mode when backend table does not exist', verbose)
    return

  pro_rate_stats_across_all_partitions = False
  if not offload_source_table.is_partitioned() or source_data_client.partitions_to_offload.count() == 0:
    pro_rate_stats_across_all_partitions = True
    pro_rate_num_rows = rdbms_tab_stats['num_rows']
    pro_rate_size_bytes = rdbms_tab_stats['num_bytes']
    log('Pro-rate row count from RDBMS table: %s' % str(pro_rate_num_rows), vverbose)
    # Full table offloads overwrite backend side stats
    additive_stats = False
  else:
    # Partition append offloads add to existing backend side stats
    additive_stats = True
    rdbms_part_col_names = set(_.name.upper() for _ in offload_source_table.partition_columns)
    upper_fn = lambda x: x.upper() if isinstance(x, str) else x
    backend_part_col_names = get_partition_source_column_names(offload_target_table.get_columns(), conv_fn=upper_fn)
    pro_rate_num_rows = source_data_client.partitions_to_offload.row_count()
    pro_rate_size_bytes = source_data_client.partitions_to_offload.size_in_bytes()
    tab_stats['num_rows'] = pro_rate_num_rows
    tab_stats['num_bytes'] = pro_rate_size_bytes
    if not rdbms_part_col_names.issubset(backend_part_col_names):
      messages.notice('RDBMS partition scheme is not a subset of backend partition scheme, pro-rating stats across all partitions')
      # sum num_rows for all offloaded partitions
      pro_rate_stats_across_all_partitions = True
      log('Pro-rate row count from offloaded partitions: %s' % str(pro_rate_num_rows), vverbose)
    if source_data_client.get_partition_append_predicate_type() == INCREMENTAL_PREDICATE_TYPE_LIST \
        and source_data_client.partitions_to_offload.has_default_partition():
      messages.notice('Offloading DEFAULT partition means backend partitions cannot be identified, pro-rating stats across all partitions')
      pro_rate_stats_across_all_partitions = True

  backend_partitions = offload_target_table.get_table_stats_partitions()

  # table level stats
  backend_tab_stats, _ = offload_target_table.get_table_stats(as_dict=True)
  if tab_stats['num_rows'] is None:
    messages.notice('No RDBMS table stats to copy to backend')
  elif not additive_stats and max(tab_stats['num_rows'], 0) <= max(backend_tab_stats['num_rows'], 0):
    messages.notice('RDBMS table stats not copied to backend due to row count (RDBMS:%s <= %s:%s)' \
        % (tab_stats['num_rows'], offload_target_table.backend_db_name(), backend_tab_stats['num_rows']))
    ndv_cap = backend_tab_stats['num_rows']
  else:
    if additive_stats:
      log('Copying table stats (%s:%s + RDBMS:%s)' \
          % (offload_target_table.backend_db_name(), max(backend_tab_stats['num_rows'], 0), tab_stats['num_rows']), verbose)
      ndv_cap = max(backend_tab_stats['num_rows'], 0) + max(tab_stats['num_rows'], 0)
    else:
      log('Copying table stats (RDBMS:%s -> %s:%s)' \
          % (tab_stats['num_rows'], offload_target_table.backend_db_name(), backend_tab_stats['num_rows']), verbose)
      ndv_cap = tab_stats['num_rows']
    offload_target_table.set_table_stats(tab_stats, additive_stats)

  # column level stats
  if not rdbms_col_stats:
    messages.notice('No RDBMS column stats to copy to backend')
  elif max(rdbms_tab_stats['num_rows'], 0) <= max(backend_tab_stats['num_rows'], 0):
    messages.notice('RDBMS column stats not copied due to row count (RDBMS:%s <= %s:%s)' \
        % (rdbms_tab_stats['num_rows'], offload_target_table.backend_db_name(), backend_tab_stats['num_rows']))
  else:
    if offload_target_table.column_stats_set_supported():
      if additive_stats and pro_rate_num_rows and rdbms_tab_stats['num_rows']:
        # when doing incremental offloads we need to factor down num_nulls accordingly
        num_null_factor = (float(pro_rate_num_rows + max(backend_tab_stats['num_rows'], 0)) / float(rdbms_tab_stats['num_rows']))
      else:
        num_null_factor = 1
      log('Copying stats to %s columns' % len(rdbms_col_stats), verbose)
      offload_target_table.set_column_stats(rdbms_col_stats, ndv_cap, num_null_factor)
    else:
      messages.warning('Unable to copy column stats in %s v%s' % (offload_target_table.backend_db_name(), offload_target_table.target_version()))

  # partition level stats
  if pro_rate_stats_across_all_partitions:
    part_stats = []
    if tab_stats['num_rows'] is None:
      messages.notice('No RDBMS table stats to copy to backend partitions')
    else:
      target_partition_count = max(len(backend_partitions), 1)
      log('Copying stats to %s partitions%s' \
        % (len(backend_partitions), ' (0 expected in non-execute mode)' if dry_run and not backend_partitions else ''), verbose)

      for partition_spec in backend_partitions:
        part_stats.append({'partition_spec': partition_spec,
                           'num_rows': max(pro_rate_num_rows // target_partition_count, 1) if pro_rate_num_rows is not None else -1,
                           'num_bytes': max(pro_rate_size_bytes // target_partition_count, 1) if pro_rate_size_bytes is not None else -1,
                           'avg_row_len': rdbms_tab_stats['avg_row_len']})
  else:
    # Source table is partitioned so we can use partitions_to_offload to get more targeted stats
    synth_part_expressions = offload_target_table.gen_synthetic_partition_col_expressions(as_python_fns=True)

    # When comparing offloaded high values with backend partition keys where need to retain
    # the order of the partition columns from the RDBMS. For example if we offloaded a source
    # partitioned by (year, month, day) then that's how we need to compare, even if backend is
    # partitioned by (category, day, month, year, wibble).
    rdbms_only_expr = [exprs for exprs in synth_part_expressions
                       if exprs[0] != offload_bucket_name() and case_insensitive_in(exprs[3], rdbms_part_col_names)]
    rdbms_only_expr_by_rdbms_col = {exprs[3].upper(): exprs for exprs in rdbms_only_expr}
    # regenerate rdbms_only_expr in the same order as the RDBMS partition keys
    rdbms_only_expr = [rdbms_only_expr_by_rdbms_col[col_name] for col_name in rdbms_part_col_names]
    filter_synth_col_names = [col_name.lower() for col_name, _, _, _ in rdbms_only_expr]
    log('Filtering backend partitions on columns: %s' % filter_synth_col_names, vverbose)

    log('Building partition filters based on predicate type: %s' % source_data_client.get_partition_append_predicate_type(), vverbose)
    if source_data_client.get_partition_append_predicate_type() in [INCREMENTAL_PREDICATE_TYPE_RANGE, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE]:
      prior_hvs = get_prior_offloaded_hv(offload_source_table, source_data_client, offload_operation=offload_operation)
      lower_hvs = prior_hvs[1] if prior_hvs else None
      lower_hv_tuple = comparision_tuple_from_hv(lower_hvs, rdbms_only_expr)

      new_hvs = get_current_offload_hv(offload_source_table, source_data_client, offload_operation)
      upper_hvs = new_hvs[1] if new_hvs else None
      upper_hv_tuple = comparision_tuple_from_hv(upper_hvs, rdbms_only_expr)

      log('Filtering backend partitions by range: %s -> %s' % (lower_hv_tuple, upper_hv_tuple), vverbose)

      partitions_affected_by_offload = filter_for_affected_partitions_range(backend_partitions, filter_synth_col_names, lower_hv_tuple, upper_hv_tuple)
    else:
      hv_tuple = get_current_offload_hv(offload_source_table, source_data_client, offload_operation)
      new_hvs = hv_tuple[1] if hv_tuple else None
      # In the backend the list partition literals are not grouped like they may be in the RDBMS, therefore
      # we need to flatten the groups out
      new_hvs = flatten_lpa_high_values(new_hvs)
      # LIST can only have singular partition keys, we multiply this up for each HV
      hv_tuples = [comparision_tuple_from_hv([hv], rdbms_only_expr) for hv in new_hvs]

      log('Filtering backend partitions by list: %s' % str(new_hvs), detail=vverbose)

      partitions_affected_by_offload = filter_for_affected_partitions_list(backend_partitions, filter_synth_col_names, hv_tuples)

    log('Copying stats to %s partitions%s' \
      % (len(partitions_affected_by_offload), ' (0 expected in non-execute mode)' if dry_run else ''), verbose)

    # Rather than pro-rate values using offload min/max HV we could loop through RDBMS partitions being more specific. This
    # would cater for skew between partitions. However it could create confusing output with the same partitions being
    # altered several times. Plus this is intended for large tables where COMPUTE will be inefficient, for these we will
    # only offload a small number at a time therefore I decided that simplified code & screen output was preferable to more
    # granular stats
    num_rows_per_backend_partition = max(pro_rate_num_rows // max(len(partitions_affected_by_offload), 1), 2) if pro_rate_num_rows is not None else -1
    size_bytes_per_backend_partition = max(pro_rate_size_bytes // max(len(partitions_affected_by_offload), 1), 2) if pro_rate_num_rows is not None else -1
    part_stats = []
    for partition_spec in partitions_affected_by_offload:
      part_stats.append({'partition_spec': partition_spec,
                         'num_rows': num_rows_per_backend_partition,
                         'num_bytes': size_bytes_per_backend_partition,
                         'avg_row_len': rdbms_tab_stats['avg_row_len']})

  offload_target_table.set_partition_stats(part_stats, additive_stats)


def pre_op_checks(check_version, config_options, frontend_api, exc_class=OffloadException):
  if config_options.db_type == DBTYPE_ORACLE:
    abort, v_goe, v_ora = version_abort(check_version, frontend_api)
    if abort:
      raise exc_class('Mismatch between Oracle component version (' + v_ora + ') and binary version (' + v_goe + ')')

  if config_options.target != DBTYPE_BIGQUERY:
    # As of GOE-2334 we only support BigQuery as a target.
    exc_class(f'Unsupported Offload target: {config_options.target}')

  return True


def get_hdfs(config_options, messages=None, force_ssh=False):
  return get_dfs_from_options(config_options, messages=messages, force_ssh=force_ssh)


def offload_operation_logic(offload_operation, offload_source_table, offload_target_table, offload_options,
                            source_data_client, existing_metadata, messages):
  """ Logic defining what will be offloaded and what the final objects will look like
      There's a lot goes on in here but one key item to note is there are 2 distinct routes through:
      1) The table either is new or is being reset, we take on board lots of options to define the final table
      2) The table already exists and we are adding data, many options are ignored to remain consistent with the target table
  """

  if offload_operation.reset_backend_table and not offload_operation.force:
    log('Enabling force mode based on --reset-backend-table', detail=vverbose)
    offload_operation.force = True

  # Cache some source data attributes in offload_operation to carry through rest of offload logic
  offload_operation.offload_type = source_data_client.get_offload_type()
  offload_operation.hwm_in_hybrid_view = source_data_client.pred_for_90_10_in_hybrid_view()
  incr_append_capable = source_data_client.is_incremental_append_capable()

  if existing_metadata:
    offload_type_force_and_aapd_effects(offload_operation, source_data_client, existing_metadata, offload_source_table,
                                        offload_options, messages)

  if source_data_client.nothing_to_offload():
    # Drop out early
    return False

  offload_operation.column_transformations = normalise_column_transformations(offload_operation.column_transformation_list, offload_cols=offload_source_table.columns)

  if not offload_target_table.exists() or offload_operation.reset_backend_table:
    # We are creating a fresh table and therefore don't need to concern ourselves with existing structure
    canonical_columns = \
        messages.offload_step(command_steps.STEP_ANALYZE_DATA_TYPES,
                              lambda: offload_source_to_canonical_mappings(offload_source_table, offload_target_table,
                                                                           offload_operation, offload_options.not_null_propagation,
                                                                           messages),
                              execute=offload_options.execute, mandatory_step=True)

    offload_operation.defaults_for_fresh_offload(offload_source_table, offload_options, messages, offload_target_table)

    # Need to add partition information after we've tuned operation settings
    canonical_columns = offload_operation.set_partition_info_on_canonical_columns(canonical_columns,
                                                                                  offload_source_table.columns,
                                                                                  offload_target_table)
    backend_columns = offload_target_table.convert_canonical_columns_to_backend(canonical_columns)

    offload_operation.set_bucket_hash_method_for_backend(offload_target_table, backend_columns, messages)
    offload_target_table.set_columns(backend_columns)
  else:
    # The backend table already exists therefore some options should be ignored/defaulted
    existing_part_digits = derive_partition_digits(offload_target_table)
    incremental_offload_partition_overrides(offload_operation, existing_part_digits, messages)
    offload_operation.unicode_string_columns_csv = offload_target_table.derive_unicode_string_columns(as_csv=True)

    if incr_append_capable:
      if offload_operation.gen_canonical_overrides(offload_target_table, columns_override=offload_source_table.columns):
        # For incremental append offload we ignore user data type controls, let them know they are being ignored here
        messages.notice('Retaining column data types from original offload (any data type control options are ignored)')

      if offload_operation.not_null_columns_csv:
        messages.notice('Retaining NOT NULL columns from original offload (ignoring --not-null-columns)')

      if offload_options.offload_fs_scheme and offload_options.offload_fs_scheme != OFFLOAD_FS_SCHEME_INHERIT \
      and get_default_location_fs_scheme(offload_target_table) not in (offload_options.offload_fs_scheme, None):
        # If the user tried to influence the fs scheme for the table then we should tell them we're ignoring the attempt
        messages.notice(
            'Original table filesystem scheme will be used for new partitions (ignoring --offload-fs-scheme, using %s)'
            % get_default_location_fs_scheme(offload_target_table)
        )

      if existing_metadata.transformations:
        offload_operation.column_transformations = normalise_column_transformations(existing_metadata.transformations,
                                                                                    offload_cols=offload_source_table.columns)
      else:
        offload_operation.column_transformations = {}
      if offload_operation.column_transformation_list:
        messages.notice('Retaining column transformations from original offload (ignoring --transform-column)')

  offload_operation.validate_sort_columns(offload_source_table.get_column_names(), messages, offload_options,
                                          offload_target_table.get_columns(), existing_metadata,
                                          backend_api=offload_target_table.get_backend_api())
  if offload_operation.offload_transport_method:
    # There's a user defined transport method and we now have enough information to fully check it
    validate_offload_transport_method(offload_operation.offload_transport_method, offload_options,
                                      offload_operation=offload_operation, offload_source_table=offload_source_table,
                                      messages=messages)

  if offload_target_table.exists() and not offload_operation.reset_backend_table:
    if incr_append_capable:
      if source_data_client.nothing_to_offload():
        return False
    else:
      return False

  return True


def offload_backend_db_message(messages, db_type, backend_table, execute_mode):
  """ Construct messages when backend databases do not exists.
      Either throw exception in execute mode or output warnings in preview mode.
  """
  if backend_table.create_database_supported():
    message = 'Offload %s does not exist or is incomplete, please create it or include --create-backend-db option' % db_type
  else:
    message = 'Offload %s does not exist or is incomplete, please create it before re-executing command' % db_type
  if execute_mode:
    # Stop here to avoid subsequent exception further along the process
    raise OffloadException(message)
  else:
    messages.warning(message, ansi_code='red')


def check_table_structure(frontend_table, backend_table, messages):
    """ Compare frontend and backend columns by name and throw an exception if there is a mismatch.
        Ideally we would use SchemaSyncAnalyzer for this but circular dependencies prevent that for the time being.
        FIXME revisit this in the future to see if we can hook into SchemaSyncAnalyzer for comparison, see GOE-1307
    """
    frontend_cols = frontend_table.get_column_names(conv_fn=str.upper)
    backend_cols = get_column_names(backend_table.get_non_synthetic_columns(), conv_fn=str.upper)
    new_frontend_cols = sorted([_ for _ in frontend_cols if _ not in backend_cols])
    missing_frontend_cols = sorted([_ for _ in backend_cols if _ not in frontend_cols])
    if new_frontend_cols and not missing_frontend_cols:
        # There are extra columns in the source and no dropped columns, we can recommend Schema Sync
        messages.warning(dedent("""\
                                New columns detected in the source table. Use Schema Sync to resolve.
                                Recommended schema_sync command to add columns to {}:
                                    schema_sync --include {}.{} -x
                                """).format(backend_table.backend_db_name(),
                                            frontend_table.owner,
                                            frontend_table.table_name), ansi_code='red')
        raise OffloadException('{}: {}.{}'.format(OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT,
                                                  frontend_table.owner, frontend_table.table_name))
    elif missing_frontend_cols:
        # There are extra columns in the source but also dropped columns, Schema Sync cannot be used
        column_table = [(frontend_table.frontend_db_name(), backend_table.backend_db_name())]
        column_table.extend([(_, '-') for _ in new_frontend_cols])
        column_table.extend([('-', _) for _ in missing_frontend_cols])
        messages.warning(dedent("""\
                                The following column mismatches were detected between the source and backend table:
                                {}
                                """).format(format_list_for_logging(column_table, underline_char='-')), ansi_code='red')
        raise OffloadException('{}: {}.{}'.format(OFFLOAD_SCHEMA_CHECK_EXCEPTION_TEXT,
                                                  frontend_table.owner, frontend_table.table_name))


def offload_table(offload_options, offload_operation, offload_source_table, offload_target_table, messages):
  if not pre_op_checks(offload_operation.ver_check, offload_options, offload_source_table.get_frontend_api()):
    return False

  global suppress_stdout_override
  global execution_id

  execution_id = messages.get_execution_id()
  suppress_stdout_override = (True if messages.verbosity == 3 else False)

  offload_options.override_offload_dfs_config(offload_operation.offload_fs_scheme,
                                              offload_operation.offload_fs_container,
                                              offload_operation.offload_fs_prefix)
  offload_options.check_backend_support(offload_target_table.get_backend_api())
  repo_client = offload_operation.repo_client

  if offload_target_table.identifier_contains_invalid_characters(offload_operation.target_owner_name.split('.')[0]):
    messages.warning('Unsupported character(s) %s in Oracle schema name. Use --target-name to specify a compatible backend database name.'
                     % offload_target_table.identifier_contains_invalid_characters(offload_operation.target_owner_name.split('.')[0]))
    if offload_options.execute:
        raise OptionError('Unsupported character(s) in Oracle schema name.')

  if not offload_source_table.columns:
    log('No columns found for table: %s.%s' % (offload_source_table.owner, offload_source_table.table_name))
    return False

  if not offload_source_table.check_data_types_supported(offload_target_table.max_datetime_scale(),
                                                         offload_target_table.nan_supported(),
                                                         offload_operation.allow_nanosecond_timestamp_columns,
                                                         offload_operation.allow_floating_point_conversions):
    return False

  if not offload_operation.create_backend_db:
    if not offload_target_table.db_exists():
      offload_backend_db_message(messages, 'target database %s' % offload_target_table.db_name, offload_target_table,
                                 offload_options.execute)
    if not offload_target_table.staging_area_exists():
      offload_backend_db_message(messages, 'staging area', offload_target_table, offload_options.execute)

  existing_metadata = None
  if offload_target_table.exists() and not offload_operation.reset_backend_table:
    # We need to pickup defaults for an existing table here, BEFORE we start looking for data to offload (get_offload_data_manager())
    existing_metadata = offload_operation.defaults_for_existing_table(
      offload_options, offload_source_table.get_frontend_api(), messages=messages
    )
    check_table_structure(offload_source_table, offload_target_table, messages)
    offload_target_table.refresh_operational_settings(offload_operation, rdbms_columns=offload_source_table.columns)

  # Call validate_offload_by_subpartition early so we can switch it on before any partition related information is requested.
  # The ipa_predicate_type checks are intertwined with offload_by_subpartition.
  offload_operation.default_ipa_predicate_type(offload_source_table, messages)
  offload_operation.validate_offload_by_subpartition(offload_source_table, messages, existing_metadata)
  offload_operation.validate_ipa_predicate_type(offload_source_table)

  if not offload_target_table.is_valid_staging_format():
    raise OffloadException('OFFLOAD_STAGING_FORMAT is not valid for backend system %s: %s'
                           % (offload_target_table.backend_type(), offload_options.offload_staging_format))

  source_data_client = messages.offload_step(
      command_steps.STEP_FIND_OFFLOAD_DATA,
      lambda: get_offload_data_manager(offload_source_table, offload_target_table, offload_operation,
                                       offload_options, messages, existing_metadata, OFFLOAD_SOURCE_CLIENT_OFFLOAD),
      execute=offload_options.execute, mandatory_step=True
  )

  # Write ipa_predicate_type back to operation in case the source_data_client has defined a new one.
  offload_operation.ipa_predicate_type = source_data_client.get_partition_append_predicate_type()

  # source_data_client may modify offload-predicate provided as input, this variable contains those
  # changes for transport and verification.
  offload_operation.inflight_offload_predicate = source_data_client.get_inflight_offload_predicate()

  # We need to set defaults for a fresh offload here, AFTER get_offload_data_manager() has decided what type
  # of offload we'll do. This happens inside offload_operation_logic().
  if not offload_operation_logic(offload_operation, offload_source_table, offload_target_table, offload_options,
                                 source_data_client, existing_metadata, messages):
    return False

  data_gov_client = get_data_gov_client(offload_options,
                                        messages,
                                        rdbms_schema=offload_source_table.owner,
                                        source_rdbms_object_name=offload_source_table.table_name,
                                        goe_version=offload_operation.goe_version)
  offload_operation.offload_transport_method = choose_offload_transport_method(offload_operation, offload_source_table,
                                                                               offload_options, messages)
  dfs_client = get_dfs_from_options(offload_options, messages)

  # For a fresh offload we may have tuned offload_operation attributes
  offload_target_table.refresh_operational_settings(offload_operation, rdbms_columns=offload_source_table.columns,
                                                    data_gov_client=data_gov_client)

  if offload_operation.create_backend_db:
    offload_target_table.create_backend_db_step()

  if offload_operation.reset_backend_table:
    offload_target_table.drop_backend_table_step(purge=offload_operation.purge_backend_table)

  rows_offloaded = None
  pre_offload_scn = None

  # pre-offload SCN will be stored in metadata
  if offload_options.db_type == DBTYPE_ORACLE:
    pre_offload_scn = offload_source_table.get_current_scn(return_none_on_failure=True)

  create_final_backend_table(offload_target_table, offload_operation, offload_options, offload_source_table.columns)

  data_transport_client = offload_transport_factory(offload_operation.offload_transport_method,
                                                    offload_source_table,
                                                    offload_target_table,
                                                    offload_operation,
                                                    offload_options,
                                                    messages,
                                                    dfs_client)

  offload_target_table.set_final_table_casts(offload_source_table.columns,
                                              data_transport_client.get_staging_file().get_staging_columns())
  offload_target_table.setup_staging_area_step(data_transport_client.get_staging_file())

  rows_offloaded = offload_data_to_target(data_transport_client,
                                          offload_source_table,
                                          offload_target_table,
                                          offload_operation,
                                          offload_options,
                                          source_data_client,
                                          messages,
                                          data_gov_client)
  log('%s: %s' % (TOTAL_ROWS_OFFLOADED_LOG_TEXT, str(rows_offloaded)), detail=VVERBOSE)

  if not offload_operation.preserve_load_table:
    offload_target_table.cleanup_staging_area_step()

  if not existing_metadata:
    # New or reset offload. Start a new base metadata dictionary...
    base_metadata = {'OFFLOADED_OWNER': offload_source_table.owner.upper(),
                     'OFFLOADED_TABLE': offload_source_table.table_name.upper(),
                     'OFFLOAD_SCN': pre_offload_scn,
                     'OFFLOAD_VERSION': offload_operation.goe_version}
  else:
    # Re-use existing metadata as base...
    base_metadata = existing_metadata.as_dict()

  new_metadata = gen_and_save_offload_metadata(
      repo_client, messages, offload_operation, offload_options,
      incremental_key_columns=offload_source_table.partition_columns,
      incremental_high_values=source_data_client.get_incremental_high_values(),
      incremental_predicate_values=source_data_client.get_post_offload_predicates(),
      object_type=METADATA_HYBRID_VIEW,
      hadoop_owner=offload_target_table.db_name,
      hadoop_table_name=offload_target_table.table_name,
      base_metadata_dict=base_metadata
  )
  offload_operation.reset_hybrid_metadata(offload_options.execute, new_metadata)

  if offload_source_table.hybrid_schema_supported():
    raise OffloadException('Hybrid Schema is no longer supported')

  if offload_operation.verify_row_count:
    if rows_offloaded != 0:
      # Only run verification if data has been transferred
      offload_data_verification(offload_source_table, offload_target_table, offload_operation,
                                offload_options, messages, source_data_client)
    else:
      log('Skipped data verification, no data was transferred', verbose)

  return True


def get_offload_target_table(offload_operation, offload_options, messages, metadata_override=None):
  check_and_set_nls_lang(offload_options, messages)
  existing_metadata = metadata_override or offload_operation.get_hybrid_metadata()
  if existing_metadata:
    backend_table = get_backend_table_from_metadata(existing_metadata, offload_options, messages,
                                                    offload_operation=offload_operation)
    if (backend_table.db_name, backend_table.table_name) != (offload_operation.target_owner, offload_operation.target_name):
      log('Re-using backend table name from metadata: %s.%s'
          % (backend_table.db_name, backend_table.table_name), detail=vverbose)

  else:
    db_name = data_db_name(offload_operation.target_owner, offload_options)
    db_name, table_name = convert_backend_identifier_case(offload_options, db_name, offload_operation.target_name)
    if (db_name, table_name) != (offload_operation.target_owner, offload_operation.target_name):
      log(f'{ADJUSTED_BACKEND_IDENTIFIER_MESSAGE_TEXT}: {db_name}.{table_name}', detail=vverbose)
    backend_table = backend_table_factory(db_name, table_name, offload_options.target, offload_options,
                                          messages, orchestration_operation=offload_operation)
  return backend_table


def get_synthetic_partition_cols(backend_cols):
  return [col for col in backend_cols if is_synthetic_partition_column(col.name)]


def get_data_gov_client(options, messages, rdbms_schema=None, source_rdbms_object_name=None, target_rdbms_object_name=None, goe_version=None):
  if options.data_governance_api_url:
    data_gov_client = get_hadoop_data_governance_client_from_options(options, messages, dry_run=bool(not options.execute))
    data_gov_client.healthcheck_api()
    data_gov_client.cache_property_values(auto_tags_csv=options.data_governance_auto_tags_csv,
                                          custom_tags_csv=options.data_governance_custom_tags_csv,
                                          auto_properties_csv=options.data_governance_auto_properties_csv,
                                          custom_properties=options.data_governance_custom_properties,
                                          rdbms_name=get_db_unique_name(options),
                                          rdbms_schema=rdbms_schema,
                                          source_rdbms_object_name=source_rdbms_object_name,
                                          target_rdbms_object_name=target_rdbms_object_name,
                                          goe_version=goe_version)
    return data_gov_client
  return None


def list_for_option_help(opt_list):
  return '|'.join(opt_list) if opt_list else None


def check_opt_is_posint(opt_name, opt_val, exception_class=OptionValueError, allow_zero=False):
  if is_pos_int(opt_val, allow_zero=allow_zero):
    return int(opt_val)
  else:
    raise exception_class("option %s: invalid positive integer value: %s" % (opt_name, opt_val))


def check_posint(option, opt, value):
  """Type checker for a new "posint" type to define
     and enforce positive integer option values. Used
     by GluentOptionTypes class below.
  """
  return check_opt_is_posint(opt, value, exception_class=OptionValueError)


class GluentOptionTypes(Option):
  """Options type class to extend the OptParse types set
     used to define and enforce option value types (initially
     added to enforce positive integers)
  """
  TYPES = Option.TYPES + ("posint",)
  TYPE_CHECKER = copy(Option.TYPE_CHECKER)
  TYPE_CHECKER["posint"] = check_posint


def get_common_options(usage=None):
  opt = OptionParser(usage=usage, option_class=GluentOptionTypes)

  opt.add_option('-c', type="posint", help=SUPPRESS_HELP)

  opt.add_option('--version', dest='version', action='store_true', default=orchestration_defaults.version_default(),
                 help='Print version and exit')

  opt.add_option('-x', '--execute', dest='execute', action='store_true',
                 default=orchestration_defaults.execute_default(), help='Perform operations, rather than just printing')

  opt.add_option('--log-path', dest='log_path', default=orchestration_defaults.log_path_default(),
                 help=SUPPRESS_HELP)
  opt.add_option('--log-level', dest='log_level', default=orchestration_defaults.log_level_default(),
                 help=SUPPRESS_HELP)
  opt.add_option('-v', dest='verbose', action='store_true', help='Verbose output.',
                 default=orchestration_defaults.verbose_default())
  opt.add_option('--vv', dest='vverbose', action='store_true', help='More verbose output.',
                 default=orchestration_defaults.vverbose_default())
  opt.add_option('--quiet', dest='quiet', action='store_true', help='Minimal output',
                 default=orchestration_defaults.quiet_default())
  opt.add_option('--skip-steps', dest='skip', default=orchestration_defaults.skip_default(),
                 help='Skip given steps. Csv of step ids you want to skip. Step ids are found by replacing spaces with underscore. Case insensitive.')
  # For development purposes, to allow tools to be executed without upgrading database installation
  opt.add_option('--no-version-check', dest='ver_check', action='store_false',
                 default=orchestration_defaults.ver_check_default(), help=SUPPRESS_HELP)
  # For testing purposes, provide a step title to throw an exception before running the step
  opt.add_option('--error-before-step', dest='error_before_step', help=SUPPRESS_HELP)
  # For testing purposes, provide a step title to throw an exception after completing the step
  opt.add_option('--error-after-step', dest='error_after_step', help=SUPPRESS_HELP)
  # For testing purposes, provide an exception token to throw an exception
  opt.add_option('--error-on-token', dest='error_on_token', help=SUPPRESS_HELP)

  opt.add_option('--no-ansi', dest='ansi', default=orchestration_defaults.ansi_default(), action='store_false')
  opt.add_option('--suppress-stdout', dest='suppress_stdout', default=orchestration_defaults.suppress_stdout_default(),
                 help=SUPPRESS_HELP)

  return opt


def get_oracle_options(opt):
  opt.add_option('--oracle-adm-user', dest='ora_adm_user', default=orchestration_defaults.ora_adm_user_default(), help=SUPPRESS_HELP)
  opt.add_option('--oracle-adm-pass', dest='ora_adm_pass', default=orchestration_defaults.ora_adm_pass_default(), help=SUPPRESS_HELP)
  opt.add_option('--oracle-app-user', dest='ora_app_user', default=orchestration_defaults.ora_app_user_default(), help=SUPPRESS_HELP)
  opt.add_option('--oracle-app-pass', dest='ora_app_pass', default=orchestration_defaults.ora_app_pass_default(), help=SUPPRESS_HELP)
  opt.add_option('--oracle-dsn', dest='oracle_dsn', default=orchestration_defaults.oracle_dsn_default(), help=SUPPRESS_HELP)
  opt.add_option('--oracle-adm-dsn', dest='oracle_adm_dsn', default=orchestration_defaults.oracle_adm_dsn_default(), help=SUPPRESS_HELP)
  opt.add_option('--oracle-repo-user', dest='ora_repo_user', default=orchestration_defaults.ora_repo_user_default(), help=SUPPRESS_HELP)
  opt.add_option('--use-oracle-wallet', dest='use_oracle_wallet', default=orchestration_defaults.use_oracle_wallet_default(), help=SUPPRESS_HELP)
  return opt


def get_data_governance_options(opt):
  opt.add_option('--data-governance-custom-properties', dest='data_governance_custom_properties',
                 default=orchestration_defaults.data_governance_custom_properties_default(),
                 help=SUPPRESS_HELP)
  opt.add_option('--data-governance-custom-tags', dest='data_governance_custom_tags_csv',
                 default=orchestration_defaults.data_governance_custom_tags_default(),
                 help=SUPPRESS_HELP)


def get_options(usage=None, operation_name=None):
  opt = get_common_options(usage)

  opt.add_option('-t', '--table', dest='owner_table', help='Required. Owner and table-name, eg. OWNER.TABLE')
  opt.add_option('--ext-table-name', dest='ext_table_name', help=SUPPRESS_HELP)
  opt.add_option('--target', dest='target', default=orchestration_defaults.query_engine_default(),
                 help=SUPPRESS_HELP)
  opt.add_option('--target-name', dest='target_owner_name',
                 help='Override owner and/or name of created frontend or backend object as appropriate for a command. Format is OWNER.VIEW_NAME')
  opt.add_option('-f', '--force', dest='force', action='store_true', default=orchestration_defaults.force_default(),
                 help='Replace tables/views as required. Use with caution.')
  opt.add_option('--create-backend-db', dest='create_backend_db', action='store_true',
                 default=orchestration_defaults.create_backend_db_default(),
                 help='Automatically create backend databases. Either use this option, or ensure databases matching 1) the Oracle schema and 2) the Oracle schema with suffix _load already exist.')
  opt.add_option('--reset-backend-table', dest='reset_backend_table', action='store_true', default=False,
                 help='Remove backend data table. Use with caution - this will delete previously offloaded data for this table!')
  opt.add_option('--reset-hybrid-view', dest='reset_hybrid_view', action='store_true', default=False,
                 help='Reset Incremental Partition Append or Predicate-Based Offload predicates in the hybrid view.')
  opt.add_option('--purge', dest='purge_backend_table', action='store_true',
                 default=orchestration_defaults.purge_backend_table_default(),
                 help='Include PURGE operation (if appropriate for the backend system) when removing data table with --reset-backend-table. Use with caution')
  opt.add_option('--equal-to-values', dest='equal_to_values', action='append',
                 help='Offload partitions with a matching partition key list. This option can be included multiple times to match multiple partitions.')
  opt.add_option('--less-than-value', dest='less_than_value',
                 help='Offload partitions with high water mark less than this value.')
  opt.add_option('--older-than-days', dest='older_than_days',
                 help='Offload partitions older than this number of days (exclusive, ie. the boundary partition is not offloaded). Suitable for keeping data up to a certain age in the source table. Alternative to --older-than-date option.')
  opt.add_option('--older-than-date', dest='older_than_date',
                 help='Offload partitions older than this date (use YYYY-MM-DD format).')
  opt.add_option('--partition-names', dest='partition_names_csv',
                 help='CSV of RDBMS partition names to be used to derive values for --less-than-value, --older-than-date or --equal-to-values as appropriate. Specifying multiple partitions is only valid for list partitioned tables.')

  opt.add_option('--max-offload-chunk-size', dest='max_offload_chunk_size',
                 default=orchestration_defaults.max_offload_chunk_size_default(),
                 help='Restrict size of partitions offloaded per cycle. [\d.]+[MG] eg. 100M, 1G, 1.5G')
  opt.add_option('--max-offload-chunk-count', dest='max_offload_chunk_count',
                 default=orchestration_defaults.max_offload_chunk_count_default(),
                 help='Restrict number of partitions offloaded per cycle. Allowable values between 1 and 1000.')
  opt.add_option('--num-buckets', dest='num_buckets', help='Number of offload bucket partitions to create in offloaded table.',
                 default=orchestration_defaults.num_buckets_default())
  # No default for --num-location_files because we need to know the difference between NUM_LOCATION_FILES=16 and an explicit --num-location-files=16
  opt.add_option('--num-location-files', dest='num_location_files',
                 help=SUPPRESS_HELP)
  opt.add_option('--bucket-hash-column', dest='bucket_hash_col',
                 help='Column to use when calculating offload bucket, defaults to first column.')
  opt.add_option('--not-null-propagation', dest='not_null_propagation',
                 default=orchestration_defaults.not_null_propagation_default(),
                 help=SUPPRESS_HELP)

  opt.add_option('--partition-columns', dest='offload_partition_columns',
                 help='Override column used by Offload to partition backend data. Defaults to source-table partition columns')
  opt.add_option('--partition-digits', dest='synthetic_partition_digits',
                 default=orchestration_defaults.synthetic_partition_digits_default(),
                 help='Maximum digits allowed for a numeric partition value, defaults to 15')
  opt.add_option('--partition-functions', dest='offload_partition_functions',
                 help='External UDF(s) used by Offload to partition backend data.')
  opt.add_option('--partition-granularity', dest='offload_partition_granularity',
                 help='Y|M|D|\d+ partition level/granularity. Use integral size for numeric partitions. Use sub-string length for string partitions. eg. "M" partitions by Year-Month, "D" partitions by Year-Month-Day, "5000" partitions in blocks of 5000 values, "2" on a string partition key partitions using the first two characters.')
  opt.add_option('--partition-lower-value', dest='offload_partition_lower_value',
                 help='Integer value defining the lower bound of a range values used for backend integer range partitioning.')
  opt.add_option('--partition-upper-value', dest='offload_partition_upper_value',
                 help='Integer value defining the upper bound of a range values used for backend integer range partitioning.')

  opt.add_option('--storage-format', dest='storage_format',
                 help='ORC|PARQUET. Defaults to ORC when offloading to Hive',
                 default=orchestration_defaults.storage_format_default())
  opt.add_option('--storage-compression', dest='storage_compression',
                 help='Backage storage compression, valid values are: HIGH|MED|NONE|GZIP|ZLIB|SNAPPY.',
                 default=orchestration_defaults.storage_compression_default())

  opt.add_option('--ext-table-degree', dest='hybrid_ext_table_degree',
                 default=orchestration_defaults.hybrid_ext_table_degree_default(),
                 help=SUPPRESS_HELP)
  opt.add_option('--ext-table-readsize', dest='ext_readsize', default=orchestration_defaults.ext_readsize_default(),
                 help=SUPPRESS_HELP)
  opt.add_option('--lob-data-length', dest='lob_data_length', default=orchestration_defaults.lob_data_length_default(),
                 help=SUPPRESS_HELP)

  opt.add_option('--hive-column-stats', dest='hive_column_stats',
                 default=orchestration_defaults.hive_column_stats_default(), action='store_true',
                 help='Enable computation of column stats with "NATIVE" or "HISTORY" offload stats methods. Applies to Hive only')
  opt.add_option('--offload-stats', dest='offload_stats_method',
                 default=orchestration_defaults.offload_stats_method_default(operation_name=operation_name),
                 help='NATIVE|HISTORY|COPY|NONE. Method used to manage backend table stats during an Offload. NATIVE is the default. HISTORY will gather stats on all partitions without stats (applicable to Hive only and will automatically be replaced with NATIVE on Impala). COPY will copy table statistics from the RDBMS to an offloaded table (applicable to Impala only)')

  opt.add_option('--preserve-load-table', dest='preserve_load_table', action='store_true',
                 default=orchestration_defaults.preserve_load_table_default(),
                 help='Stops the load table from being dropped on completion of offload')
  # --compress-load-table can be True or False but the command line option only supports store_true, it was left
  # this way because otherwise we would break backward compatibility, users would need --compress-load-table=True
  opt.add_option('--offload-by-subpartition', dest='offload_by_subpartition', action='store_true', default=False,
                 help='Identifies that a range partitioned offload should use subpartition partition keys and high values in place of top level information')
  opt.add_option('--offload-chunk-column', dest='offload_chunk_column',
                 help='Splits load data by this column during insert from the load table to the final table. This can be used to manage memory usage')
  opt.add_option('--offload-chunk-impala-insert-hint', dest='impala_insert_hint',
                 help='SHUFFLE|NOSHUFFLE. Used to inject a hint into the insert/select moving data from load table to final destination. The absence of a value injects no hint. Impala only')

  opt.add_option('--decimal-padding-digits', dest='decimal_padding_digits', type=int,
                 default=orchestration_defaults.decimal_padding_digits_default(),
                 help='Padding to apply to precision and scale of decimals during an offload')
  opt.add_option('--sort-columns', dest='sort_columns_csv', default=orchestration_defaults.sort_columns_default(),
                 help='CSV list of sort/cluster columns to use when storing data in a backend table')
  opt.add_option('--offload-distribute-enabled', dest='offload_distribute_enabled', action='store_true',
                 default=orchestration_defaults.offload_distribute_enabled_default(),
                 help='Distribute data by partition key(s) during the final INSERT operation of an offload')

  opt.add_option('--offload-predicate', dest='offload_predicate', type=str,
                 help='Predicate used to select data to offload, e.g. --offload-predicate=\'column(product_code) = string("ABC")\'. See documentation for usage and full predicate grammar')
  opt.add_option('--no-modify-hybrid-view', dest='offload_predicate_modify_hybrid_view', action='store_false',
                 default=orchestration_defaults.offload_predicate_modify_hybrid_view_default(),
                 help='Prevent an offload predicate from being added to the boundary conditions in a hybrid view. Can only be used in conjunction with --offload-predicate for --offload-predicate-type values of %s|%s|%s|%s' % (INCREMENTAL_PREDICATE_TYPE_RANGE, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE, INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE))

  # (debug instrumentation) logger. Disabled, if not specified. "File" will direct logging messages to (log-dir)/dev_(log_name) log file (whatever log name was specified in "init_log()". "stdout" will print messages to standard output
  opt.add_option('--dev-log', dest='dev_log', help=SUPPRESS_HELP)
  # (debug instrumentation) logger level
  opt.add_option('--dev-log-level', dest='dev_log_level', default=orchestration_defaults.dev_log_level_default(),
                 help=SUPPRESS_HELP)

  opt.add_option('--no-generate-dependent-views', dest='generate_dependent_views',
                 default=orchestration_defaults.generate_dependent_views_default(), action='store_false',
                 help='Any application views dependent on offloaded data will not be re-generated in to the hybrid schema')

  opt.add_option('--transform-column', dest='column_transformation_list', action='append', help=SUPPRESS_HELP)
                #, help='Transform a column, format "column:transformation[(params)]". The transformation can be one of the following keywords: suppress, null, translate(from, to), regexp_replace(pattern, replacement)')

  opt.add_option('--numeric-fns', dest='numeric_fns', default=orchestration_defaults.numeric_fns_default(),
                 help=SUPPRESS_HELP)
  opt.add_option('--string-fns', dest='string_fns', default=orchestration_defaults.string_fns_default(),
                 help=SUPPRESS_HELP)
  opt.add_option('--date-fns', dest='date_fns', default=orchestration_defaults.date_fns_default(),
                 help=SUPPRESS_HELP)

  get_data_governance_options(opt)

  return opt


def get_options_from_list(option_list, usage=None):
  """ Get only the options that are in 'option_list'

      TODO: maxym@ 2016-12-11
      This is a somewhat ugly hack. Need to reorganize option processing at some point
  """
  parser = get_options(usage=usage)

  to_remove = []

  for opt in parser.option_list:
    opt_dest, opt_string = opt.dest, opt.get_opt_string()
    if opt_dest and opt_dest not in option_list and parser.has_option(opt_string):
      to_remove.append(opt_string)

  # Need to remove as a 2nd step, otherwise option_list iteration may malfunction
  [parser.remove_option(_) for _ in to_remove]

  return parser


def get_offload_options(opt):
  """ Options applicable to offload only """

  opt.add_option('--allow-decimal-scale-rounding', dest='allow_decimal_scale_rounding',
                 default=orchestration_defaults.allow_decimal_scale_rounding_default(), action='store_true',
                 help='Confirm it is acceptable for offload to round decimal places when loading data into a backend system')
  opt.add_option('--allow-floating-point-conversions', dest='allow_floating_point_conversions',
                 default=orchestration_defaults.allow_floating_point_conversions_default(), action='store_true',
                 help='Confirm it is acceptable for offload to convert NaN/Inf values to NULL when loading data into a backend system')
  opt.add_option('--allow-nanosecond-timestamp-columns', dest='allow_nanosecond_timestamp_columns',
                 default=orchestration_defaults.allow_nanosecond_timestamp_columns_default(),
                 action='store_true', help='Confirm it is safe to offload timestamp column with nanosecond capability')
  opt.add_option('--compress-load-table', dest='compress_load_table', action='store_true',
                 default=orchestration_defaults.compress_load_table_default(),
                 help='Compress the contents of the load table during offload')
  opt.add_option('--compute-load-table-stats', dest='compute_load_table_stats', action='store_true',
                 default=orchestration_defaults.compute_load_table_stats_default(),
                 help='Compute statistics on the load table during each offload chunk')
  opt.add_option('--data-sample-percent', dest='data_sample_pct', default=orchestration_defaults.data_sample_pct_default(),
                 help='Sample RDBMS data for columns with no precision/scale properties. 0 = no sampling')
  opt.add_option('--data-sample-parallelism', type=int, dest='data_sample_parallelism',
                 default=orchestration_defaults.data_sample_parallelism_default(),
                 help=config_descriptions.DATA_SAMPLE_PARALLELISM)
  opt.add_option('--not-null-columns', dest='not_null_columns_csv',
                 help='CSV list of columns to offload with a NOT NULL constraint')
  opt.add_option('--offload-predicate-type', dest='ipa_predicate_type',
                 help='Override the default INCREMENTAL_PREDICATE_TYPE for a partitioned table. Used to offload LIST partitioned tables using RANGE logic with --offload-predicate-type=%s or used for specialized cases of Incremental Partition Append and Predicate-Based Offload offloading' % INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE)
  opt.add_option('--offload-fs-scheme', dest='offload_fs_scheme',
                 default=orchestration_defaults.offload_fs_scheme_default(),
                 help='%s. Filesystem type for Offloaded tables' % list_for_option_help(VALID_OFFLOAD_FS_SCHEMES))
  opt.add_option('--offload-fs-prefix', dest='offload_fs_prefix',
                 help='The path with which to prefix offloaded table paths. Takes precedence over --hdfs-data when --offload-fs-scheme != "inherit"')
  opt.add_option('--offload-fs-container', dest='offload_fs_container',
                 help='A valid bucket name when offloading to cloud storage')
  opt.add_option('--offload-type', dest='offload_type',
                 help='Identifies a range partitioned offload as FULL or INCREMENTAL. FULL dictates that all data is offloaded. INCREMENTAL dictates that data up to an incremental threshold will be offloaded')

  opt.add_option('--integer-1-columns', dest='integer_1_columns_csv',
                 help='CSV list of columns to offload as a 1-byte integer (only effective for numeric columns)')
  opt.add_option('--integer-2-columns', dest='integer_2_columns_csv',
                 help='CSV list of columns to offload as a 2-byte integer (only effective for numeric columns)')
  opt.add_option('--integer-4-columns', dest='integer_4_columns_csv',
                 help='CSV list of columns to offload as a 4-byte integer (only effective for numeric columns)')
  opt.add_option('--integer-8-columns', dest='integer_8_columns_csv',
                 help='CSV list of columns to offload as a 8-byte integer (only effective for numeric columns)')
  opt.add_option('--integer-38-columns', dest='integer_38_columns_csv',
                 help='CSV list of columns to offload as a 38 digit integer (only effective for numeric columns)')

  opt.add_option('--decimal-columns', dest='decimal_columns_csv_list', action='append',
                 help='CSV list of columns to offload as DECIMAL(p,s) where "p,s" is specified in a paired --decimal-columns-type option. --decimal-columns and --decimal-columns-type allow repeat inclusion for flexible data type specification, for example "--decimal-columns-type=18,2 --decimal-columns=price,cost --decimal-columns-type=6,4 --decimal-columns=location" (only effective for numeric columns)')
  opt.add_option('--decimal-columns-type', dest='decimal_columns_type_list', action='append',
                 help='State the precision and scale of columns listed in a paired --decimal-columns option, '
                      + DECIMAL_COL_TYPE_SYNTAX_TEMPLATE.format(p='38', s='38')
                      + '. e.g. "--decimal-columns-type=18,2"')

  opt.add_option('--date-columns', dest='date_columns_csv',
                 help='CSV list of columns to offload as a date (no time element, only effective for date based columns)')

  opt.add_option('--unicode-string-columns', dest='unicode_string_columns_csv',
                 help='CSV list of columns to offload as Unicode string (only effective for string columns)')
  opt.add_option('--double-columns', dest='double_columns_csv',
                 help='CSV list of columns to offload as a double-precision floating point number (only effective for numeric columns)')
  opt.add_option('--variable-string-columns', dest='variable_string_columns_csv',
                 help='CSV list of columns to offload as a variable string type (only effective for date based columns)')
  opt.add_option('--timestamp-tz-columns', dest='timestamp_tz_columns_csv',
                 help='CSV list of columns to offload as a time zoned column (only effective for date based columns)')

  opt.add_option('--offload-transport-consistent-read', dest='offload_transport_consistent_read',
                 default=orchestration_defaults.offload_transport_consistent_read_default(),
                 help='Parallel data transport tasks should have a consistent point in time when reading RDBMS data')
  opt.add_option('--offload-transport-dsn', dest='offload_transport_dsn',
                 default=orchestration_defaults.offload_transport_dsn_default(),
                 help='DSN override for RDBMS connection during data transport.')
  opt.add_option('--offload-transport-fetch-size', dest='offload_transport_fetch_size',
                 default=orchestration_defaults.offload_transport_fetch_size_default(),
                 help='Number of records to fetch in a single batch from the RDBMS during Offload')
  opt.add_option('--offload-transport-jvm-overrides', dest='offload_transport_jvm_overrides',
                 help='JVM overrides (inserted right after "sqoop import" or "spark-submit")')
  opt.add_option('--offload-transport-method', dest='offload_transport_method',
                 choices=VALID_OFFLOAD_TRANSPORT_METHODS, help=SUPPRESS_HELP)
  opt.add_option('--offload-transport-parallelism', dest='offload_transport_parallelism',
                 default=orchestration_defaults.offload_transport_parallelism_default(),
                 help='Number of slaves to use when transporting data during an Offload.')
  opt.add_option('--offload-transport-queue-name', dest='offload_transport_queue_name',
                 help='Yarn queue name to be used for Offload transport jobs.')
  opt.add_option('--offload-transport-small-table-threshold', dest='offload_transport_small_table_threshold',
                 default=orchestration_defaults.offload_transport_small_table_threshold_default(),
                 help='Threshold above which Query Import is no longer considered the correct offload choice for non-partitioned tables. [\d.]+[MG] eg. 100M, 0.5G, 1G')
  opt.add_option('--offload-transport-spark-properties', dest='offload_transport_spark_properties',
                 default=orchestration_defaults.offload_transport_spark_properties_default(),
                 help='Override defaults for Spark configuration properties using key/value pairs in JSON format')
  opt.add_option('--offload-transport-validation-polling-interval', dest='offload_transport_validation_polling_interval',
                 default=orchestration_defaults.offload_transport_validation_polling_interval_default(),
                 help='Polling interval in seconds for validation of Spark transport row count. -1 disables retrieval of RDBMS SQL statistics. 0 disables polling resulting in a single capture of SQL statistics. A value greater than 0 polls transport SQL statistics using the specified interval')

  opt.add_option('--sqoop-additional-options', dest='sqoop_additional_options',
                 default=orchestration_defaults.sqoop_additional_options_default(),
                 help='Sqoop additional options (added to the end of the command line)')
  opt.add_option('--sqoop-mapreduce-map-memory-mb', dest='sqoop_mapreduce_map_memory_mb',
                 help='Sqoop specific setting for mapreduce.map.memory.mb')
  opt.add_option('--sqoop-mapreduce-map-java-opts', dest='sqoop_mapreduce_map_java_opts',
                 help='Sqoop specific setting for mapreduce.map.java.opts')

  opt.add_option('--no-verify', dest='verify_row_count', action='store_false')
  opt.add_option('--verify', dest='verify_row_count', choices=['minus', 'aggregate'],
                 default=orchestration_defaults.verify_row_count_default())
  opt.add_option('--verify-parallelism', dest='verify_parallelism', type=int,
                 default=orchestration_defaults.verify_parallelism_default(),
                 help=config_descriptions.VERIFY_PARALLELISM)
