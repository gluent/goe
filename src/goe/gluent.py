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
import traceback
from typing import Union

import orjson

from goe.config import config_descriptions, orchestration_defaults
from goe.config.config_validation_functions import normalise_size_option
from goe.filesystem.gluent_dfs import get_scheme_from_location_uri,\
    OFFLOAD_FS_SCHEME_INHERIT, VALID_OFFLOAD_FS_SCHEMES
from goe.filesystem.gluent_dfs_factory import get_dfs_from_options

from goe.offload.backend_api import IMPALA_SHUFFLE_HINT, IMPALA_NOSHUFFLE_HINT
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.factory.backend_table_factory import backend_table_factory, get_backend_table_from_metadata
from goe.offload.column_metadata import invalid_column_list_message, match_table_column,\
    is_synthetic_partition_column, valid_column_list, \
    GLUENT_TYPE_DECIMAL, GLUENT_TYPE_DATE, GLUENT_TYPE_DOUBLE,\
    GLUENT_TYPE_INTEGER_1, GLUENT_TYPE_INTEGER_2, GLUENT_TYPE_INTEGER_4, GLUENT_TYPE_INTEGER_8,\
    GLUENT_TYPE_INTEGER_38, \
    GLUENT_TYPE_VARIABLE_STRING, GLUENT_TYPE_TIMESTAMP_TZ
from goe.offload.offload_constants import (
    ADJUSTED_BACKEND_IDENTIFIER_MESSAGE_TEXT,
    DBTYPE_BIGQUERY, DBTYPE_IMPALA, DBTYPE_ORACLE, DBTYPE_MSSQL,
    FILE_STORAGE_COMPRESSION_CODEC_GZIP, FILE_STORAGE_COMPRESSION_CODEC_SNAPPY, FILE_STORAGE_COMPRESSION_CODEC_ZLIB,
    IPA_PREDICATE_TYPE_CHANGE_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT,
    LOG_LEVEL_INFO, LOG_LEVEL_DETAIL, LOG_LEVEL_DEBUG,
    MISSING_METADATA_EXCEPTION_TEMPLATE,
    OFFLOAD_BUCKET_NAME,
    NUM_BUCKETS_AUTO,
    OFFLOAD_STATS_METHOD_COPY,
    OFFLOAD_STATS_METHOD_NATIVE,
    OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED,
    SORT_COLUMNS_NO_CHANGE,
    TOTAL_ROWS_OFFLOADED_LOG_TEXT,
)
from goe.offload.offload_functions import convert_backend_identifier_case, data_db_name
from goe.offload.offload_source_data import get_offload_type_for_config, \
    OFFLOAD_SOURCE_CLIENT_OFFLOAD
from goe.offload.offload_source_table import OffloadSourceTableInterface, \
    OFFLOAD_PARTITION_TYPE_RANGE, OFFLOAD_PARTITION_TYPE_LIST
from goe.offload.offload_messages import OffloadMessages, VERBOSE, VVERBOSE
from goe.offload.offload_metadata_functions import gen_and_save_offload_metadata
from goe.offload.offload_validation import BackendCountValidator, CrossDbValidator,\
    build_verification_clauses
from goe.offload.offload_transport import choose_offload_transport_method, offload_transport_factory, \
    validate_offload_transport_method, \
    VALID_OFFLOAD_TRANSPORT_METHODS
from goe.offload.operation.data_type_controls import (
    DECIMAL_COL_TYPE_SYNTAX_TEMPLATE,
    canonical_columns_from_columns_csv,
    offload_source_to_canonical_mappings,
)
from goe.offload.operation.transport import (
    offload_data_to_target,
)
from goe.offload.offload import (
    OffloadException,
    OffloadOptionError,
    active_data_append_options,
    check_opt_is_posint,
    check_ipa_predicate_type_option_conflicts,
    check_table_structure,
    create_final_backend_table_step,
    drop_backend_table_step,
    get_current_offload_hv,
    get_offload_data_manager,
    get_prior_offloaded_hv,
    offload_backend_db_message,
    offload_type_force_effects,
    normalise_data_sampling_options,
    normalise_less_than_options,
    normalise_offload_predicate_options,
    normalise_stats_options,
    normalise_verify_options,
)
from goe.offload.operation.partition_controls import derive_partition_digits, offload_options_to_partition_info,\
    validate_offload_partition_columns, validate_offload_partition_functions, validate_offload_partition_granularity
from goe.offload.operation.sort_columns import sort_columns_csv_to_sort_columns
from goe.orchestration import command_steps
from goe.orchestration.execution_id import ExecutionId
from goe.persistence.factory.orchestration_repo_client_factory import orchestration_repo_client_factory
from goe.persistence.orchestration_metadata import OrchestrationMetadata, \
    INCREMENTAL_PREDICATE_TYPE_PREDICATE, INCREMENTAL_PREDICATE_TYPE_LIST, INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,\
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE, INCREMENTAL_PREDICATE_TYPE_RANGE,\
    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE

from goe.data_governance.hadoop_data_governance import get_hadoop_data_governance_client_from_options,\
    is_valid_data_governance_tag

from goe.util.misc_functions import all_int_chars, csv_split, bytes_to_human_size,\
    human_size_to_bytes, standard_log_name
from goe.util.ora_query import get_oracle_connection
from goe.util.redis_tools import RedisClient


dev_logger = logging.getLogger('gluent')

OFFLOAD_PATTERN_100_0, OFFLOAD_PATTERN_90_10, OFFLOAD_PATTERN_100_10 = list(range(3))

OFFLOAD_OP_NAME = 'offload'

CONFIG_FILE_NAME = 'offload.env'

# Used in test to identify specific warnings
HYBRID_SCHEMA_STEPS_DUE_TO_HWM_CHANGE_MESSAGE_TEXT = 'Including post transport steps due to HWM change'
NLS_LANG_MISSING_CHARACTER_SET_EXCEPTION_TEMPLATE = 'NLS_LANG value %s missing character set delimiter (.)'
OFFLOAD_STATS_COPY_EXCEPTION_TEXT = 'Invalid --offload-stats value'
RETAINING_PARTITITON_FUNCTIONS_MESSAGE_TEXT = 'Retaining partition functions from backend target'

# Config that you would expect to be different from one offload to the next
EXPECTED_OFFLOAD_ARGS = [
    'allow_decimal_scale_rounding', 'allow_floating_point_conversions', 'allow_nanosecond_timestamp_columns',
    'bucket_hash_col',
    'column_transformation_list', 'compress_load_table', 'compute_load_table_stats', 'create_backend_db',
    'data_governance_custom_tags_csv', 'data_governance_custom_properties',
    'data_sample_parallelism', 'data_sample_pct', 'date_columns_csv',
    'decimal_columns_csv_list', 'decimal_columns_type_list', 'decimal_padding_digits', 'double_columns_csv',
    'equal_to_values', 'error_before_step', 'error_after_step',
    'force',
    'hive_column_stats',
    'impala_insert_hint',
    'integer_1_columns_csv', 'integer_2_columns_csv', 'integer_4_columns_csv', 'integer_8_columns_csv',
    'integer_38_columns_csv', 'ipa_predicate_type',
    'less_than_value',
    'max_offload_chunk_count', 'max_offload_chunk_size', 'not_null_columns_csv',
    'num_buckets', 'num_location_files',
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
    'skip', 'storage_compression', 'storage_format', 'synthetic_partition_digits', 'suppress_stdout',
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
        prior_hv_tuple = get_prior_offloaded_hv(offload_source_table, source_data_client, offload_operation, messages)
        if prior_hv_tuple:
            prior_hvs = prior_hv_tuple[1]

    if offloading_open_ended_partition:
      messages.log('MAXVALUE/DEFAULT partition was offloaded therefore cannot add upper bound to verification query', detail=VVERBOSE)
    else:
      new_hv_tuple = get_current_offload_hv(offload_source_table, source_data_client, offload_operation, messages)
      if new_hv_tuple:
        new_hvs = new_hv_tuple[1]

  if offload_operation.verify_row_count == 'minus':
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
            messages.log(f'Source and {offload_target_table.backend_db_name()} table data matches: offload successful'
                         + (' (with warnings)' if messages.get_warnings() else ''),
                         ansi_code='green')
            messages.log('%s origin_rows, %s backend_rows' % (source_rows, hybrid_rows), detail=VERBOSE)
        else:
            raise OffloadException('Source and Hybrid mismatch: %s differences, %s origin_rows, %s backend_rows'
                                   % (num_diff, source_rows, hybrid_rows))
  else:
    verify_fn = lambda: verify_row_count_by_aggs(offload_source_table, offload_target_table,
                                                 offload_operation.ipa_predicate_type,
                                                 offload_options, messages,
                                                 new_hvs, prior_hvs, offload_operation.verify_parallelism,
                                                 inflight_offload_predicate=source_data_client.get_inflight_offload_predicate())
    if messages.offload_step(command_steps.STEP_VERIFY_EXPORTED_DATA,
                             verify_fn, execute=offload_options.execute):
      messages.log('Source and target table data matches: offload successful%s'
                   % (' (with warnings)' if messages.get_warnings() else ''), ansi_code='green')
    else:
      raise OffloadException('Source and target mismatch')


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
      raise OffloadOptionError('Missing transformation for column: %s' % ct)

    m = re.search(r'^([\w$#]+):([\w$#]+)(\(%s\))?$' % param_match, ct)
    if not m or len(m.groups()) != 3:
      raise OffloadOptionError('Malformed transformation: %s' % ct)

    cname = m.group(1).lower()
    transformation = m.group(2).lower()
    param_str = m.group(3)

    if not transformation.lower() in custom_transformations:
      raise OffloadOptionError('Unknown transformation for column %s: %s' % (cname, transformation))

    if offload_cols:
      match_col = match_table_column(cname, offload_cols)
    else:
      match_col = match_table_column(cname, backend_cols)

    if not match_col:
      raise OffloadOptionError('Unknown column in transformation: %s' % cname)

    if transformation in ['translate', 'regexp_replace'] and not match_col.is_string_based():
      raise OffloadOptionError('Transformation "%s" not valid for %s column' % (transformation, match_col.data_type.upper()))

    trans_params = []
    if param_str:
      # remove surrounding brackets
      param_str = re.search(r'^\((%s)\)$' % param_match, param_str).group(1)
      trans_params = csv_split(param_str)

    if custom_transformations[transformation] != len(trans_params):
      raise OffloadOptionError('Malformed transformation parameters "%s" for column "%s"' % (param_str, cname))

    column_transformations.update({cname: {'transformation': transformation, 'params': trans_params}})

  return column_transformations


def bool_option_from_string(opt_name, opt_val):
    return orchestration_defaults.bool_option_from_string(opt_name, opt_val)


def normalise_owner_table_options(options):
  if not options.owner_table or len(options.owner_table.split('.')) != 2:
    raise OffloadOptionError('Option -t or --table required in form SCHEMA.TABLENAME')

  options.owner, options.table_name = options.owner_table.split('.')

  # target/base needed for role separation
  if not hasattr(options, 'target_owner_name') or not options.target_owner_name:
    options.target_owner_name = options.owner_table

  if not hasattr(options, 'base_owner_name') or not options.base_owner_name:
    options.base_owner_name = options.owner_table

  if len(options.target_owner_name.split('.')) != 2:
    raise OffloadOptionError('Option --target-name required in form SCHEMA.TABLENAME')
  if len(options.base_owner_name.split('.')) != 2:
    raise OffloadOptionError('Option --base-name required in form SCHEMA.TABLENAME')

  options.target_owner, options.target_name = options.target_owner_name.split('.')
  # Need to maintain the upper below for backward compatibility if names pass through trunc_with_hash().
  options.base_owner, options.base_name = options.base_owner_name.upper().split('.')

  if options.base_owner_name.upper() != options.owner_table.upper() and options.target_owner.upper() != options.base_owner.upper():
    raise OffloadOptionError('The SCHEMA provided in options --target-name and --base-name must match')


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
      raise OffloadOptionError('Invalid value for --impala-insert-hint: %s' % opts.impala_insert_hint)

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
                and (re.search(r'^[\d\.]+$', options.offload_transport_validation_polling_interval)
                     or options.offload_transport_validation_polling_interval == str(
                            OFFLOAD_TRANSPORT_VALIDATION_POLLER_DISABLED))):
            options.offload_transport_validation_polling_interval = float(
                options.offload_transport_validation_polling_interval)
        elif not isinstance(options.offload_transport_validation_polling_interval, (int, float)):
            raise OffloadOptionError('Invalid value "%s" for --offload-transport-validation-polling-interval'
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

  normalise_less_than_options(options, exc_cls=OffloadOptionError)

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

  valid_combination, message = check_bucket_location_combinations(options)
  if not valid_combination:
    raise OffloadOptionError(message)

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


def verify_json_option(option_name, option_value):
    if option_value:
      try:
        properties = json.loads(option_value)

        invalid_props = [k for k, v in properties.items() if type(v) not in (str, int, float)]
        if invalid_props:
          [log('Invalid property value for key/value pair: %s: %s' % (k, properties[k]), detail=vverbose) for k in invalid_props]
          raise OffloadOptionError('Invalid property value in %s for keys: %s' % (option_name, str(invalid_props)))
      except ValueError as ve:
        log(traceback.format_exc(), vverbose)
        raise OffloadOptionError('Invalid JSON value for %s: %s' % (option_name, str(ve)))


def normalise_data_governance_options(options):
    tag_list = options.data_governance_custom_tags_csv.split(',') if options.data_governance_custom_tags_csv else []
    invalid_custom_tags = [_ for _ in tag_list if not is_valid_data_governance_tag(_)]
    if invalid_custom_tags:
        raise OffloadOptionError('Invalid values for --data-governance-custom-tags: %s' % invalid_custom_tags)

    verify_json_option('--data-governance-custom-properties', options.data_governance_custom_properties)


def version():
    """ Note that this function is modified in the top level Makefile """
    with open(os.path.join(os.environ.get('OFFLOAD_HOME'), 'version_build')) as version_file:
        return version_file.read().strip()


def license():
    """ Note that this function is modified in the top level Makefile """
    return 'LICENSE_TEXT'


def comp_ver_check(frontend_api):
    v_goe = version()
    v_ora = frontend_api.gdp_db_component_version()
    return v_goe == v_ora, v_goe, v_ora


def version_abort(check_version, frontend_api):
  match, v_goe, v_ora = comp_ver_check(frontend_api)
  if check_version and not match and "-DEV" not in v_goe:
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
                                                            dry_run=bool(not self._orchestration_config.execute),
                                                            trace_action="repo_client(OffloadOperation)")
    return self._repo_client

  def vars(self):
    raise NotImplementedError('vars() has not been implemented for this operation')

  def override_bucket_hash_col(self, new_bucket_hash_col, messages):
    """ Pass messages as None to suppress any warnings/notices
    """
    upper_or_none = lambda x: x.upper() if x else x
    if self.bucket_hash_col and upper_or_none(new_bucket_hash_col) != upper_or_none(self.bucket_hash_col):
      messages.notice('Retaining bucket hash column from original offload (ignoring --bucket-hash-column)')
    self.bucket_hash_col = upper_or_none(new_bucket_hash_col)

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
    messages.log('Checking if --num-buckets should be reduced (%s < %s): %s'
                 % (size, self._num_buckets_threshold, auto_tune), detail=VVERBOSE)
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
    """ Get metadata for current table and add to state
        force can be used to ensure we read regardless of the reset status of the operation, but not store in state.
    """
    if not self._existing_metadata and not self.reset_backend_table:
      self._existing_metadata = OrchestrationMetadata.from_name(self.owner, self.table_name,
                                                                client=self.repo_client)
    elif force:
      return OrchestrationMetadata.from_name(self.owner, self.table_name, client=self.repo_client)
    return self._existing_metadata

  def reset_hybrid_metadata(self, execute, new_metadata):
    if execute:
      self._existing_metadata = self.get_hybrid_metadata(force=True)
    else:
      # If we're not in execute mode then we need to re-use the in-flight metadata
      self._existing_metadata = new_metadata
    return self._existing_metadata

  def enable_reset_backend_table(self):
    """ If we need to programmatically enable backend reset then we also need to drop cached metadata.
    """
    self.reset_backend_table = True
    self._existing_metadata = None

  def set_bucket_info_from_metadata(self, existing_metadata, messages):
    if existing_metadata:
      self.override_bucket_hash_col(existing_metadata.offload_bucket_column, messages)
    else:
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

  def defaults_for_existing_table(self, messages):
    """ Default bucket hash column and datatype mappings from existing table
        This is required for setting up a pre-existing table and is used by
        incremental partition append and incremental update
        Pass messages as None to suppress any warnings/notices
    """
    existing_metadata = self.get_hybrid_metadata()

    if not existing_metadata:
      raise OffloadException(MISSING_METADATA_EXCEPTION_TEMPLATE % (self.owner, self.table_name))

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
        messages.log('Defaulting INCREMENTAL_PREDICATE_TYPE=%s due to options: %s' % (self.ipa_predicate_type, lpa_opts_set), detail=VVERBOSE)
      elif offload_source_table.partition_type == OFFLOAD_PARTITION_TYPE_LIST and rpa_opts_set:
        unsupported_range_types = offload_source_table.unsupported_partition_data_types(partition_type_override=OFFLOAD_PARTITION_TYPE_RANGE)
        if unsupported_range_types:
          messages.debug('default_ipa_predicate_type for LIST has unsupported_range_types: %s' % str(unsupported_range_types))
        if not unsupported_range_types or not offload_source_table.offload_by_subpartition_capable():
          self.ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE
          messages.log('Defaulting INCREMENTAL_PREDICATE_TYPE=%s due to options: %s' % (self.ipa_predicate_type, rpa_opts_set), detail=VVERBOSE)
          # No subpartition complications so use check_ipa_predicate_type_option_conflicts to throw exception
          check_ipa_predicate_type_option_conflicts(self, rdbms_table=offload_source_table)
      elif offload_source_table.partition_type == OFFLOAD_PARTITION_TYPE_RANGE and rpa_opts_set:
        self.ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        messages.log('Defaulting INCREMENTAL_PREDICATE_TYPE=%s due to options: %s' % (self.ipa_predicate_type, rpa_opts_set), detail=VVERBOSE)

  def set_bucket_hash_method_for_backend(self, backend_table, new_backend_columns, messages):
    if not backend_table.synthetic_bucketing_supported():
      self.bucket_hash_method = None
      return
    backend_column = match_table_column(self.bucket_hash_col, new_backend_columns)
    method, _ = backend_table.synthetic_bucket_filter_capable_column(backend_column)
    if method:
      messages.log('Using optimized bucket expression for to enable bucket partition pruning: %s' % method, detail=VVERBOSE)
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
      messages.log('Partition granularities: %s' % ','.join(self.offload_partition_granularity), detail=VVERBOSE)

  def set_partition_info_on_canonical_columns(self, canonical_columns, rdbms_columns, backend_table):
    def get_partition_info(rdbms_column):
      return offload_options_to_partition_info(self.offload_partition_columns, self.offload_partition_functions,
                                               self.offload_partition_granularity, self.offload_partition_lower_value,
                                               self.offload_partition_upper_value, self.synthetic_partition_digits,
                                               rdbms_column, backend_table)

    new_columns = []
    for canonical_column in canonical_columns:
      rdbms_column = match_table_column(canonical_column.name, rdbms_columns)
      canonical_column.partition_info = get_partition_info(rdbms_column)
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
        messages.log('Retaining --offload-by-subpartition from offloaded table', detail=VVERBOSE)

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
        messages.log('Leaving --offload-by-subpartition=false due to OFFLOAD_TYPE: %s' % offload_type, detail=VVERBOSE)

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
          decimal_columns_csv_list=options.decimal_columns_csv_list,
          decimal_columns_type_list=options.decimal_columns_type_list,
          decimal_padding_digits=options.decimal_padding_digits,
          double_columns_csv=options.double_columns_csv,
          equal_to_values=options.equal_to_values,
          error_after_step=options.error_after_step,
          error_before_step=options.error_before_step,
          force=options.force,
          hive_column_stats=options.hive_column_stats,
          impala_insert_hint=options.impala_insert_hint,
          integer_1_columns_csv=options.integer_1_columns_csv,
          integer_2_columns_csv=options.integer_2_columns_csv,
          integer_4_columns_csv=options.integer_4_columns_csv,
          integer_8_columns_csv=options.integer_8_columns_csv,
          integer_38_columns_csv=options.integer_38_columns_csv,
          ipa_predicate_type=options.ipa_predicate_type,
          less_than_value=options.less_than_value,
          max_offload_chunk_count=options.max_offload_chunk_count,
          max_offload_chunk_size=options.max_offload_chunk_size,
          not_null_columns_csv=options.not_null_columns_csv,
          num_buckets=options.num_buckets,
          num_location_files=options.num_location_files,
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
        decimal_columns_csv_list=operation_dict.get('decimal_columns_csv_list'),
        decimal_columns_type_list=operation_dict.get('decimal_columns_type_list'),
        decimal_padding_digits=operation_dict.get('decimal_padding_digits',
                                                  orchestration_defaults.decimal_padding_digits_default()),
        double_columns_csv=operation_dict.get('double_columns_csv'),
        equal_to_values=operation_dict.get('equal_to_values'),
        error_after_step=operation_dict.get('error_after_step'),
        error_before_step=operation_dict.get('error_before_step'),
        force=operation_dict.get('force', orchestration_defaults.force_default()),
        hive_column_stats=operation_dict.get('hive_column_stats',
                                             orchestration_defaults.hive_column_stats_default()),
        impala_insert_hint=operation_dict.get('impala_insert_hint'),
        integer_1_columns_csv=operation_dict.get('integer_1_columns_csv'),
        integer_2_columns_csv=operation_dict.get('integer_2_columns_csv'),
        integer_4_columns_csv=operation_dict.get('integer_4_columns_csv'),
        integer_8_columns_csv=operation_dict.get('integer_8_columns_csv'),
        integer_38_columns_csv=operation_dict.get('integer_38_columns_csv'),
        ipa_predicate_type=operation_dict.get('ipa_predicate_type'),
        less_than_value=operation_dict.get('less_than_value'),
        max_offload_chunk_count=operation_dict.get('max_offload_chunk_count',
                                                   orchestration_defaults.max_offload_chunk_count_default()),
        max_offload_chunk_size=operation_dict.get('max_offload_chunk_size',
                                                  orchestration_defaults.max_offload_chunk_size_default()),
        not_null_columns_csv=operation_dict.get('not_null_columns_csv'),
        num_buckets=operation_dict.get('num_buckets', orchestration_defaults.num_buckets_default()),
        num_location_files=operation_dict.get('num_location_files'),
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


def canonical_to_rdbms_mappings(canonical_columns: list, rdbms_table: OffloadSourceTableInterface):
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


def pre_op_checks(check_version, config_options, frontend_api, exc_class=OffloadException):
  if config_options.db_type == DBTYPE_ORACLE:
    abort, v_goe, v_ora = version_abort(check_version, frontend_api)
    if abort:
      raise exc_class('Mismatch between Oracle component version (' + v_ora + ') and binary version (' + v_goe + ')')

  if config_options.target != DBTYPE_BIGQUERY:
    # As of GOE-2334 we only support BigQuery as a target.
    exc_class(f'Unsupported Offload target: {config_options.target}')

  return True


def offload_operation_logic(offload_operation, offload_source_table, offload_target_table, offload_options,
                            source_data_client, existing_metadata, messages):
  """ Logic defining what will be offloaded and what the final objects will look like
      There's a lot goes on in here but one key item to note is there are 2 distinct routes through:
      1) The table either is new or is being reset, we take on board lots of options to define the final table
      2) The table already exists and we are adding data, many options are ignored to remain consistent with the target table
  """

  if offload_operation.reset_backend_table and not offload_operation.force:
      messages.log('Enabling force mode based on --reset-backend-table', detail=VVERBOSE)
      offload_operation.force = True

  # Cache some source data attributes in offload_operation to carry through rest of offload logic
  offload_operation.offload_type = source_data_client.get_offload_type()
  offload_operation.hwm_in_hybrid_view = source_data_client.pred_for_90_10_in_hybrid_view()
  incr_append_capable = source_data_client.is_incremental_append_capable()

  if existing_metadata:
      offload_type_force_effects(offload_operation, source_data_client, existing_metadata, offload_source_table, messages)

  if source_data_client.nothing_to_offload():
      # Drop out early
      return False

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
        raise OffloadOptionError('Unsupported character(s) in Oracle schema name.')

  if not offload_source_table.columns:
    messages.log('No columns found for table: %s.%s' % (offload_source_table.owner, offload_source_table.table_name))
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
    existing_metadata = offload_operation.defaults_for_existing_table(messages)
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
      drop_backend_table_step(
          offload_source_table.owner,
          offload_source_table.table_name,
          offload_target_table,
          messages,
          repo_client,
          offload_options.execute,
          purge=offload_operation.purge_backend_table
      )

  rows_offloaded = None

  pre_offload_snapshot = None
  if offload_options.db_type == DBTYPE_ORACLE:
      # Pre-offload SCN will be stored in metadata.
      pre_offload_snapshot = offload_source_table.get_current_scn(return_none_on_failure=True)

  create_final_backend_table_step(offload_target_table, offload_operation)

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
  messages.log('%s: %s' % (TOTAL_ROWS_OFFLOADED_LOG_TEXT, str(rows_offloaded)), detail=VVERBOSE)

  if not offload_operation.preserve_load_table:
    offload_target_table.cleanup_staging_area_step()

  new_metadata = gen_and_save_offload_metadata(
      repo_client,
      messages,
      offload_operation,
      offload_options,
      offload_source_table,
      offload_target_table,
      offload_source_table.partition_columns,
      source_data_client.get_incremental_high_values(),
      source_data_client.get_post_offload_predicates(),
      pre_offload_snapshot,
      existing_metadata
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
      messages.log('Skipped data verification, no data was transferred', detail=VERBOSE)

  return True


def get_offload_target_table(offload_operation, offload_options, messages, metadata_override=None):
  check_and_set_nls_lang(offload_options, messages)
  existing_metadata = metadata_override or offload_operation.get_hybrid_metadata()
  if existing_metadata:
    backend_table = get_backend_table_from_metadata(existing_metadata, offload_options, messages,
                                                    offload_operation=offload_operation)
    if (backend_table.db_name, backend_table.table_name) != (offload_operation.target_owner, offload_operation.target_name):
      messages.log('Re-using backend table name from metadata: %s.%s'
          % (backend_table.db_name, backend_table.table_name), detail=VVERBOSE)

  else:
    db_name = data_db_name(offload_operation.target_owner, offload_options)
    db_name, table_name = convert_backend_identifier_case(offload_options, db_name, offload_operation.target_name)
    if (db_name, table_name) != (offload_operation.target_owner, offload_operation.target_name):
      messages.log(f'{ADJUSTED_BACKEND_IDENTIFIER_MESSAGE_TEXT}: {db_name}.{table_name}', detail=VVERBOSE)
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
                 help='Y|M|D|\\d+ partition level/granularity. Use integral size for numeric partitions. Use sub-string length for string partitions. eg. "M" partitions by Year-Month, "D" partitions by Year-Month-Day, "5000" partitions in blocks of 5000 values, "2" on a string partition key partitions using the first two characters.')
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

  opt.add_option('--transform-column', dest='column_transformation_list', action='append', help=SUPPRESS_HELP)
                #, help='Transform a column, format "column:transformation[(params)]". The transformation can be one of the following keywords: suppress, null, translate(from, to), regexp_replace(pattern, replacement)')

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
                 help='Threshold above which Query Import is no longer considered the correct offload choice for non-partitioned tables. [\\d.]+[MG] eg. 100M, 0.5G, 1G')
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
