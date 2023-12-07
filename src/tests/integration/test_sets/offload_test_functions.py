#! /usr/bin/env python3
""" Functions used to test offload(). These are shared by many test sets which is why the module is in parent directory.
    LICENSE_TEXT
"""

import decimal
from distutils.version import StrictVersion
import math

from goe.gluent import OffloadOperation, get_log_fh, get_offload_target_table
from goe.config import orchestration_defaults
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_IMPALA, DBTYPE_HIVE, DBTYPE_SNOWFLAKE,\
    PART_COL_GRANULARITY_DAY, PART_COL_GRANULARITY_MONTH, PART_OUT_OF_RANGE
from goe.offload.offload_messages import VERBOSE, VVERBOSE, OffloadMessages
from goe.offload.offload_metadata_functions import flatten_lpa_individual_high_values,\
    INCREMENTAL_PREDICATE_TYPE_RANGE, INCREMENTAL_PREDICATE_TYPE_LIST
from goe.offload.offload_source_data import offload_source_data_factory, OFFLOAD_SOURCE_CLIENT_OFFLOAD
from goe.offload.offload_transport import MISSING_ROWS_IMPORTED_WARNING, MISSING_ROWS_SPARK_WARNING
from goe.orchestration import orchestration_constants
from goe.orchestration.orchestration_runner import OrchestrationRunner
from goe.persistence.factory.orchestration_repo_client_factory import orchestration_repo_client_factory
from tests.testlib.setup import setup_constants
from tests.testlib.test_framework.test_functions import text_in_warnings


class TestOffloadException(Exception):
    pass


def connector_sql_engine_supports_nanoseconds():
    """ We can't use backend_api for this because it is possible for Smart Connector to use a different engine. """
    return bool(orchestration_defaults.connector_sql_engine_default() in (DBTYPE_IMPALA,
                                                                          DBTYPE_HIVE,
                                                                          DBTYPE_SNOWFLAKE))


def get_offload_test_fn(base_schema, table_name, frontend_api, backend_api, orchestration_config, test_messages,
                        run_offload=True, offload_modifiers=None, config_modifiers=None, create_backend_db=True,
                        verification_mode=False, with_assertions=False):
    desc_prefix = 'Table %soffload %s' % ('verification ' if verification_mode else '', table_name)
    offload_modifiers = offload_modifiers or {}

    def test_fn():
        base = '%s.%s' % (base_schema, table_name)
        step_timings = None
        run_assertions = with_assertions

        if run_offload:
            if verification_mode:
                run_assertions = False

            offload_options = {}
            # Remove the block below when GOE-1542 resolved
            if (orchestration_config.target == DBTYPE_IMPALA
                    and table_name.lower() in ('gl_range_large_number_pos', 'gl_range_large_number_neg')
                    and not verification_mode):
                if StrictVersion(backend_api.target_version()) >= StrictVersion('3.0.0'):
                    test_messages.log('Skipping offload test of %s on Impala version %s (GOE-1542)'
                                      % (table_name, backend_api.target_version()))
                    return
            offload_options['owner_table'] = base
            offload_options['reset_backend_table'] = True
            offload_options['create_backend_db'] = create_backend_db
            offload_options['max_offload_chunk_size'] = '1G'
            offload_options['verify_row_count'] = 'minus'
            (offload_options['offload_partition_columns'], offload_options['offload_partition_granularity'],
             offload_options['offload_partition_lower_value'], offload_options['offload_partition_upper_value']) = \
                get_suitable_offload_granularity_options(base_schema, table_name, orchestration_config, test_messages)
            offload_options['partition_names_csv'] = get_suitable_offload_partition_name(base_schema, table_name,
                                                                                         orchestration_config,
                                                                                         test_messages)
            offload_options['synthetic_partition_digits'] = setup_constants.OFFLOAD_CUSTOM_DIGITS.get(table_name, '15')
            offload_options['allow_floating_point_conversions'] = True
            offload_options['allow_nanosecond_timestamp_columns'] = True
            # if you have expected failures then describe them in the dict below:
            #   keyed by table_name, item is a string to look for in the exception text
            #   e.g. expected_failed_offloads['gl_bad_dates_indy'] = 'type conversion issue in load data'
            expected_failed_offloads = {}
            if table_name in ['gl_clobs', 'gl_blobs']:
                offload_options['lob_data_length'] = '512K'
            elif table_name in ['gl_binary']:
                offload_options['lob_data_length'] = '1M'
            elif table_name == 'gl_flexible_number':
                offload_options['double_columns_csv'] = 'num'
                offload_options['allow_decimal_scale_rounding'] = True
            elif table_name == 'gl_bad_dates_indy':
                offload_options['variable_string_columns_csv'] = 'dt,ts'
            elif table_name in ['gl_exotic_chars', 'gl_exotic_chars_hash', 'gl_range_exotic_chars']:
                offload_options['unicode_string_columns_csv'] = 'words'
            elif table_name == 'gl_strings':
                offload_options['unicode_string_columns_csv'] = ','.join(
                    ['C_SHORT', 'C_SHORT_C', 'C_LONG', 'C_LONG_C', 'VC_SHORT', 'VC_SHORT_C',
                     'VC_LONG', 'VC_LONG_C', 'NC_SHORT', 'NC_LONG', 'NVC_SHORT', 'NVC_LONG'])
            offload_options.update(offload_modifiers)

            offload_messages = OffloadMessages.from_options_dict(offload_options, log_fh=get_log_fh(),
                                                                 command_type=orchestration_constants.COMMAND_OFFLOAD)
            try:
                OrchestrationRunner(config_overrides=config_modifiers,
                                    dry_run=verification_mode).offload(offload_options, reuse_log=True,
                                                                       messages_override=offload_messages)
                if table_name in expected_failed_offloads:
                    # We expect this test to fail, therefore we should never get to here
                    raise TestOffloadException(f'offload({table_name}) should have thrown an exception and didn\'t')
            except Exception as exc:
                if table_name in expected_failed_offloads and expected_failed_offloads[table_name] in str(exc):
                    # let this pass as we expect the test to fail checking casts
                    test_messages.log('Expected failure of %s' % table_name)
                    run_assertions = False
                else:
                    raise
            step_timings = offload_messages.steps

        if run_assertions:
            # We should always know how many rows were offloaded
            assert not text_in_warnings(test_messages, MISSING_ROWS_IMPORTED_WARNING), desc_prefix + ' (MISSING_ROWS_IMPORTED_WARNING)'
            assert not text_in_warnings(test_messages, MISSING_ROWS_SPARK_WARNING), desc_prefix + ' (MISSING_ROWS_SPARK_WARNING)'
        else:
            test_messages.log('Skipping assertions')
        return step_timings
    return test_fn


def get_suitable_offload_granularity_options(owner, table_name, config, messages):
    offload_source_table = OffloadSourceTable.create(owner, table_name, config, messages)
    if not offload_source_table.columns:
        raise TestOffloadException(f'Offload source table does not exist: {owner}.{table_name}')
    if offload_source_table.offload_by_subpartition_capable(valid_for_auto_enable=True):
        offload_source_table.enable_offload_by_subpartition()
    offload_partition_lower_value = None
    offload_partition_upper_value = None
    if len(offload_source_table.partition_columns or []) == 1 and offload_source_table.partition_columns[0].is_date_based():
        # There's only a single date based partition column, we'll let offload use it's default granularity
        return None, None, offload_partition_lower_value, offload_partition_upper_value
    part_col_csv = None
    gran_list = []
    for i, part_col in enumerate(offload_source_table.partition_columns or []):
        if part_col.is_date_based():
            gran_list.append(PART_COL_GRANULARITY_DAY if config.target == DBTYPE_BIGQUERY else PART_COL_GRANULARITY_MONTH)
        elif part_col.is_string_based():
            gran_list.append('1')
        elif part_col.is_number_based():
            if offload_partition_lower_value is None:
                offload_partition_lower_value = 0
            if part_col.name.upper() in ['YEAR', 'MONTH']:
                gran_list.append('1')
                if offload_partition_upper_value is None or offload_partition_upper_value < 10000:
                    offload_partition_upper_value = 10000
            elif offload_source_table.partition_type == 'LIST':
                gran_list.append('1000')
                partitions_hvs = flatten_lpa_individual_high_values([_.high_values_python
                                                                     for _ in offload_source_table.get_partitions()
                                                                     if _.high_values_python != ('DEFAULT', )],
                                                                    to_str=False)
                min_partitions_hvs = min(partitions_hvs)
                if offload_partition_lower_value is None or offload_partition_lower_value > min_partitions_hvs:
                    offload_partition_lower_value = min_partitions_hvs
                max_partitions_hvs = max(partitions_hvs)
                if offload_partition_upper_value is None or offload_partition_upper_value < max_partitions_hvs:
                    offload_partition_upper_value = max_partitions_hvs
            elif offload_source_table.partition_type == 'RANGE':
                partitions_hvs = [_.high_values_python[i]
                                  for _ in offload_source_table.get_partitions()
                                  if _.high_values_individual[i] != PART_OUT_OF_RANGE]
                # For RANGE min(partitions_hvs) is still not a min of data, so we take a few extra off
                min_partitions_hvs = decimal.Decimal(min(partitions_hvs))
                if offload_partition_lower_value is None or offload_partition_lower_value > min_partitions_hvs - 1000:
                    offload_partition_lower_value = min_partitions_hvs - 1000
                max_partitions_hvs = decimal.Decimal(max(partitions_hvs))
                if offload_partition_upper_value is None or offload_partition_upper_value < max_partitions_hvs:
                    offload_partition_upper_value = max_partitions_hvs
                # Try and pick a granularity matching no more than 1000 values
                granularity_magnitude = math.floor(math.log10(max(math.floor(max_partitions_hvs / 1000), 100)))
                gran_list.append(str(10**granularity_magnitude))
            else:
                gran_list.append('1000')
                if offload_partition_upper_value:
                    offload_partition_upper_value = max(offload_partition_upper_value, 1000000)
                else:
                    offload_partition_upper_value = 1000000
    if config.target == DBTYPE_BIGQUERY and len(gran_list) > 1:
        # BigQuery only supports single partition columns
        part_col_csv = offload_source_table.partition_columns[0].name
        gran_list = [gran_list[0]]
    return (part_col_csv, ','.join(gran_list), offload_partition_lower_value, offload_partition_upper_value)


def get_suitable_offload_partition_name(owner, table_name, config, messages):
    offload_source_table = OffloadSourceTable.create(owner, table_name, config, messages)
    if not offload_source_table.columns:
        raise TestOffloadException('Offload source table does not exist: %s.%s' % (owner, table_name))
    if offload_source_table.offload_by_subpartition_capable(valid_for_auto_enable=True):
        offload_source_table.enable_offload_by_subpartition()
    ltv_operation = OffloadOperation.from_dict({'owner_table': '%s.%s' % (owner, table_name),
                                                'reset_backend_table': True}, config, messages)
    backend_table = get_offload_target_table(ltv_operation, config, messages)
    source_data_client = offload_source_data_factory(offload_source_table, backend_table, ltv_operation, None,
                                                     messages, OFFLOAD_SOURCE_CLIENT_OFFLOAD)
    if not source_data_client.is_partition_append_capable():
        return None
    source_data_client.populate_source_partitions()
    if source_data_client.source_partitions.count() <= 1:
        # no HWM if only 1 partition (or none at all)
        return None
    if source_data_client.get_partition_append_predicate_type() == INCREMENTAL_PREDICATE_TYPE_RANGE:
        # single HWM partition
        hv_partition = source_data_client.source_partitions.get_prior_partition(
            partition=source_data_client.source_partitions.get_partition_by_index(0))
        partition_csv = hv_partition.partition_name
    elif source_data_client.get_partition_append_predicate_type() == INCREMENTAL_PREDICATE_TYPE_LIST:
        # all partitions bar the last one
        offload_partition_names = source_data_client.source_partitions.partition_names()
        offload_partition_names.pop(0)
        partition_csv = ','.join(offload_partition_names)
    else:
        raise NotImplementedError('get_suitable_partition_name does not support predicate type: %s'
                                  % source_data_client.get_partition_append_predicate_type())
    messages.log('get_suitable_partition_name: --partition_names %s' % partition_csv, detail=VERBOSE)
    return partition_csv
