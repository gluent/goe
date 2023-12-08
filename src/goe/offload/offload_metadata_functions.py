#! /usr/bin/env python3
""" Library for logic/interaction with hybrid metadata
    For now a handy location for like minded functions, in the future this might become a class
    LICENSE_TEXT
"""

import logging

from goe.offload.column_metadata import invalid_column_list_message, match_table_column, valid_column_list
from goe.offload.offload_source_table import OffloadSourceTableInterface, convert_high_values_to_python, \
    OFFLOAD_PARTITION_TYPE_LIST, OFFLOAD_PARTITION_TYPE_RANGE
from goe.offload.offload_xform_functions import transformations_to_metadata
from goe.orchestration import command_steps, orchestration_constants
from goe.persistence.orchestration_metadata import OrchestrationMetadata,\
    INCREMENTAL_PREDICATE_TYPE_PREDICATE, INCREMENTAL_PREDICATE_TYPE_LIST, INCREMENTAL_PREDICATE_TYPE_RANGE
from goe.util.misc_functions import csv_split, nvl, unsurround


class OffloadMetadataException(Exception): pass


###############################################################################
# CONSTANTS
###############################################################################

OFFLOAD_TYPE_FULL = 'FULL'
OFFLOAD_TYPE_INCREMENTAL = 'INCREMENTAL'

HYBRID_VIEW_IPA_HWM_TOKEN_RDBMS_BEGIN = '--BEGINRDBMSHWM'
HYBRID_VIEW_IPA_HWM_TOKEN_RDBMS_END = '--ENDRDBMSHWM'
HYBRID_VIEW_IPA_HWM_TOKEN_REMOTE_BEGIN = '--BEGINREMOTEHWM'
HYBRID_VIEW_IPA_HWM_TOKEN_REMOTE_END = '--ENDREMOTEHWM'

METADATA_AGGREGATE_HYBRID_VIEW = 'GLUENT_OFFLOAD_AGGREGATE_HYBRID_VIEW'
METADATA_HYBRID_VIEW = 'GLUENT_OFFLOAD_HYBRID_VIEW'
METADATA_JOIN_HYBRID_VIEW = 'GLUENT_OFFLOAD_JOIN_HYBRID_VIEW'


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

def gen_offload_metadata_from_base(repo_client, hybrid_operation, threshold_cols, offload_high_values,
                                   incremental_predicate_values, object_type, backend_owner, backend_table_name,
                                   base_metadata, object_hash=None):
    """ base_metadata is used to carry certain metadata attributes forward to another operation,
        e.g. from an offload to a present. Also used to carry forward attributes from one offload to another which would
        not otherwise be set, e.g. Incremental Update attributes.
        Backend owner and table names should NOT have their case interfered with. Some backends are case sensitive so
        we cannot uppercase all values for consistency (which is what we used to do).
    """
    if hybrid_operation.sort_columns:
        assert isinstance(hybrid_operation.sort_columns, list), \
            '{} is not of type list'.format(type(hybrid_operation.offload_partition_functions))
    if hybrid_operation.offload_partition_functions:
        assert isinstance(hybrid_operation.offload_partition_functions, list),\
            '{} is not of type list'.format(type(hybrid_operation.offload_partition_functions))

    offloaded_owner, offloaded_table, offload_scn, offload_version = None, None, None, None
    bucket_hash_column, bucket_hash_method = None, None
    incremental_predicate_type = None
    incremental_range = None
    offload_partition_functions = None
    iu_key_columns, iu_extraction_method, iu_extraction_scn, iu_extraction_time = None, None, None, None
    changelog_table, changelog_trigger, changelog_sequence = None, None, None
    updatable_view, updatable_trigger = None, None

    if base_metadata:
        offloaded_owner = base_metadata.get('OFFLOADED_OWNER', '')
        offloaded_table = base_metadata.get('OFFLOADED_TABLE', '')
        offload_scn = base_metadata.get('OFFLOAD_SCN')
        bucket_hash_column = base_metadata.get('OFFLOAD_BUCKET_COLUMN')
        bucket_hash_method = base_metadata.get('OFFLOAD_BUCKET_METHOD')
        offload_version = base_metadata.get('OFFLOAD_VERSION')
        incremental_range = base_metadata.get('INCREMENTAL_RANGE')
        incremental_predicate_type = base_metadata.get('INCREMENTAL_PREDICATE_TYPE')
        if base_metadata.get('OFFLOAD_PARTITION_FUNCTIONS'):
            offload_partition_functions = csv_split(base_metadata['OFFLOAD_PARTITION_FUNCTIONS'])
        if object_type == METADATA_HYBRID_VIEW:
            # Incremental Update metadata is only persisted to the base hybrid view metadata...
            if base_metadata.get('IU_KEY_COLUMNS'):
                iu_key_columns = csv_split(base_metadata.get('IU_KEY_COLUMNS'))
            iu_extraction_method = base_metadata.get('IU_EXTRACTION_METHOD')
            iu_extraction_scn = base_metadata.get('IU_EXTRACTION_SCN')
            iu_extraction_time = base_metadata.get('IU_EXTRACTION_TIME')
            changelog_table = base_metadata.get('CHANGELOG_TABLE')
            changelog_trigger = base_metadata.get('CHANGELOG_TRIGGER')
            changelog_sequence = base_metadata.get('CHANGELOG_SEQUENCE')
            updatable_view = base_metadata.get('UPDATABLE_VIEW')
            updatable_trigger = base_metadata.get('UPDATABLE_TRIGGER')

    # Predicate type might be changing so need to take it off the currently-executing operation...
    incremental_predicate_type = hybrid_operation.ipa_predicate_type or incremental_predicate_type

    if not incremental_range:
        if hybrid_operation.offload_by_subpartition:
            incremental_range = 'SUBPARTITION'
        elif threshold_cols and incremental_predicate_type != INCREMENTAL_PREDICATE_TYPE_PREDICATE:
            incremental_range = 'PARTITION'

    bucket_hash_column = bucket_hash_column or hybrid_operation.bucket_hash_col
    bucket_hash_method = bucket_hash_method or hybrid_operation.bucket_hash_method
    offload_partition_functions = offload_partition_functions or hybrid_operation.offload_partition_functions

    if hybrid_operation.hwm_in_hybrid_view and incremental_predicate_type != INCREMENTAL_PREDICATE_TYPE_PREDICATE:
        # incremental_predicate_type PREDICATE has a UNION ALL hybrid view but no incremental keys
        incremental_key_csv = incremental_key_csv_from_part_keys(threshold_cols)
        incremental_hv_csv = incremental_hv_csv_from_list(offload_high_values)
    else:
        incremental_key_csv = None
        incremental_hv_csv = None

    offload_sort_csv = column_name_list_to_csv(hybrid_operation.sort_columns)
    offload_partition_functions_csv = offload_partition_functions_to_csv(offload_partition_functions)
    xform_str = transformations_to_metadata(hybrid_operation.column_transformations)
    incremental_predicate_value = [p.dsl for p in
                                   incremental_predicate_values] if incremental_predicate_values else None
    iu_key_columns_csv = column_name_list_to_csv(iu_key_columns)

    return OrchestrationMetadata.from_attributes(
        client=repo_client,
        hybrid_owner=hybrid_operation.hybrid_owner,
        hybrid_view=hybrid_operation.hybrid_name,
        object_type=object_type,
        external_table=hybrid_operation.ext_table_name,
        backend_owner=backend_owner,
        backend_table=backend_table_name,
        offload_type=hybrid_operation.offload_type,
        offloaded_owner=offloaded_owner,
        offloaded_table=offloaded_table,
        incremental_key=incremental_key_csv,
        incremental_high_value=incremental_hv_csv,
        incremental_range=incremental_range,
        incremental_predicate_type=incremental_predicate_type,
        incremental_predicate_value=incremental_predicate_value,
        offload_bucket_column=bucket_hash_column,
        offload_bucket_method=bucket_hash_method,
        offload_bucket_count=hybrid_operation.num_buckets if bucket_hash_column else None,
        offload_version=offload_version,
        offload_sort_columns=offload_sort_csv,
        transformations=xform_str,
        object_hash=object_hash,
        offload_scn=offload_scn,
        offload_partition_functions=offload_partition_functions_csv,
        iu_key_columns=iu_key_columns_csv,
        iu_extraction_method=iu_extraction_method,
        iu_extraction_scn=iu_extraction_scn,
        iu_extraction_time=iu_extraction_time,
        changelog_table=changelog_table,
        changelog_trigger=changelog_trigger,
        changelog_sequence=changelog_sequence,
        updatable_view=updatable_view,
        updatable_trigger=updatable_trigger,
        execution_id=hybrid_operation.execution_id
    )


def gen_and_save_offload_metadata(repo_client, messages, hybrid_operation, hybrid_config,
                                  incremental_key_columns, incremental_high_values, incremental_predicate_values,
                                  object_type, hadoop_owner, hadoop_table_name, base_metadata_dict,
                                  object_hash=None, parent_command_type=None):
    """ Simple wrapper over generation and saving of hybrid metadata.
        Returns the new metadata object for convenience.
    """
    assert isinstance(base_metadata_dict, dict)
    hybrid_metadata = gen_offload_metadata_from_base(repo_client, hybrid_operation,
                                                     incremental_key_columns, incremental_high_values,
                                                     incremental_predicate_values, object_type,
                                                     hadoop_owner, hadoop_table_name,
                                                     base_metadata_dict, object_hash=object_hash)

    messages.offload_step(command_steps.STEP_SAVE_METADATA,
                          lambda: hybrid_metadata.save(hybrid_operation.execution_id), execute=hybrid_config.execute,
                          command_type=parent_command_type)
    return hybrid_metadata


def partition_columns_from_metadata(inc_key_string, rdbms_columns):
    """ Creates a list of tuples matching OffloadSourceTable.partition_columns from metadata.
    """
    assert valid_column_list(rdbms_columns), invalid_column_list_message(rdbms_columns)
    if not inc_key_string:
        return []
    inc_keys = csv_split(inc_key_string.upper())
    partition_columns = []
    for inc_key in inc_keys:
        col = match_table_column(inc_key, rdbms_columns)
        if not col:
            raise OffloadMetadataException('Column %s not found in table' % inc_key)
        partition_columns.append(col)
    return partition_columns


def incremental_key_csv_from_part_keys(incremental_keys):
    """ Takes partition columns list of tuples and formats metadata CSV.
        Broken out as a function as formatting should be consistent.
    """
    assert valid_column_list(incremental_keys), invalid_column_list_message(incremental_keys)
    return ', '.join([_.name for _ in incremental_keys]) if incremental_keys else None


def unknown_to_str(ch):
    """ Convert anything to str but not if it's already str.
        Basically: don't downgrade unicode data to str.
    """
    if not isinstance(ch, str):
        return str(ch)
    else:
        return ch


def incremental_hv_csv_from_list(incremental_high_values):
    """ Takes partition HWM literals (for Oracle as stored in data dictionary) and formats them.
        Broken out as a function because formatting should be consistent.
        For RANGE this gives us a simple CSV with spaces after commas.
        For LIST this gives is a CSV of tuples with spaces after commas, this format is decoded in
        incremental_hv_list_from_csv so should not be modified lightly.
    """
    def hv_to_str(hv):
        if type(hv) in (tuple, list):
            return '(%s)' % ', '.join(unknown_to_str(_) for _ in hv)
        else:
            return unknown_to_str(hv)
    return ', '.join(hv_to_str(_) for _ in incremental_high_values) if incremental_high_values else None


def incremental_hv_list_from_csv(metadata_high_value_string, ipa_predicate_type):
    """ Accepts hybrid metadata INCREMENTAL_HIGH_VALUE and decodes it based on the ipa_predicate_type.
        For RANGE this is just a pass through because metadata only stores a single HV list (implied tuple).
        For LIST the metadata is a list of tuples so we need to pre-process the string to get a list of the
        individual tuples.
    """
    def metadata_high_value_split(metadata_str):
        """ Splits a string on commas when not nested inside parentheses or quotes
        """
        tokens = []
        current_token = ''
        paren_level = 0
        gobble_until_next_char = None
        for c in metadata_str:
            if gobble_until_next_char:
                current_token += c
                if c == gobble_until_next_char:
                    gobble_until_next_char = None
            elif c == ',' and paren_level == 0:
                tokens.append(current_token.strip())
                current_token = ''
            else:
                if c in ("'", '"'):
                    gobble_until_next_char = c
                elif c == '(':
                    paren_level += 1
                elif c == ')':
                    paren_level -= 1
                current_token += c
        # add the final token
        if current_token:
            tokens.append(current_token.strip())
        return tokens

    if not metadata_high_value_string:
        return None
    if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        tokens = metadata_high_value_split(metadata_high_value_string)
        # for list each token is surrounded by brackets so we should remove those
        tokens = [unsurround(_, '(', ')') for _ in tokens]
        return tokens
    else:
        return metadata_high_value_string


def decode_metadata_incremental_high_values_from_metadata(base_metadata, rdbms_base_table):
    """ Equivalent of OffloadSourceTable.decode_partition_high_values_with_literals but for metadata based values
        This function subverts ipa_predicate_type by using it as partition_type. This works currently but is not
        ideal.
    """
    assert isinstance(base_metadata, OrchestrationMetadata)
    assert isinstance(rdbms_base_table, OffloadSourceTableInterface),\
        '%s is not of type OffloadSourceTableInterface' % str(type(rdbms_base_table))
    if not base_metadata or not base_metadata.incremental_high_value:
        return [], [], []
    return decode_metadata_incremental_high_values(base_metadata.incremental_predicate_type,
                                                   base_metadata.incremental_key,
                                                   base_metadata.incremental_high_value,
                                                   rdbms_base_table)


def decode_metadata_incremental_high_values(incremental_predicate_type, incremental_key,
                                            incremental_high_value, rdbms_base_table):
    """ Equivalent of OffloadSourceTable.decode_partition_high_values_with_literals but for metadata based values
        This function subverts ipa_predicate_type by using it as partition_type. This works currently but is not
        ideal.
    """
    def hvs_to_python(hv_csv, partition_columns, rdbms_base_table, partition_type):
        hv_literals = rdbms_base_table.split_partition_high_value_string(hv_csv)
        partition_type = OFFLOAD_PARTITION_TYPE_RANGE if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_RANGE else OFFLOAD_PARTITION_TYPE_LIST
        hv_python = convert_high_values_to_python(partition_columns, hv_literals, partition_type, rdbms_base_table)
        return tuple(hv_python), tuple(hv_literals)

    ipa_predicate_type = incremental_predicate_type or INCREMENTAL_PREDICATE_TYPE_RANGE
    partition_columns = partition_columns_from_metadata(incremental_key, rdbms_base_table.columns)
    partitionwise_hvs = incremental_hv_list_from_csv(incremental_high_value, ipa_predicate_type)
    if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        hv_real_list, hv_indiv_list = [], []
        for partitionwise_hv in partitionwise_hvs:
            hv_real_vals, hv_indiv_vals = hvs_to_python(partitionwise_hv, partition_columns,
                                                        rdbms_base_table, ipa_predicate_type)
            hv_real_list.append(hv_real_vals)
            hv_indiv_list.append(hv_indiv_vals)
        return partition_columns, hv_real_list, hv_indiv_list
    else:
        hv_real_vals, hv_indiv_vals = hvs_to_python(partitionwise_hvs, partition_columns,
                                                    rdbms_base_table, ipa_predicate_type)
        return partition_columns, hv_real_vals, hv_indiv_vals


def split_metadata_incremental_high_values(base_metadata, frontend_api):
    """ Equivalent of decode_metadata_incremental_high_values() above but for when we don't have access
        to an RDBMS base table.
        This means we can only decode as far as string HVs, no conversion to real Python values.
    """
    if not base_metadata or not base_metadata.incremental_high_value:
        return None
    ipa_predicate_type = base_metadata.incremental_predicate_type or INCREMENTAL_PREDICATE_TYPE_RANGE
    partitionwise_hvs = incremental_hv_list_from_csv(base_metadata.incremental_high_value, ipa_predicate_type)
    if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST:
        return [tuple(frontend_api.split_partition_high_value_string(_)) for _ in partitionwise_hvs]
    else:
        return frontend_api.split_partition_high_value_string(partitionwise_hvs)


def flatten_lpa_individual_high_values(incremental_high_values, to_str=True):
    """ Takes a set of list partition high values, as produced by split_metadata_incremental_high_values() or
        decode_metadata_incremental_high_values():

        [('A', 'B'), ('C')]

        and flattens them out for use in SQL:

        ['A', 'B', 'C']
    """
    assert type(incremental_high_values) is list, 'Type %s is not list' % type(incremental_high_values)
    if to_str:
        return [unknown_to_str(v) for hv in incremental_high_values for v in hv]
    else:
        return [v for hv in incremental_high_values for v in hv]


def flatten_lpa_high_values(incremental_high_values):
    """ Wrapper for flatten_lpa_individual_high_values but does retains
        literal data types
    """
    return flatten_lpa_individual_high_values(incremental_high_values, to_str=False)


def column_name_list_to_csv(columns, delimiter=','):
    """ Based on original implementation of offload_sort_columns_to_csv...
        Seems pretty trivial but need to be sure we do this consistently in a few different places
    """
    if not columns:
        return None
    return '%s' % delimiter.join(columns)


def offload_sort_columns_to_csv(offload_sort_columns):
    return column_name_list_to_csv(offload_sort_columns)


def offload_partition_functions_to_csv(offload_partition_functions):
    """ Copied behaviour from offload_sort_columns_to_csv above
    """
    if offload_partition_functions:
        assert isinstance(offload_partition_functions, list)
    else:
        return None
    return ','.join(nvl(_, '') for _ in offload_partition_functions)


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


if __name__ == "__main__":
    import sys
    from goe.util.misc_functions import set_gluentlib_logging

    log_level = sys.argv[-1:][0].upper()
    if log_level not in ('DEBUG', 'INFO', 'WARNING', 'CRITICAL', 'ERROR'):
        log_level = 'CRITICAL'

    set_gluentlib_logging(log_level)

    test_metadata = "TO_DATE(' 2011-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
    print('test_metadata:', test_metadata)
    decoded_metadata = incremental_hv_list_from_csv(test_metadata, INCREMENTAL_PREDICATE_TYPE_RANGE)
    print('incremental_hv_list_from_csv:', decoded_metadata)
    assert decoded_metadata == test_metadata

    test_metadata = "('A', 'B'), ('C', 'D')"
    print('test_metadata:', test_metadata)
    decoded_metadata = incremental_hv_list_from_csv(test_metadata, INCREMENTAL_PREDICATE_TYPE_LIST)
    print('incremental_hv_list_from_csv:', decoded_metadata)
    assert decoded_metadata == ["'A', 'B'", "'C', 'D'"]

    test_metadata = "2015, 03"
    print('test_metadata:', test_metadata)
    decoded_metadata = incremental_hv_list_from_csv(test_metadata, INCREMENTAL_PREDICATE_TYPE_RANGE)
    print('incremental_hv_list_from_csv:', decoded_metadata)
    assert decoded_metadata == test_metadata

    test_metadata = ['COLUMN_1', 'COLUMN_2']
    print('test_metadata:', test_metadata)
    csv_formatted_metadata = column_name_list_to_csv(test_metadata)
    print('csv_formatted_metadata:', csv_formatted_metadata)
    assert csv_formatted_metadata == 'COLUMN_1,COLUMN_2'
    tsv_formatted_metadata = column_name_list_to_csv(test_metadata, '~')
    print('tsv_formatted_metadata:', tsv_formatted_metadata)
    assert tsv_formatted_metadata == 'COLUMN_1~COLUMN_2'

