#! /usr/bin/env python3
""" data_type_controls: Library of functions used in GOE to process data type control options/structures.
    LICENSE_TEXT
"""

from gluentlib.offload.column_metadata import CanonicalColumn, CANONICAL_TYPE_OPTION_NAMES, \
    CANONICAL_CHAR_SEMANTICS_UNICODE, match_table_column
from gluentlib.offload.offload_functions import expand_columns_csv
from gluentlib.offload.offload_constants import INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT

class OffloadDataTypeControlsException(Exception): pass


###############################################################################
# CONSTANTS
###############################################################################

CONFLICTING_DATA_TYPE_OPTIONS_EXCEPTION_TEXT = 'Data type conflict for columns'


###############################################################################
# GLOBAL FUNCTIONS
###############################################################################

def canonical_columns_from_columns_csv(data_type, column_list_csv, existing_canonical_list, reference_columns,
                                       precision=None, scale=None):
    """ Return a list of CanonicalColumn objects based on an incoming data type and CSV of column names """
    if not column_list_csv:
        return []
    column_list = expand_columns_csv(column_list_csv, reference_columns)
    conflicting_columns = [_.name for _ in existing_canonical_list if _.name in column_list]
    if conflicting_columns:
        raise OffloadDataTypeControlsException('%s %s when assigning type with %s'
                               % (CONFLICTING_DATA_TYPE_OPTIONS_EXCEPTION_TEXT, conflicting_columns,
                                  CANONICAL_TYPE_OPTION_NAMES[data_type]))
    canonical_list = [CanonicalColumn(_, data_type, data_precision=precision, data_scale=scale, from_override=True)
                      for _ in column_list]
    if '*' in column_list_csv and not canonical_list:
        raise OffloadDataTypeControlsException(f'No columns match pattern: {column_list_csv}')
    return canonical_list


def char_semantics_override_map(unicode_string_columns_csv, reference_columns):
    """ Return a dictionary map of char semantics overrides """
    if not unicode_string_columns_csv:
        return {}
    unicode_string_columns_list = expand_columns_csv(unicode_string_columns_csv, reference_columns)
    if '*' in unicode_string_columns_csv and not unicode_string_columns_list:
        raise OffloadDataTypeControlsException(f'No columns match pattern: {unicode_string_columns_csv}')
    for col in unicode_string_columns_list:
        rdbms_column = match_table_column(col, reference_columns)
        if not rdbms_column.is_string_based():
            raise OffloadDataTypeControlsException('%s %s: %s is not string based'
                                                   % (INVALID_DATA_TYPE_CONVERSION_EXCEPTION_TEXT, rdbms_column.name,
                                                      rdbms_column.data_type))
    return dict(zip(unicode_string_columns_list, [CANONICAL_CHAR_SEMANTICS_UNICODE] * len(unicode_string_columns_list)))
