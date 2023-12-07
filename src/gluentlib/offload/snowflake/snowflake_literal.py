#! /usr/bin/env python3
""" SnowflakeLiteral: Format an Snowflake literal based on data type.
    LICENSE_TEXT
"""

from datetime import date, time
import logging

from numpy import datetime64

from gluentlib.offload.snowflake.snowflake_column import SNOWFLAKE_TYPE_DATE, SNOWFLAKE_TYPE_TIME,\
    SNOWFLAKE_TYPE_TIMESTAMP_NTZ, SNOWFLAKE_TYPE_TIMESTAMP_TZ
from gluentlib.offload.format_literal import FormatLiteralInterface

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# SnowflakeLiteral
###########################################################################

class SnowflakeLiteral(FormatLiteralInterface):

    @classmethod
    def _format_data_type_with_suffix(cls, str_val, data_type):
        if data_type == SNOWFLAKE_TYPE_DATE:
            return '\'%s\'::DATE' % str_val[:10]
        elif data_type == SNOWFLAKE_TYPE_TIMESTAMP_NTZ:
            return '\'%s\'::TIMESTAMP_NTZ' % cls._strip_unused_time_scale(str_val)
        elif data_type == SNOWFLAKE_TYPE_TIMESTAMP_TZ:
            return '\'%s\'::TIMESTAMP_TZ' % str_val
        elif data_type == SNOWFLAKE_TYPE_TIME:
            return '\'%s\'::TIME' % str_val
        else:
            return str_val

    @classmethod
    def format_literal(cls, python_value, data_type=None):
        """ Translate a Python value to a Snowflake literal
        """
        logger.debug('Formatting %s literal: %r' % (type(python_value), python_value))
        logger.debug('For data type: %s' % data_type)
        if isinstance(python_value, datetime64):
            if data_type and data_type == SNOWFLAKE_TYPE_TIME:
                new_py_val = cls._format_data_type_with_suffix(str(python_value).split('T')[1], data_type)
            elif data_type:
                new_py_val = cls._format_data_type_with_suffix(str(python_value).replace('T', ' '), data_type)
            else:
                # Assuming TIMESTAMP_NTZ if no data_type specified
                new_py_val = '\'%s\'::TIMESTAMP_NTZ' % cls._strip_unused_time_scale(str(python_value).replace('T', ' '))
        elif isinstance(python_value, date):
            if data_type == SNOWFLAKE_TYPE_DATE:
                new_py_val = cls._format_data_type_with_suffix(python_value.strftime("%Y-%m-%d"), data_type)
            elif data_type == SNOWFLAKE_TYPE_TIMESTAMP_TZ:
                if not python_value.tzinfo:
                    # Assume empty TZ means UTC
                    new_py_val = cls._format_data_type_with_suffix(python_value.strftime("%Y-%m-%d %H:%M:%S.%f +00:00"),
                                                                   data_type)
                else:
                    # Snowflake only understands HH:MM time zone offset, not named time zones
                    new_py_val = cls._format_data_type_with_suffix(python_value.strftime("%Y-%m-%d %H:%M:%S.%f %z"),
                                                                   data_type)
            elif data_type == SNOWFLAKE_TYPE_TIME:
                new_py_val = cls._format_data_type_with_suffix(python_value.strftime("%H:%M:%S.%f"), data_type)
            elif data_type:
                new_py_val = cls._format_data_type_with_suffix(python_value.strftime("%Y-%m-%d %H:%M:%S.%f"), data_type)
            else:
                # Assuming TIMESTAMP_NTZ if no data_type specified
                new_py_val = '\'%s\'::TIMESTAMP_NTZ' % python_value.strftime("%Y-%m-%d %H:%M:%S.%f")
        elif python_value is None:
            return 'NULL'
        elif data_type == SNOWFLAKE_TYPE_TIME:
            if isinstance(python_value, time):
                new_py_val = cls._format_data_type_with_suffix(python_value.strftime("%H:%M:%S.%f"), data_type)
            else:
                new_py_val = cls._format_data_type_with_suffix(python_value, data_type)
        elif isinstance(python_value, str):
            new_py_val = '\'%s\'' % python_value
        elif isinstance(python_value, float):
            new_py_val = repr(python_value)
        else:
            new_py_val = str(python_value)
        return new_py_val
