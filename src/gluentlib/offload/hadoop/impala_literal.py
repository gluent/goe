#! /usr/bin/env python3
""" ImpalaLiteral: Format an Impala literal based on data type.
    LICENSE_TEXT
"""

from datetime import date
import logging

from numpy import datetime64

from gluentlib.offload.format_literal import FormatLiteralInterface

###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default

###########################################################################
# ImpalaLiteral
###########################################################################


class ImpalaLiteral(FormatLiteralInterface):

    @classmethod
    def format_literal(cls, python_value, data_type=None):
        """ Translate a Python value to an Impala literal, only dates and strings are impacted, other types
            just pass through data_type is ignored for Impala
        """
        logger.debug('Formatting %s literal: %r' % (type(python_value), python_value))
        logger.debug('For backend datatype: %s' % data_type)
        if isinstance(python_value, datetime64):
            str_value = cls._strip_unused_time_scale(str(python_value).replace('T', ' '),
                                                     trim_unnecessary_subseconds=True)
            new_py_val = '\'%s\'' % str_value
        elif isinstance(python_value, date):
            str_value = cls._strip_unused_time_scale(python_value.strftime("%Y-%m-%d %H:%M:%S.%f"),
                                                     trim_unnecessary_subseconds=True)
            new_py_val = '\'%s\'' % str_value
        elif isinstance(python_value, str):
            new_py_val = '\'%s\'' % python_value
        elif python_value is None:
            return 'NULL'
        elif isinstance(python_value, float):
            new_py_val = repr(python_value)
        else:
            new_py_val = str(python_value)
        return new_py_val