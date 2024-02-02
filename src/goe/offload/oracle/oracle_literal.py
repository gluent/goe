#! /usr/bin/env python3

# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" OracleLiteral: Format an Oracle literal based on data type.
"""

from datetime import date
import logging

from numpy import datetime64

from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_DATE,
    ORACLE_TYPE_TIMESTAMP,
    ORACLE_TIMESTAMP_RE,
    ORACLE_TYPE_TIMESTAMP_TZ,
)
from goe.offload.format_literal import FormatLiteralInterface

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# OracleLiteral
###########################################################################


class OracleLiteral(FormatLiteralInterface):
    @classmethod
    def _format_data_type(cls, str_val, data_type):
        if data_type == ORACLE_TYPE_DATE:
            # Oracle DATE includes time upto seconds
            adjusted_str = str_val[:19]
            if len(adjusted_str) == 10:
                # This is date only
                adjusted_str += " 00:00:00"
            return "DATE' %s'" % adjusted_str
        elif data_type == ORACLE_TYPE_TIMESTAMP or ORACLE_TIMESTAMP_RE.match(data_type):
            m = ORACLE_TIMESTAMP_RE.match(data_type)
            ts_ff = int(m.group(1)) if m else 9
            adjusted_str = str_val
            if len(adjusted_str) == 10:
                # Value is date part only
                adjusted_str += " 00:00:00.0"
            elif len(adjusted_str) == 19:
                # Value is up to seconds only
                adjusted_str += ".0"
            adjusted_str = str_val[: 20 + ts_ff]
            fmt = "YYYY-MM-DD HH24:MI:SS.FF%s" % (str(ts_ff) if ts_ff > 0 else "")
            return "TO_TIMESTAMP('%s','%s')" % (
                cls._strip_unused_time_scale(adjusted_str),
                fmt,
            )
        else:
            return str_val

    @classmethod
    def format_literal(cls, python_value, data_type=None):
        """Translate a Python value to an Oracle literal.
        Without a data type the code should infer a literal from the Python type.
        """
        logger.debug("Formatting %s literal: %r" % (type(python_value), python_value))
        logger.debug("For backend datatype: %s" % data_type)
        if isinstance(python_value, datetime64):
            if data_type:
                new_py_val = cls._format_data_type(
                    str(python_value).replace("T", " "), data_type
                )
            elif len(str(python_value)) <= 19:
                # Assuming DATE based on string length
                new_py_val = cls._format_data_type(
                    str(python_value).replace("T", " "), ORACLE_TYPE_DATE
                )
            else:
                # Assuming TIMESTAMP if no data_type specified
                new_py_val = cls._format_data_type(
                    str(python_value).replace("T", " "), ORACLE_TYPE_TIMESTAMP
                )
        elif isinstance(python_value, date):
            if data_type == ORACLE_TYPE_DATE:
                new_py_val = cls._format_data_type(
                    python_value.strftime("%Y-%m-%d %H:%M:%S"), data_type
                )
            elif data_type == ORACLE_TYPE_TIMESTAMP_TZ:
                new_py_val = cls._format_data_type(
                    python_value.strftime("%Y-%m-%d %H:%M:%S.%f%z"), data_type
                )
            elif data_type:
                new_py_val = cls._format_data_type(
                    python_value.strftime("%Y-%m-%d %H:%M:%S.%f"), data_type
                )
            elif len(str(python_value)) <= 19:
                # Assuming DATE based on string length
                new_py_val = cls._format_data_type(
                    python_value.strftime("%Y-%m-%d %H:%M:%S"), ORACLE_TYPE_DATE
                )
            else:
                # Assuming TIMESTAMP if no data_type specified
                new_py_val = cls._format_data_type(
                    python_value.strftime("%Y-%m-%d %H:%M:%S.%f"), ORACLE_TYPE_TIMESTAMP
                )
        elif isinstance(python_value, str):
            new_py_val = "'%s'" % python_value
        elif python_value is None:
            return "NULL"
        elif isinstance(python_value, float):
            new_py_val = repr(python_value)
        else:
            new_py_val = str(python_value)
        return new_py_val
