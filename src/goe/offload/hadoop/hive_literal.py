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

""" HiveLiteral: Format a Hive literal based on data type.
"""

from datetime import date
import logging

from numpy import datetime64

from goe.offload.format_literal import FormatLiteralInterface
from goe.offload.hadoop.hadoop_column import HADOOP_TYPE_DATE, HADOOP_TYPE_TIMESTAMP

###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default

###########################################################################
# HiveLiteral
###########################################################################


class HiveLiteral(FormatLiteralInterface):
    @classmethod
    def _format_data_type_with_prefix(cls, str_val, data_type):
        if data_type == HADOOP_TYPE_DATE:
            return "date '%s'" % str_val[:10]
        elif data_type == HADOOP_TYPE_TIMESTAMP:
            return "timestamp '%s'" % str_val
        else:
            return str_val

    @classmethod
    def format_literal(cls, python_value, data_type=None):
        """Translate a Python value to a Hive literal, only dates and strings are impacted, other types
        just pass through data_type is ignored for Hive
        """
        logger.debug("Formatting %s literal: %r" % (type(python_value), python_value))
        logger.debug("For backend datatype: %s" % data_type)
        if isinstance(python_value, datetime64):
            # Assuming TIMESTAMP if no data_type specified
            str_value = cls._strip_unused_time_scale(
                str(python_value).replace("T", " "), trim_unnecessary_subseconds=True
            )
            new_py_val = cls._format_data_type_with_prefix(
                str_value, data_type or HADOOP_TYPE_TIMESTAMP
            )
        elif isinstance(python_value, date):
            if data_type == HADOOP_TYPE_DATE:
                new_py_val = cls._format_data_type_with_prefix(
                    python_value.strftime("%Y-%m-%d"), data_type
                )
            else:
                # Assuming TIMESTAMP if no data_type specified
                str_value = cls._strip_unused_time_scale(
                    python_value.strftime("%Y-%m-%d %H:%M:%S.%f"),
                    trim_unnecessary_subseconds=True,
                )
                new_py_val = cls._format_data_type_with_prefix(
                    str_value, data_type or HADOOP_TYPE_TIMESTAMP
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
