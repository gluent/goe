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

""" BigQueryLiteral: Format a BigQuery literal based on data type.
"""

from datetime import date, time
import logging

from numpy import datetime64

from goe.offload.bigquery.bigquery_column import (
    BIGQUERY_TYPE_BIGNUMERIC,
    BIGQUERY_TYPE_BYTES,
    BIGQUERY_TYPE_DATE,
    BIGQUERY_TYPE_DATETIME,
    BIGQUERY_TYPE_NUMERIC,
    BIGQUERY_TYPE_TIME,
    BIGQUERY_TYPE_TIMESTAMP,
)
from goe.offload.format_literal import FormatLiteralInterface

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# BigQueryLiteral
###########################################################################


class BigQueryLiteral(FormatLiteralInterface):
    @classmethod
    def _format_data_type_with_prefix(cls, str_val, data_type):
        if data_type == BIGQUERY_TYPE_NUMERIC:
            return "NUMERIC '%s'" % str_val
        elif data_type == BIGQUERY_TYPE_BIGNUMERIC:
            return "BIGNUMERIC '%s'" % str_val
        elif data_type == BIGQUERY_TYPE_DATE:
            return "DATE '%s'" % str_val[:10]
        elif data_type == BIGQUERY_TYPE_DATETIME:
            return "DATETIME '%s'" % cls._strip_unused_time_scale(str_val)
        elif data_type == BIGQUERY_TYPE_TIMESTAMP:
            return "TIMESTAMP '%s'" % str_val
        elif data_type == BIGQUERY_TYPE_TIME:
            return "TIME '%s'" % str_val
        else:
            return str_val

    @classmethod
    def format_literal(cls, python_value, data_type=None):
        """Return a string containing a correctly formatted literal for data_type.
        Without a data type the code should infer a literal from the Python type.
        See link below for more detail:
        https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#literals
        """
        logger.debug("Formatting %s literal: %r" % (type(python_value), python_value))
        logger.debug("For backend datatype: %s" % data_type)

        if isinstance(python_value, datetime64):
            if data_type and data_type == BIGQUERY_TYPE_TIME:
                new_py_val = cls._format_data_type_with_prefix(
                    str(python_value).split("T")[1], data_type
                )
            elif data_type:
                new_py_val = cls._format_data_type_with_prefix(
                    str(python_value).replace("T", " "), data_type
                )
            else:
                # Assuming DATETIME if no data_type specified
                new_py_val = "DATETIME '%s'" % cls._strip_unused_time_scale(
                    str(python_value).replace("T", " ")
                )
        elif isinstance(python_value, date):
            if data_type == BIGQUERY_TYPE_DATE:
                new_py_val = cls._format_data_type_with_prefix(
                    python_value.strftime("%Y-%m-%d"), data_type
                )
            elif data_type == BIGQUERY_TYPE_TIMESTAMP:
                new_py_val = cls._format_data_type_with_prefix(
                    python_value.strftime("%Y-%m-%d %H:%M:%S.%f%z"), data_type
                )
            elif data_type == BIGQUERY_TYPE_TIME:
                new_py_val = cls._format_data_type_with_prefix(
                    python_value.strftime("%H:%M:%S.%f"), data_type
                )
            elif data_type:
                new_py_val = cls._format_data_type_with_prefix(
                    python_value.strftime("%Y-%m-%d %H:%M:%S.%f"), data_type
                )
            else:
                # Assuming DATETIME if no data_type specified
                new_py_val = "DATETIME '%s'" % python_value.strftime(
                    "%Y-%m-%d %H:%M:%S.%f"
                )
        elif python_value is None:
            return "NULL"
        elif data_type == BIGQUERY_TYPE_TIME:
            if isinstance(python_value, time):
                new_py_val = cls._format_data_type_with_prefix(
                    python_value.strftime("%H:%M:%S.%f"), data_type
                )
            else:
                new_py_val = cls._format_data_type_with_prefix(
                    str(python_value), data_type
                )
        elif data_type in [BIGQUERY_TYPE_NUMERIC, BIGQUERY_TYPE_BIGNUMERIC]:
            new_py_val = cls._format_data_type_with_prefix(str(python_value), data_type)
        elif isinstance(python_value, str):
            if data_type == BIGQUERY_TYPE_BYTES:
                new_py_val = "b'%s'" % python_value
            else:
                new_py_val = "'%s'" % python_value
        elif isinstance(python_value, float):
            new_py_val = repr(python_value)
        else:
            new_py_val = str(python_value)

        return new_py_val
