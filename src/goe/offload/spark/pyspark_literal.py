# Copyright 2024 The GOE Authors. All rights reserved.
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

""" PysparkLiteral: Format a Pyspark literal based on value type.
"""

from datetime import date
import logging

from numpy import datetime64

from goe.offload.format_literal import FormatLiteralInterface

###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default

###########################################################################
# PysparkLiteral
###########################################################################


class PysparkLiteral(FormatLiteralInterface):
    @classmethod
    def format_literal(cls, python_value, data_type=None):
        """Translate a Python value to a Spark/Python literal that can be embedded in Pyspark code.

        Only dates and strings are impacted, other types just pass through.
        """
        logger.debug("Formatting %s literal: %r" % (type(python_value), python_value))
        logger.debug("For backend datatype: %s" % data_type)
        if isinstance(python_value, datetime64):
            str_value = cls._strip_unused_time_scale(
                str(python_value).replace("T", " "), trim_unnecessary_subseconds=True
            )
            new_py_val = f"'{str_value}'"
        elif isinstance(python_value, date):
            str_value = cls._strip_unused_time_scale(
                python_value.strftime("%Y-%m-%d %H:%M:%S.%f"),
                trim_unnecessary_subseconds=True,
            )
            new_py_val = f"'{str_value}'"
        elif isinstance(python_value, str):
            new_py_val = f"'{python_value}'"
        elif python_value is None:
            return "None"
        elif isinstance(python_value, float):
            new_py_val = repr(python_value)
        else:
            new_py_val = str(python_value)
        return new_py_val
