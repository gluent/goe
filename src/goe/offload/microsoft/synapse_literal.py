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

""" SynapseLiteral: Format an Synapse literal based on data type.
"""

from datetime import date, time
import logging

import re
from numpy import datetime64

from goe.offload.format_literal import FormatLiteralInterface, RE_TIMEZONE_NO_COLON
from goe.offload.microsoft.synapse_column import (
    SYNAPSE_TYPE_BINARY,
    SYNAPSE_TYPE_DATE,
    SYNAPSE_TYPE_DATETIME,
    SYNAPSE_TYPE_DATETIME2,
    SYNAPSE_TYPE_DATETIMEOFFSET,
    SYNAPSE_TYPE_NCHAR,
    SYNAPSE_TYPE_NVARCHAR,
    SYNAPSE_TYPE_SMALLDATETIME,
    SYNAPSE_TYPE_TIME,
    SYNAPSE_TYPE_VARBINARY,
)


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

###########################################################################
# SynapseLiteral
###########################################################################


class SynapseLiteral(FormatLiteralInterface):
    @classmethod
    def format_literal(cls, python_value, data_type=None):
        """Translate a Python value to a Synapse literal, only dates, strings and binary are impacted, other types
        just pass through.
        This is based on:
            https://docs.microsoft.com/en-us/sql/analytics-platform-system/load-with-insert?view=aps-pdw-2016-au7
        Disappointingly primitive.
        """

        def format_date_for_data_type(str_val, data_type):
            if data_type == SYNAPSE_TYPE_DATE:
                return "'%s'" % str_val[:10]
            elif data_type in [
                SYNAPSE_TYPE_DATETIME,
                SYNAPSE_TYPE_DATETIME2,
                SYNAPSE_TYPE_SMALLDATETIME,
            ]:
                return "'%s'" % cls._strip_unused_time_scale(str_val)
            elif data_type == SYNAPSE_TYPE_DATETIMEOFFSET and re.match(
                RE_TIMEZONE_NO_COLON, str_val
            ):
                # %z in strftime does not have a colon in the timezone offset which Synapse requires
                return "'{0}:{1}'".format(str_val[:-2], str_val[-2:])
            else:
                return "'%s'" % str_val

        logger.debug("Formatting %s literal: %r" % (type(python_value), python_value))
        logger.debug("For data type: %s" % data_type)
        if isinstance(python_value, datetime64):
            if data_type == SYNAPSE_TYPE_TIME:
                new_py_val = format_date_for_data_type(
                    str(python_value).split("T")[1], data_type
                )
            elif data_type:
                new_py_val = format_date_for_data_type(
                    str(python_value).replace("T", " "), data_type
                )
            else:
                # Assuming DATETIME if no data_type specified
                new_py_val = format_date_for_data_type(
                    str(python_value).replace("T", " "), SYNAPSE_TYPE_DATETIME
                )
        elif isinstance(python_value, date):
            if data_type == SYNAPSE_TYPE_DATE:
                new_py_val = format_date_for_data_type(
                    python_value.strftime("%Y-%m-%d"), data_type
                )
            elif data_type == SYNAPSE_TYPE_DATETIMEOFFSET:
                if not python_value.tzinfo:
                    # Assume empty TZ means UTC
                    new_py_val = format_date_for_data_type(
                        python_value.strftime("%Y-%m-%d %H:%M:%S.%f +00:00"), data_type
                    )
                else:
                    # Synapse only understands HH:MM time zone offset, not named time zones
                    new_py_val = format_date_for_data_type(
                        python_value.strftime("%Y-%m-%d %H:%M:%S.%f %z"), data_type
                    )
            elif data_type == SYNAPSE_TYPE_TIME:
                new_py_val = format_date_for_data_type(
                    python_value.strftime("%H:%M:%S.%f"), data_type
                )
            elif data_type:
                new_py_val = format_date_for_data_type(
                    python_value.strftime("%Y-%m-%d %H:%M:%S.%f"), data_type
                )
            else:
                # Assuming DATETIME if no data_type specified
                new_py_val = format_date_for_data_type(
                    python_value.strftime("%Y-%m-%d %H:%M:%S.%f"), SYNAPSE_TYPE_DATETIME
                )
        elif python_value is None:
            return "NULL"
        elif data_type == SYNAPSE_TYPE_TIME:
            if isinstance(python_value, time):
                new_py_val = format_date_for_data_type(
                    python_value.strftime("%H:%M:%S.%f"), data_type
                )
            else:
                new_py_val = format_date_for_data_type(python_value, data_type)
        elif data_type in [SYNAPSE_TYPE_NCHAR, SYNAPSE_TYPE_NVARCHAR]:
            new_py_val = "N'%s'" % python_value
        elif data_type in [SYNAPSE_TYPE_VARBINARY, SYNAPSE_TYPE_BINARY]:
            if isinstance(python_value, str):
                new_py_val = "0x%s" % python_value.encode().hex()
            else:
                new_py_val = "0x%s" % python_value.hex()
        elif isinstance(python_value, str):
            # Any uniqueidentifier literal should drop in here therefore we don't need a specific format.
            new_py_val = "'%s'" % python_value
        elif isinstance(python_value, bytes):
            new_py_val = "'%s'" % python_value.decode()
        elif isinstance(python_value, float):
            new_py_val = repr(python_value)
        else:
            new_py_val = str(python_value)
        return new_py_val
