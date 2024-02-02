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

from abc import ABCMeta, abstractmethod
import base64
from goe.offload.oracle.oracle_column import (
    ORACLE_TYPE_BLOB,
    ORACLE_TYPE_CLOB,
    ORACLE_TYPE_NCLOB,
)
from goe.offload.offload_messages import VVERBOSE


###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

###########################################################################
# QueryImportInterface
###########################################################################


class QueryImportInterface(metaclass=ABCMeta):
    """Abstract base class which acts as an interface for backend specific Avro and Parquet based sub-classes.
    Also provides some common functions for the sub-classes.
    """

    def __init__(self, schema, messages, compression=False, base64_columns=None):
        assert schema
        self.schema = None
        self._messages = messages
        self._codec = None
        self._base64_columns = base64_columns or []
        self._source_data_types_requiring_read = [
            ORACLE_TYPE_BLOB,
            ORACLE_TYPE_NCLOB,
            ORACLE_TYPE_CLOB,
        ]

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _debug(self, msg):
        self._messages.log(msg, detail=VVERBOSE)

    def _log(self, msg, detail=None):
        self._messages.log(msg, detail=detail)

    def _get_base64_encode_fn(self, rdbms_data_type):
        if rdbms_data_type in self._source_data_types_requiring_read:
            # BLOB should not undergo any character conversion therefore avoiding write_utf8
            return lambda x: base64.b64encode(x.read())
        else:
            return lambda x: base64.b64encode(x)

    def _get_encode_read_fn(self):
        return lambda x: x.read()

    def _get_tsltz_encode_fn(self):
        """WITH LOCAL TIME ZONE needs UTC suffix to match Sqoop.
        Can't achieve with NLS_TIMESTAMP_FORMAT because that impacts normal timestamp format.
        """
        return lambda x: str(x) + " UTC"

    def _strip_trailing_dot(self, strval):
        return strval[:-1] if strval.endswith(".") else strval

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @abstractmethod
    def write_from_cursor(
        self, local_output_path, extraction_cursor, source_columns, fetch_size=None
    ):
        """fetch_size optional because not all frontends take a parameter to fetchmany()."""
