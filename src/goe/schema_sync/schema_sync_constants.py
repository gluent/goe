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

SCHEMA_SYNC_OP_NAME = "schema_sync"

# schema sync step names

ADD_BACKEND_COLUMN = "add_backend_column"
ADD_ORACLE_COLUMN = "add_oracle_column"
PRESENT_TABLE = "present_table"

# schema sync exception names

EXCEPTION_ADD_BACKEND_COLUMN = "GOESS-01: exception caught while adding backend column"
EXCEPTION_ADD_ORACLE_COLUMN = "GOESS-03: exception caught while adding Oracle column"
EXCEPTION_ORACLE_HIVE_NAME_MISMATCH = (
    "GOESS-11: Oracle and backend object names do not match"
)
EXCEPTION_ORACLE_MORE_THAN_998_COLUMNS = "GOESS-12: Oracle Incremental Update changelog table would exceed the 1000 Oracle column limit"
EXCEPTION_SCHEMA_EVOLUTION_NOT_SUPPORTED = (
    "GOESS-13: Schema evolution is not supported for this backend system"
)

# schema sync analyzer

SOURCE_TYPE_RDBMS = "rdbms"
SOURCE_TYPE_BACKEND = "backend"
