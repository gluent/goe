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

""" Standalone functions for data_governance
"""

###############################################################################
# CONSTANTS
###############################################################################

SUPPORTED_DATA_GOVERNANCE_BACKENDS = ["navigator"]
DATA_GOVERNANCE_OBJECT_TYPE_TABLE = "table"
DATA_GOVERNANCE_OBJECT_TYPE_VIEW = "view"

DATA_GOVERNANCE_PRE_REGISTRATION, DATA_GOVERNANCE_POST_REGISTRATION = list(range(2))

DATA_GOVERNANCE_DYNAMIC_TAG_PREFIX = "+"
DATA_GOVERNANCE_DYNAMIC_TAG_EXCLUSION_PREFIX = "-"
DATA_GOVERNANCE_DYNAMIC_TAG_RDBMS_NAME = "RDBMS_NAME"
VALID_DATA_GOVERNANCE_DYNAMIC_TAGS = [DATA_GOVERNANCE_DYNAMIC_TAG_RDBMS_NAME]

DATA_GOVERNANCE_DYNAMIC_PROPERTY_GOE_OBJECT_TYPE = "GOE_OBJECT_TYPE"
DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT = "GOE_SOURCE_RDBMS_OBJECT"
DATA_GOVERNANCE_DYNAMIC_PROPERTY_TARGET_RDBMS_OBJECT = "GOE_TARGET_RDBMS_OBJECT"
DATA_GOVERNANCE_DYNAMIC_PROPERTY_INITIAL_OPERATION_DATETIME = (
    "GOE_INITIAL_OPERATION_DATETIME"
)
DATA_GOVERNANCE_DYNAMIC_PROPERTY_LATEST_OPERATION_DATETIME = (
    "GOE_LATEST_OPERATION_DATETIME"
)
VALID_DATA_GOVERNANCE_DYNAMIC_PROPERTIES = [
    DATA_GOVERNANCE_DYNAMIC_PROPERTY_GOE_OBJECT_TYPE,
    DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT,
    DATA_GOVERNANCE_DYNAMIC_PROPERTY_TARGET_RDBMS_OBJECT,
]

DATA_GOVERNANCE_GOE_OBJECT_TYPE_BASE_TABLE = "Base Table"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_LOAD_TABLE = "Load Table"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_CONV_VIEW = "Data Conversion View"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_JOIN_VIEW = "Join Pushdown View"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_JOIN_MVIEW = "Join Pushdown Table"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_DELTA_VIEW = "Delta View"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_DELTA_TABLE = "Delta Table"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_INT_DELTA_VIEW = "Internal Delta View"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_COMP_BACKUP_TABLE = "Compaction Backup Table"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_SEQUENCE_TABLE = "Sequence Table"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_OFFLOAD_DB = "Offload Database"
DATA_GOVERNANCE_GOE_OBJECT_TYPE_LOAD_DB = "Load Database"
VALID_DATA_GOVERNANCE_GOE_OBJECT_TYPES = [
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_BASE_TABLE,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_LOAD_TABLE,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_CONV_VIEW,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_JOIN_VIEW,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_JOIN_MVIEW,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_DELTA_VIEW,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_DELTA_TABLE,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_INT_DELTA_VIEW,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_COMP_BACKUP_TABLE,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_SEQUENCE_TABLE,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_OFFLOAD_DB,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_LOAD_DB,
]

# constants categorising properties and tags by object type
DATA_GOVERNANCE_GOE_DB_OBJECT_TYPES = [
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_OFFLOAD_DB,
    DATA_GOVERNANCE_GOE_OBJECT_TYPE_LOAD_DB,
]

MUTED_DATA_GOVERNANCE_OBJECT_TAGS = []
MUTED_DATA_GOVERNANCE_OBJECT_PROPERTIES = []
MUTED_DATA_GOVERNANCE_DB_TAGS = [DATA_GOVERNANCE_DYNAMIC_TAG_RDBMS_NAME]
MUTED_DATA_GOVERNANCE_DB_PROPERTIES = [
    DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT,
    DATA_GOVERNANCE_DYNAMIC_PROPERTY_TARGET_RDBMS_OBJECT,
    DATA_GOVERNANCE_DYNAMIC_PROPERTY_LATEST_OPERATION_DATETIME,
]
