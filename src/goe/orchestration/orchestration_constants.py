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

"""
Constants used across multiple Orchestration commands.
"""

# Try not to import any modules in here, this module is widely imported and we've previously had subtle side effects

PRODUCT_NAME_GOE = "GOE"
PRODUCT_NAME_GEL = "GOE Listener"

# Command type codes, matches up with data in GOE_REPO.COMMAND_TYPES table.
COMMAND_OFFLOAD = "OFFLOAD"
COMMAND_OFFLOAD_JOIN = "OFFJOIN"
COMMAND_PRESENT = "PRESENT"
COMMAND_PRESENT_JOIN = "PRESJOIN"
COMMAND_IU_ENABLE = "IUENABLE"
COMMAND_IU_DISABLE = "IUDISABLE"
COMMAND_IU_EXTRACTION = "IUEXTRACT"
COMMAND_IU_COMPACTION = "IUCOMPACT"
COMMAND_DIAGNOSE = "DIAGNOSE"
COMMAND_OSR = "OSR"
COMMAND_SCHEMA_SYNC = "SCHSYNC"
COMMAND_TEST = "TEST"

ALL_COMMAND_CODES = [
    COMMAND_OFFLOAD,
    COMMAND_OFFLOAD_JOIN,
    COMMAND_PRESENT,
    COMMAND_PRESENT_JOIN,
    COMMAND_IU_ENABLE,
    COMMAND_IU_DISABLE,
    COMMAND_IU_EXTRACTION,
    COMMAND_IU_COMPACTION,
    COMMAND_DIAGNOSE,
    COMMAND_OSR,
    COMMAND_SCHEMA_SYNC,
    COMMAND_TEST,
]

# Command status codes, matches up with data in GOE_REPO.STATUS table.
COMMAND_SUCCESS = "SUCCESS"
COMMAND_ERROR = "ERROR"
COMMAND_EXECUTING = "EXECUTING"
