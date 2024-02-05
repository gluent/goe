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

""" Constants and global function for triggering exceptions in Orchestration commands for test purposes.
"""


class ForcedOrchestrationException(Exception):
    pass


###########################################################################
# CONSTANTS
###########################################################################

EXCEPTION_TRIGGERED_TEXT = "Exception triggered for token"

INCREMENTAL_MERGE_POST_INSERT = "IU_IUD_POST_INSERT"
INCREMENTAL_MERGE_POST_UPDATE = "IU_IUD_POST_UPDATE"
INCREMENTAL_MERGE_POST_DELETE = "IU_IUD_POST_DELETE"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def trigger_exception(token, orchestration_config):
    """If a token is in orchestration_config then trigger an artifical exception.
    orchestration_config can be an options namespace or the str defined in error_on_token.
    """
    if isinstance(orchestration_config, str):
        error_on_token = orchestration_config
    else:
        error_on_token = orchestration_config.error_on_token
    if token and token == error_on_token:
        raise ForcedOrchestrationException(
            f"{EXCEPTION_TRIGGERED_TEXT}: {error_on_token}"
        )
