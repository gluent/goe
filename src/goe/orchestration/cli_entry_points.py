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
Functions used as entry points for Orchestration CLI commands.
"""

from goe.goe import (
    init,
    init_log,
    get_log_fh,
    log,
    normalise_options,
    OFFLOAD_OP_NAME,
    verbose,
)
from goe.orchestration.orchestration_runner import OrchestrationRunner
from goe.config.orchestration_config import OrchestrationConfig


def offload_by_cli(options, messages_override=None):
    """
    The offload CLI script is an enry point for multiple actions, this function directs the work based on options.
    Actions we may call:
        Offload
        Incremental Update Setup/Clean Up
        Incremental Update Extraction
        Incremental Update Compaction
    messages_override: Only present so tests can access messages object.
    """
    if not get_log_fh():
        init(options)
        init_log("offload_%s" % options.owner_table)

    options.operation_name = OFFLOAD_OP_NAME
    normalise_options(options)

    config_overrides = {
        "verbose": options.verbose,
        "vverbose": options.vverbose,
        "offload_transport_dsn": options.offload_transport_dsn,
        "error_on_token": options.error_on_token,
    }
    config = OrchestrationConfig.from_dict(config_overrides)

    config.log_connectivity_messages(lambda m: log(m, detail=verbose))

    OrchestrationRunner(config_overrides=config_overrides).offload(
        options, reuse_log=True, messages_override=messages_override
    )
