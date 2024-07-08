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

import sys

from goe.goe import (
    get_log_fh,
    get_log_fh_name,
    init,
    init_log,
    log,
    log_command_line,
    log_close,
    log_timestamp,
    normalise_options,
    version,
    OFFLOAD_OP_NAME,
    verbose,
)
from goe.config.orchestration_config import OrchestrationConfig
from goe.orchestration.orchestration_runner import OrchestrationRunner
from goe.util.goe_log import log_exception


def offload_by_cli(options, messages_override=None):
    """
    CLI entrypoint for Offload.

    messages_override: Only used during testing to access messages object.
    """
    init(options)
    init_log("offload_%s" % options.owner_table)

    try:
        log("")
        log("Offload v%s" % version(), ansi_code="underline")
        log("Log file: %s" % get_log_fh_name())
        log("")
        log_command_line()

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

        log_close()
    except Exception as exc:
        log("Exception caught at top-level", ansi_code="red")
        log_timestamp()
        log_exception(exc, log_fh=get_log_fh(), options=options)
        log_close()
        sys.exit(1)
