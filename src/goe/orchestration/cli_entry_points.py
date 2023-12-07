"""
Functions used as entry points for Orchestration CLI commands.
LICENSE_TEXT
"""

from gluent import init, init_log, get_log_fh, log, normalise_options, OFFLOAD_OP_NAME, verbose
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
        init_log('offload_%s' % options.owner_table)

    options.operation_name = OFFLOAD_OP_NAME
    normalise_options(options)

    config_overrides = {'execute': options.execute,
                        'verbose': options.verbose,
                        'vverbose': options.vverbose,
                        'offload_transport_dsn': options.offload_transport_dsn,
                        'error_on_token': options.error_on_token}
    config = OrchestrationConfig.from_dict(config_overrides)

    config.log_connectivity_messages(lambda m: log(m, detail=verbose))

    OrchestrationRunner(config_overrides=config_overrides).offload(options, reuse_log=True,
                                                                   messages_override=messages_override)
