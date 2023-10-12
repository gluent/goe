"""
Functions used as entry points for Orchestration CLI commands.
LICENSE_TEXT
"""

import sys

from gluent import OffloadException, OptionError,\
    init, init_log, get_log_fh, get_log_fh_name, log, log_command_line, normalise_options, version, \
    OFFLOAD_OP_NAME, verbose
from gluentlib.orchestration.orchestration_runner import OrchestrationRunner
from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.util.gluent_log import log_exception
from schema_sync import get_schema_sync_opts, normalise_schema_sync_options


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


def schema_sync_by_cli(options):
    normalise_schema_sync_options(options)

    config_overrides = {'execute': options.execute,
                        'verbose': options.verbose,
                        'vverbose': options.vverbose}

    return OrchestrationRunner(config_overrides=config_overrides).schema_sync(options, reuse_log=True)


def schema_sync_run():
    options = None
    try:
        opt = get_schema_sync_opts()
        options, _ = opt.parse_args()

        init(options)
        init_log('schema_sync')

        log('')
        log('Schema Sync v%s' % version(), ansi_code='underline')
        log('Log file: %s' % get_log_fh_name())
        log('', verbose)
        log_command_line()

        status = schema_sync_by_cli(options)

        sys.exit(status)

    except OptionError as exc:
        log('Option error: %s' % exc.detail, ansi_code='red')
        log('')
        opt.print_help()
        sys.exit(1)
    except Exception as exc:
        log('Exception caught at top-level', ansi_code='red')
        log_exception(exc, log_fh=get_log_fh(), options=options)
        sys.exit(1)
