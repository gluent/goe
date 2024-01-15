#! /usr/bin/env python3
""" Constants and global function for triggering exceptions in Orchestration commands for test purposes.
    LICENSE_TEXT
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
