from gluent import OffloadOperation
from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.offload.offload_messages import OffloadMessages

from tests.unit.test_functions import build_current_options, FAKE_ORACLE_BQ_ENV


def build_current_options():
    return OrchestrationConfig.from_dict({"verbose": False, "execute": False})


def build_offload_operation(operation_dict=None, options=None, messages=None):
    if options:
        offload_options = options
    else:
        offload_options = build_current_options()
    if messages:
        offload_messages = messages
    else:
        offload_messages = OffloadMessages()
    if not operation_dict:
        operation_dict = {"owner_table": "x.y"}
    offload_operation = OffloadOperation.from_dict(
        operation_dict, offload_options, offload_messages
    )
    return offload_operation
