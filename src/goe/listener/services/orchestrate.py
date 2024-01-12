# Standard Library
import logging

# GOE
from goe.orchestration.orchestration_runner import OrchestrationRunner

logger = logging.getLogger(__name__)


orchestration_runner = OrchestrationRunner(suppress_stdout=True)
