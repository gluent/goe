# Standard Library
import logging

# Gluent
from gluentlib.orchestration.orchestration_runner import OrchestrationRunner

logger = logging.getLogger(__name__)


orchestration_runner = OrchestrationRunner(suppress_stdout=True)
