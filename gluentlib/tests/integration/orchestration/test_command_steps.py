"""
TestCommandSteps: Unit test constants in command_steps are sound.
    1) Within the module.
    2) When compared to Gluent Repo data.
"""
from unittest import TestCase, main

from tests.integration.offload.unittest_functions import build_current_options

from gluentlib.offload.offload_messages import OffloadMessages
from gluentlib.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from tests.unit.orchestration.test_command_steps import (
    TestCommandSteps,
    KNOWN_MISSING_STEPS,
)


class TestCommandStepsIntegration(TestCommandSteps):
    """
    TestCommandSteps: Unit test constants in command_steps are sound when compared to Gluent Repo data.
    """

    def test_command_steps_repo(self):
        """Test the constants are present in the repo."""
        try:
            config = build_current_options()
            messages = OffloadMessages()
            client = orchestration_repo_client_factory(config, messages)
            codes = client.get_command_step_codes()
            step_constants = self._get_step_constants()
            missing_rows = set(step_constants) - set(codes)
            unexpected_missing_rows = missing_rows - set(KNOWN_MISSING_STEPS)
            self.assertEqual(unexpected_missing_rows, set([]))
            steps_that_should_be_missing = set(KNOWN_MISSING_STEPS).intersection(
                set(codes)
            )
            self.assertEqual(steps_that_should_be_missing, set([]))
        except NotImplementedError:
            pass


if __name__ == "__main__":
    main()
