# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
TestCommandSteps: Unit test constants in command_steps are sound.
    1) Within the module.
    2) When compared to GOE Repo data.
"""
from unittest import main

from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from tests.integration.test_functions import (
    cached_current_options,
)
from tests.testlib.test_framework.test_functions import (
    get_test_messages,
)
from tests.unit.orchestration.test_command_steps import (
    TestCommandSteps,
    KNOWN_MISSING_STEPS,
)


class TestCommandStepsIntegration(TestCommandSteps):
    """
    TestCommandSteps: Unit test constants in command_steps are sound when compared to GOE Repo data.
    """

    def test_command_steps_repo(self):
        """Test the constants are present in the repo."""
        try:
            config = cached_current_options()
            messages = get_test_messages(config, "TestCommandStepsIntegration")
            client = orchestration_repo_client_factory(
                config,
                messages,
                trace_action="repo_client(test_command_steps_repo)",
            )
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
