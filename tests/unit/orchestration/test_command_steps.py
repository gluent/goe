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
from unittest import TestCase, main

from goe.orchestration import command_steps


# Console phase 1 only adds steps to the repo for Offload/Present. In time the OSR/Schema Sync steps
# below will be added to the repo. KNOWN_MISSING_STEPS serves two purposes:
# 1) Prevents test_command_steps_repo from failing due to known missing steps.
# 2) Will cause a failure when these steps are added to the repo so we know to remove them from the list.
KNOWN_MISSING_STEPS = [
    command_steps.STEP_BACKEND_CONFIG,
    command_steps.STEP_BACKEND_LOGS,
    command_steps.STEP_BACKEND_QUERY_LOGS,
    command_steps.STEP_GOE_LOGS,
    command_steps.STEP_GOE_PERMISSIONS,
    command_steps.STEP_GOE_PROCESSES,
    command_steps.STEP_GOE_TABLE_METADATA,
    command_steps.STEP_NORMALIZE_INCLUDES,
    command_steps.STEP_OSR_DEMO_DATA,
    command_steps.STEP_OSR_FIND_TABLES,
    command_steps.STEP_OSR_FETCH_DATA,
    command_steps.STEP_OSR_GENERATE_REPORT,
    command_steps.STEP_OSR_PROCESS_DATA,
    command_steps.STEP_PROCESS_TABLE_CHANGES,
    command_steps.STEP_REPORT_EXCEPTIONS,
    command_steps.STEP_UNITTEST_ERROR_AFTER,
    command_steps.STEP_UNITTEST_ERROR_BEFORE,
    command_steps.STEP_UNITTEST_SKIP,
]


class TestCommandSteps(TestCase):
    """
    TestCommandSteps: Unit test constants in command_steps are sound within the module.
    """

    def _get_step_constants(self):
        return [
            v
            for k, v in vars(command_steps).items()
            if k.startswith("STEP_")
            and k != "STEP_TITLES"
            and k not in KNOWN_MISSING_STEPS
        ]

    def test_command_steps_internal(self):
        """Test the constants are sound within the module."""
        try:
            step_codes = self._get_step_constants()
            for step in step_codes:
                self.assertIn(step, command_steps.STEP_TITLES)
                self.assertIsInstance(command_steps.STEP_TITLES[step], str)
                self.assertIsInstance(command_steps.step_title(step), str)
        except NotImplementedError:
            pass


if __name__ == "__main__":
    main()
