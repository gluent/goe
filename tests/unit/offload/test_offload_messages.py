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

from unittest import TestCase, main

from goe.config import orchestration_defaults
from goe.offload.offload_messages import (
    OffloadMessages,
    OffloadMessagesForcedException,
    VERBOSE,
    VVERBOSE,
)
from goe.orchestration import command_steps, orchestration_constants


class FakeOpts(object):
    def __init__(self, option_modifiers=None):
        if option_modifiers:
            assert isinstance(option_modifiers, dict)

        self.quiet = orchestration_defaults.quiet_default()
        self.verbose = orchestration_defaults.verbose_default()
        self.vverbose = orchestration_defaults.vverbose_default()
        self.ansi = orchestration_defaults.ansi_default()
        self.suppress_stdout = orchestration_defaults.suppress_stdout_default()

        if option_modifiers:
            for k, v in option_modifiers.items():
                setattr(self, k, v)

    def as_dict(self):
        return vars(self)


class TestOffloadMessages(TestCase):
    def _std_messages_actions(self, messages):
        """Shake down messages calls.
        It would be better if we actually checked what was written a log file vs stdout but, at the moment,
        all we are doing is ensuring no unexpected exceptions.
        """
        messages.log("msg")
        messages.log("vmsg", detail=VERBOSE)
        messages.log("vvmsg", detail=VVERBOSE)
        messages.info("info", detail=VVERBOSE)
        messages.info("vinfo", detail=VERBOSE)
        messages.info("vvinfo", detail=VVERBOSE)
        messages.debug("debug", detail=VVERBOSE)
        messages.debug("vdebug", detail=VERBOSE)
        messages.debug("vvdebug", detail=VVERBOSE)
        messages.notice("a-notice")
        self.assertIn("a-notice", messages.get_notices())
        self.assertIn("a-notice", messages.get_messages())
        messages.warning("a-warning")
        self.assertIn("a-warning", messages.get_warnings())
        self.assertIn("a-warning", messages.get_messages())
        messages.event("an-event")
        self.assertIn("an-event", messages.get_events())

    def test_from_options(self):
        with open("/dev/null", "a") as fh:
            messages = OffloadMessages.from_options(FakeOpts(), log_fh=fh)
            self._std_messages_actions(messages)

    def test_from_options_dict(self):
        with open("/dev/null", "a") as fh:
            messages = OffloadMessages.from_options_dict(
                FakeOpts().as_dict(), log_fh=fh
            )
            self._std_messages_actions(messages)

    def test_offload_step(self):
        def do_nothing():
            pass

        def do_divzero():
            return 1 / 0

        with open("/dev/null", "a") as fh:
            # Sanity check with no modifiers, all steps recorded
            messages = OffloadMessages.from_options(FakeOpts(), log_fh=fh)
            messages.offload_step(
                command_steps.STEP_MESSAGES,
                do_nothing,
                command_type=orchestration_constants.COMMAND_TEST,
            )
            messages.offload_step(
                command_steps.STEP_UNITTEST_SKIP,
                do_nothing,
                command_type=orchestration_constants.COMMAND_TEST,
            )
            messages.offload_step(
                command_steps.STEP_UNITTEST_ERROR_BEFORE,
                do_nothing,
                command_type=orchestration_constants.COMMAND_TEST,
            )
            messages.offload_step(
                command_steps.STEP_UNITTEST_ERROR_AFTER,
                do_nothing,
                command_type=orchestration_constants.COMMAND_TEST,
            )
            # Now check that step interaction works
            messages_title = command_steps.step_title(command_steps.STEP_MESSAGES)
            skip_title = command_steps.step_title(command_steps.STEP_UNITTEST_SKIP)
            error_before_title = command_steps.step_title(
                command_steps.STEP_UNITTEST_ERROR_BEFORE
            )
            error_after_title = command_steps.step_title(
                command_steps.STEP_UNITTEST_ERROR_AFTER
            )
            messages = OffloadMessages.from_options(FakeOpts(), log_fh=fh)
            messages.setup_offload_step(
                skip=[skip_title.replace(" ", "_").lower()],
                error_before_step=error_before_title.replace(" ", "_").lower(),
                error_after_step=error_after_title.replace(" ", "_").lower(),
            )
            # Normal step should be recorded in steps
            messages.offload_step(
                command_steps.STEP_MESSAGES,
                do_nothing,
                command_type=orchestration_constants.COMMAND_TEST,
            )
            self.assertIn(messages_title, messages.steps)
            # Skipped step should not be in recorded steps
            messages.offload_step(
                command_steps.STEP_UNITTEST_SKIP,
                do_nothing,
                command_type=orchestration_constants.COMMAND_TEST,
            )
            self.assertNotIn(skip_title, messages.steps)
            # Check test assertion thrown
            self.assertRaises(
                OffloadMessagesForcedException,
                lambda: messages.offload_step(
                    command_steps.STEP_UNITTEST_ERROR_BEFORE,
                    do_nothing,
                    command_type=orchestration_constants.COMMAND_TEST,
                ),
            )
            self.assertNotIn(error_before_title, messages.steps)
            self.assertRaises(
                OffloadMessagesForcedException,
                lambda: messages.offload_step(
                    command_steps.STEP_UNITTEST_ERROR_AFTER,
                    do_nothing,
                    command_type=orchestration_constants.COMMAND_TEST,
                ),
            )
            self.assertIn(error_after_title, messages.steps)
            # Check flow when a genuine exception is encountered, we should still get that exception and
            # not suffer a subsequent issue in our handling code
            self.assertRaises(
                ZeroDivisionError,
                lambda: messages.offload_step(
                    command_steps.STEP_MESSAGES,
                    do_divzero,
                    command_type=orchestration_constants.COMMAND_TEST,
                ),
            )


if __name__ == "__main__":
    main()
