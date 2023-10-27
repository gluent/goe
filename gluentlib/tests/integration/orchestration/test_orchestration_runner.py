"""
TestOrchestrationRunner: Test library of entry points for orchestration commands.
We cannot test without making DB connections. These tests pick up an existing table and run a
non-execute reset Offload on them.
"""

from unittest import TestCase, main

from tests.integration.offload.unittest_functions import (
    build_current_options,
    get_real_frontend_schema_and_table,
)

from gluentlib.orchestration.orchestration_runner import OrchestrationRunner
from gluentlib.offload.offload_messages import OffloadMessages


class TestOrchestrationRunner(TestCase):
    """
    TestOrchestrationRunner: Test library of entry points for orchestration commands.
    We cannot test without making DB connections. These tests pick up an existing table and run a
    non-execute reset Offload on them.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.messages = OffloadMessages()
        self.config = build_current_options()

    def test_offload(self):
        db, table = get_real_frontend_schema_and_table(
            "CHANNELS", self.config, messages=self.messages
        )
        params = {"owner_table": f"{db}.{table}", "reset_backend_table": True}
        status = OrchestrationRunner(dry_run=True).offload(params)
        self.assertTrue(status)


if __name__ == "__main__":
    main()
