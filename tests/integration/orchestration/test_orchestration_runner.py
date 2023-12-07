"""
TestOrchestrationRunner: Test library of entry points for orchestration commands.
We cannot test without making DB connections. These tests pick up an existing table and run a
non-execute reset Offload on them.
"""

from unittest import TestCase, main

from goe.orchestration.orchestration_runner import OrchestrationRunner

from tests.integration.test_functions import (
    cached_current_options,
    get_default_test_user,
    run_setup_ddl,
)
from tests.testlib.test_framework.test_functions import (
    get_frontend_testing_api,
    get_test_messages,
)


TABLE_NAME = "TEST_ORCH_RUNNER_TABLE"


class TestOrchestrationRunner(TestCase):
    """
    TestOrchestrationRunner: Test library of entry points for orchestration commands.
    We cannot test without making DB connections. This test create a table and run a
    non-execute reset Offload on them.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = cached_current_options()
        self.messages = get_test_messages(self.config, "TestOrchestrationRunner")

    def test_offload(self):
        schema = get_default_test_user()
        frontend_api = get_frontend_testing_api(
            self.config, self.messages, trace_action="TestOrchestrationRunner"
        )

        # Setup
        run_setup_ddl(
            self.config,
            frontend_api,
            self.messages,
            frontend_api.standard_dimension_frontend_ddl(schema, TABLE_NAME),
        )

        params = {"owner_table": f"{schema}.{TABLE_NAME}", "reset_backend_table": True}
        status = OrchestrationRunner(dry_run=True).offload(params)
        self.assertTrue(status)


if __name__ == "__main__":
    main()
