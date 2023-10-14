"""
TestOrchestrationRunner: Test library of entry points for orchestration commands.
We cannot test without making DB connections. These tests pick up an existing table and run a
non-execute reset Offload on them.
"""

from unittest import TestCase, main

from tests.offload.unittest_functions import build_current_options, get_real_frontend_schema_and_table

from gluentlib.incremental.lite.ora_inc_extract import CHANGELOG
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
        db, table = get_real_frontend_schema_and_table('CHANNELS', self.config, messages=self.messages)
        params = {'owner_table': f'{db}.{table}',
                  'reset_backend_table': True}
        status = OrchestrationRunner(dry_run=True).offload(params)
        self.assertTrue(status)

    def test_iu_enable(self):
        db, table = get_real_frontend_schema_and_table('CHANNELS', self.config, messages=self.messages)
        params = {'owner_table': f'{db}.{table}',
                  'incremental_extraction_method': CHANGELOG}
        OrchestrationRunner(dry_run=True).iu_enable(params)
        # Nothing for us to assert other than we got here successfully.

