from datetime import datetime
from unittest import main

from tests.unit.offload.unittest_functions import get_real_frontend_schema_and_table

from gluentlib.offload.factory.frontend_api_factory import frontend_api_factory
from gluentlib.offload.frontend_api import QueryParameter
from gluentlib.offload.offload_messages import OffloadMessages
from tests.integration.test_functions import build_current_options
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)
from tests.unit.offload.test_frontend_api import TestFrontendApi


class TestCurrentFrontendApi(TestFrontendApi):
    def setUp(self):
        self.connect_to_frontend = True
        self.config = self._build_current_options()
        super().setUp()

    def _build_current_options(self):
        orchestration_options = build_current_options()
        self.db_type = orchestration_options.db_type
        return orchestration_options

    def test_full_api_on_current_frontend(self):
        self._run_all_tests()


if __name__ == "__main__":
    main()
