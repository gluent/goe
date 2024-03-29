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

from unittest import main

from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.offload_messages import OffloadMessages
from tests.integration.test_functions import (
    build_current_options,
    get_default_test_user,
    run_setup_ddl,
)
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)
from tests.testlib.test_framework.test_functions import get_test_messages
from tests.unit.offload.test_frontend_api import TestFrontendApi


DIM_NAME = "INTEG_FRONTEND_API_DIM"
FACT_NAME = "INTEG_FRONTEND_API_FACT"


class TestCurrentFrontendApi(TestFrontendApi):
    def setUp(self):
        self.connect_to_frontend = True
        self.config = self._build_current_options()
        messages = OffloadMessages()
        self.api = frontend_api_factory(
            self.db_type,
            self.config,
            messages,
            dry_run=True,
            do_not_connect=bool(not self.connect_to_frontend),
            trace_action="TestCurrentFrontendApi",
        )

        self.test_api = frontend_testing_api_factory(
            self.db_type,
            self.config,
            messages,
            dry_run=False,
            do_not_connect=bool(not self.connect_to_frontend),
            trace_action="frontend_api(TestCurrentFrontendApi)",
        )

        self.db = get_default_test_user()
        self.table = DIM_NAME
        self.part_table = FACT_NAME

    def _build_current_options(self):
        orchestration_options = build_current_options()
        self.db_type = orchestration_options.db_type
        return orchestration_options

    def _create_test_tables(self):
        messages = get_test_messages(self.config, "TestCurrentFrontendApi")
        # Setup non-partitioned table
        run_setup_ddl(
            self.config,
            self.test_api,
            messages,
            self.test_api.standard_dimension_frontend_ddl(self.db, self.table),
        )

        # Setup partitioned table
        run_setup_ddl(
            self.config,
            self.test_api,
            messages,
            self.test_api.sales_based_fact_create_ddl(
                self.db, self.part_table, simple_partition_names=True
            ),
        )

    def test_full_api_on_current_frontend(self):
        self._create_test_tables()
        self._run_all_tests()


if __name__ == "__main__":
    main()
