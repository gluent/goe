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

from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.offload import OffloadException
from goe.offload.offload_constants import DBTYPE_SPARK
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_messages import OffloadMessages
from tests.integration.test_functions import (
    build_current_options,
    get_default_test_user,
    run_offload,
    run_setup_ddl,
)
from tests.testlib.test_framework.factory.backend_testing_api_factory import (
    backend_testing_api_factory,
)
from tests.testlib.test_framework.factory.frontend_testing_api_factory import (
    frontend_testing_api_factory,
)
from tests.testlib.test_framework.test_functions import get_test_messages
from tests.unit.offload.test_backend_api import TestBackendApi


DIM_NAME = "INTEG_BACKEND_API_DIM"
FACT_NAME = "INTEG_BACKEND_API_FACT"


class TestCurrentBackendApi(TestBackendApi):
    def setUp(self):
        self.connect_to_backend = True
        self.config = self._build_current_options()
        self.test_messages = get_test_messages(self.config, "TestCurrentBackendApi")
        self.api = backend_api_factory(
            self.target,
            self.config,
            self.test_messages,
            dry_run=True,
            do_not_connect=bool(not self.connect_to_backend),
        )
        self.test_api = backend_testing_api_factory(
            self.target,
            self.config,
            self.test_messages,
            dry_run=True,
            do_not_connect=bool(not self.connect_to_backend),
        )
        self.schema = get_default_test_user()
        self.db = data_db_name(self.schema, self.config)
        self.db, self.table, self.part_table = convert_backend_identifier_case(
            self.config, self.db, DIM_NAME, FACT_NAME
        )

    def _build_current_options(self):
        orchestration_options = build_current_options()
        self.target = orchestration_options.target
        return orchestration_options

    def _create_test_tables(self):
        """To create backend test tables we need to create them in the frontend and offload them."""
        messages = OffloadMessages()
        frontend_api = frontend_testing_api_factory(
            self.config.db_type,
            self.config,
            messages,
            dry_run=False,
            trace_action="frontend_api(TestCurrentBackendApi)",
        )
        # Setup non-partitioned table
        run_setup_ddl(
            self.config,
            frontend_api,
            messages,
            frontend_api.standard_dimension_frontend_ddl(self.schema, DIM_NAME),
        )
        # Ignore return status, if the table has already been offloaded previously then we'll re-use it.
        try:
            run_offload(
                {
                    "owner_table": self.schema + "." + self.table,
                    "create_backend_db": True,
                }
            )
        except OffloadException:
            # If this one fails then we let the exception bubble up.
            run_offload(
                {
                    "owner_table": self.schema + "." + self.table,
                    "reset_backend_table": True,
                    "create_backend_db": True,
                }
            )

        # Setup partitioned table
        run_setup_ddl(
            self.config,
            frontend_api,
            messages,
            frontend_api.sales_based_fact_create_ddl(
                self.schema, self.part_table, simple_partition_names=True
            ),
        )
        # Ignore return status, if the table has already been offloaded previously then we'll re-use it.
        try:
            run_offload({"owner_table": self.schema + "." + self.part_table})
        except OffloadException:
            # If this one fails then we let the exception bubble up.
            run_offload(
                {
                    "owner_table": self.schema + "." + self.part_table,
                    "reset_backend_table": True,
                }
            )

    def test_full_api_on_current_backend(self):
        self._create_test_tables()
        self._run_all_tests()


class TestConnectedSparkBackendApi(TestBackendApi):
    def setUp(self):
        self.connect_to_backend = True
        self.config = self._build_current_options()
        if self._thrift_configured(self.config):
            super(TestConnectedSparkBackendApi, self).setUp()

    def _thrift_configured(self, orchestration_options):
        return bool(
            orchestration_options.offload_transport_spark_thrift_host
            and orchestration_options.offload_transport_spark_thrift_port
        )

    def _build_current_options(self):
        orchestration_options = build_current_options()
        orchestration_options.target = DBTYPE_SPARK
        self.target = orchestration_options.target
        return orchestration_options

    def test_full_api_on_current_backend(self):
        if self._thrift_configured(self.config):
            self._run_all_tests()


if __name__ == "__main__":
    main()
