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

""" TestOrchestrationConfig: Unit test library to test TestOrchestrationConfig class
"""
import os
from unittest import TestCase, main, mock

from goe.config.orchestration_config import (
    OrchestrationConfig,
    EXPECTED_CONFIG_ARGS,
)
from goe.config.config_validation_functions import (
    OrchestrationConfigException,
    verify_json_option,
)
from tests.unit.test_functions import FAKE_ORACLE_BQ_ENV


class TestOrchestrationConfig(TestCase):
    def test_verify_json_option(self):
        generic_json = '{"attrib1": "String", "attrib2": 1024, "attrib3": 1.024}'
        verify_json_option("TEST_JSON", generic_json)
        sample_rdbms_parameters_json = (
            '{"cell_offload_processing": "false", "\\"_serial_direct_read\\"": "true"}'
        )
        verify_json_option("TEST_JSON", sample_rdbms_parameters_json)
        sample_spark_parameters_json = (
            '{"spark.driver.memory": "512M", "spark.executor.memory": "1024M"}'
        )
        verify_json_option("TEST_JSON", sample_spark_parameters_json)
        for bad_json in [
            '{cell_offload_processing: "false"}',
            '{"cell_offload_processing": False}',
        ]:
            self.assertRaises(
                OrchestrationConfigException,
                lambda: verify_json_option("TEST_JSON", bad_json),
            )

    def test_as_defaults(self):
        k = mock.patch.dict(os.environ, FAKE_ORACLE_BQ_ENV)
        k.start()
        config = OrchestrationConfig.as_defaults()
        k.stop()
        # Check that every expected attribute is represented
        self.assertFalse(
            bool(set(EXPECTED_CONFIG_ARGS) - vars(config).keys()),
            set(EXPECTED_CONFIG_ARGS) - vars(config).keys(),
        )

    def test_from_dict(self):
        k = mock.patch.dict(os.environ, FAKE_ORACLE_BQ_ENV)
        k.start()
        config = OrchestrationConfig.from_dict({})
        k.stop()
        self.assertFalse(
            bool(set(EXPECTED_CONFIG_ARGS) - vars(config).keys()),
            set(EXPECTED_CONFIG_ARGS) - vars(config).keys(),
        )
        k.start()
        config = OrchestrationConfig.from_dict({"vverbose": True})
        k.stop()
        self.assertFalse(
            bool(set(EXPECTED_CONFIG_ARGS) - vars(config).keys()),
            set(EXPECTED_CONFIG_ARGS) - vars(config).keys(),
        )
        self.assertTrue(config.vverbose)


if __name__ == "__main__":
    main()
