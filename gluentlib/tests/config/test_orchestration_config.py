""" TestOrchestrationConfig: Unit test library to test TestOrchestrationConfig class
"""
from unittest import TestCase, main

from gluentlib.config.orchestration_config import OrchestrationConfig, EXPECTED_CONFIG_ARGS
from gluentlib.config.config_validation_functions import OrchestrationConfigException, verify_json_option


class TestOrchestrationConfig(TestCase):

    def test_verify_json_option(self):
        generic_json = '{"attrib1": "String", "attrib2": 1024, "attrib3": 1.024}'
        verify_json_option('TEST_JSON', generic_json)
        sample_rdbms_parameters_json = '{"cell_offload_processing": "false", "\\"_serial_direct_read\\"": "true"}'
        verify_json_option('TEST_JSON', sample_rdbms_parameters_json)
        sample_spark_parameters_json = '{"spark.driver.memory": "512M", "spark.executor.memory": "1024M"}'
        verify_json_option('TEST_JSON', sample_spark_parameters_json)
        for bad_json in ['{cell_offload_processing: "false"}',
                         '{"cell_offload_processing": False}']:
            self.assertRaises(OrchestrationConfigException,
                              lambda: verify_json_option('TEST_JSON', bad_json))

    def test_as_defaults(self):
        config = OrchestrationConfig.as_defaults()
        # Check that every expected attribute is represented
        self.assertFalse(bool(set(EXPECTED_CONFIG_ARGS) - vars(config).keys()),
                         set(EXPECTED_CONFIG_ARGS) - vars(config).keys())

    def test_from_dict(self):
        config = OrchestrationConfig.from_dict({})
        self.assertFalse(bool(set(EXPECTED_CONFIG_ARGS) - vars(config).keys()),
                         set(EXPECTED_CONFIG_ARGS) - vars(config).keys())
        config = OrchestrationConfig.from_dict({'vverbose': True})
        self.assertFalse(bool(set(EXPECTED_CONFIG_ARGS) - vars(config).keys()),
                         set(EXPECTED_CONFIG_ARGS) - vars(config).keys())
        self.assertTrue(config.vverbose)


if __name__ == '__main__':
    main()
