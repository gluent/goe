#! /usr/bin/env python3
""" Run Python unittest test set.
    LICENSE_TEXT
"""

import unittest
import sys

from gluentlib.filesystem.gluent_dfs import OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER
from gluentlib.offload.offload_constants import HADOOP_BASED_BACKEND_DISTRIBUTIONS
from gluentlib.offload.offload_messages import VERBOSE, VVERBOSE

from testlib.test_framework import test_constants
from testlib.test_framework.test_functions import test_teamcity_endtest, test_teamcity_endtestsuite,\
    test_teamcity_failtest, test_teamcity_starttest, test_teamcity_starttestsuite

from tests.conductor.test_hybrid_view_service import TestHybridViewService
from tests.config.test_orchestration_config import TestOrchestrationConfig
from tests.filesystem.test_gluent_dfs import TestCliHdfs, TestGluentAzure, TestGluentGcs, TestGluentS3, TestWebHdfs,\
    TestCurrentDfsExecuteMode
from tests.offload import test_predicate_offload
from tests.offload.test_backend_api import TestBigQueryBackendApi, TestCurrentBackendApi, TestHiveBackendApi,\
    TestImpalaBackendApi, TestSnowflakeBackendApi, TestSynapseBackendApi, TestSparkThriftBackendApi,\
    TestConnectedSparkBackendApi
from tests.offload.test_backend_table import TestCurrentBackendTable
from tests.offload.test_column_metadata import TestColumnMetadata
from tests.offload.test_columns import TestColumns
from tests.offload.test_data_type_mappings import TestAvroDataTypeMappings, TestBackendBigQueryDataTypeMappings,\
    TestBackendHiveDataTypeMappings, TestBackendImpalaDataTypeMappings, TestBackendSnowflakeDataTypeMappings,\
    TestBackendSynapseDataTypeMappings, TestMSSQLDataTypeMappings, TestNetezzaDataTypeMappings,\
    TestOracleDataTypeMappings, TestParquetDataTypeMappings
from tests.offload.test_format_literal import TestFormatLiteral
from tests.offload.test_frontend_api import TestOracleFrontendApi, TestCurrentFrontendApi, TestTeradataFrontendApi
from tests.offload.test_offload_functions import TestOffloadFunctions
from tests.offload.test_offload_messages import TestOffloadMessages
from tests.offload.test_offload_source_table import TestCurrentOffloadSourceTable, TestMSSQLOffloadSourceTable, \
    TestNetezzaOffloadSourceTable, TestOracleOffloadSourceTable
from tests.offload.test_offload_transport_functions import TestOffloadTransportFunctions
from tests.offload.test_offload_transport_rdbms_api import TestOffloadTransportRdbmsApi
from tests.offload.test_offload_metadata_functions import TestOffloadMetadataFunctions
from tests.offload.test_operation_modules import TestOperationDataTypeControls, TestOperationNotNullColumns
from tests.offload.test_staging_file import TestStagingFile
from tests.offload.test_synthetic_partition_literal import TestSyntheticPartitionLiteral
from tests.offload.test_teradata_partition_expression import TestTeradataPartitionExpression
from tests.orchestration.test_command_steps import TestCommandSteps
from tests.persistence.test_orchestration_metadata import TestOrchestrationMetadata
from tests.persistence.test_orchestration_repo_client import TestOrchestrationRepoClient
from tests.util.test_avro_encoder import TestAvroEncoder
from tests.util.test_hs2_connection import TestHs2Connection
from tests.util.test_misc_functions import TestMiscFunctions
from tests.util.test_orchestration_lock import TestOrchestrationLock
from tests.util.test_parquet_encoder import TestParquetEncoder
from tests.util.test_password_tools import TestPasswordTools
from tests.util.test_simple_timer import TestSimpleTimer
from testlib.tests.test_framework.test_backend_testing_api import TestBigQueryBackendTestingApi,\
    TestCurrentBackendTestingApi, TestHiveBackendTestingApi, TestImpalaBackendTestingApi,\
    TestSnowflakeBackendTestingApi, TestSynapseBackendTestingApi
from testlib.tests.test_framework.test_frontend_testing_api import TestMSSQLFrontendTestingApi,\
    TestOracleFrontendTestingApi, TestTeradataFrontendTestingApi, TestCurrentFrontendTestingApi
from testlib.tests.test_framework.test_test_value_generators import TestTestValueGenerators


def run_python_unit_tests(options, orchestration_config, messages, should_run_test_f):

    def log(msg, detail=None):
        if detail is not None:
            messages.log(msg, detail=detail)
        else:
            messages.log(msg)

    def run_test(test_name, test_class):
        nonlocal unittest_verbosity
        if not should_run_test_f(test_name):
            return
        test_teamcity_starttest(options, test_name)
        log('Running %s' % test_name, detail=VERBOSE)
        runner = unittest.TextTestRunner(stream=sys.stdout, verbosity=unittest_verbosity)
        result = runner.run(unittest.makeSuite(test_class))
        log('Tests run: %s' % result.testsRun, detail=VERBOSE)
        log('Errors: %s' % result.errors, detail=VERBOSE)
        log('Failures: %s' % result.failures, detail=VERBOSE)
        status = 'Pass'
        status_message = ''
        if result.errors or result.failures:
            status = 'Fail:'
            status_message = "\n%s" % str(result.errors or result.failures)
            test_teamcity_failtest(options, test_name, status)
        log('%s %s %s%s' % (test_name, messages.ansi_wrap((54 - len(test_name)) * '.', 'grey', options.ansi),
                            status, status_message))
        test_teamcity_endtest(options, test_name)

    test_teamcity_starttestsuite(options, test_constants.SET_PYTHON_UNIT)
    unittest_verbosity = 2 if messages.verbosity >= VERBOSE else 1
    try:
        # Generic base building blocks
        run_test('TestMiscFunctions', TestMiscFunctions)
        run_test('TestOrchestrationLock', TestOrchestrationLock)
        run_test('TestPasswordTools', TestPasswordTools)
        run_test('TestOrchestrationConfig', TestOrchestrationConfig)
        run_test('TestSimpleTimer', TestSimpleTimer)
        run_test('TestAvroEncoder', TestAvroEncoder)
        run_test('TestParquetEncoder', TestParquetEncoder)
        run_test('TestTestValueGenerators', TestTestValueGenerators)

        run_test('TestColumns', TestColumns)
        run_test('TestColumnMetadata', TestColumnMetadata)
        run_test('TestCommandSteps', TestCommandSteps)
        run_test('TestFormatLiteral', TestFormatLiteral)
        run_test('TestOffloadMessages', TestOffloadMessages)
        run_test('TestOrchestrationMetadata', TestOrchestrationMetadata)
        run_test('TestOrchestrationRepoClient', TestOrchestrationRepoClient)
        run_test('TestSyntheticPartitionLiteral', TestSyntheticPartitionLiteral)
        run_test('TestStagingFile', TestStagingFile)
        run_test('TestTeradataPartitionExpression', TestTeradataPartitionExpression)

        # Engine APIs
        run_test('TestBigQueryBackendApi', TestBigQueryBackendApi)
        run_test('TestHs2Connection', TestHs2Connection)
        run_test('TestHiveBackendApi', TestHiveBackendApi)
        run_test('TestImpalaBackendApi', TestImpalaBackendApi)
        run_test('TestSparkThriftBackendApi', TestSparkThriftBackendApi)
        run_test('TestSnowflakeBackendApi', TestSnowflakeBackendApi)
        run_test('TestSynapseBackendApi', TestSynapseBackendApi)
        run_test('TestCurrentBackendApi', TestCurrentBackendApi)
        run_test('TestOracleFrontendApi', TestOracleFrontendApi)
        run_test('TestTeradataFrontendApi', TestTeradataFrontendApi)
        run_test('TestCurrentFrontendApi', TestCurrentFrontendApi)

        run_test('TestCliHdfs', TestCliHdfs)
        run_test('TestGluentAzure', TestGluentAzure)
        run_test('TestGluentGcs', TestGluentGcs)
        run_test('TestGluentS3', TestGluentS3)
        run_test('TestWebHdfs', TestWebHdfs)
        if (orchestration_config.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS
                and orchestration_config.offload_fs_scheme in OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER):
            log('Skipping TestCurrentDfsExecuteMode until GOE-1869 is actioned')
        else:
            run_test('TestCurrentDfsExecuteMode', TestCurrentDfsExecuteMode)

        # Higher level tests
        run_test('TestAvroDataTypeMappings', TestAvroDataTypeMappings)
        run_test('TestParquetDataTypeMappings', TestParquetDataTypeMappings)
        run_test('TestBackendBigQueryDataTypeMappings', TestBackendBigQueryDataTypeMappings)
        run_test('TestBackendHiveDataTypeMappings', TestBackendHiveDataTypeMappings)
        run_test('TestBackendImpalaDataTypeMappings', TestBackendImpalaDataTypeMappings)
        run_test('TestBackendSnowflakeDataTypeMappings', TestBackendSnowflakeDataTypeMappings)
        run_test('TestBackendSynapseDataTypeMappings', TestBackendSynapseDataTypeMappings)
        run_test('TestMSSQLDataTypeMappings', TestMSSQLDataTypeMappings)
        run_test('TestNetezzaDataTypeMappings', TestNetezzaDataTypeMappings)
        run_test('TestOracleDataTypeMappings', TestOracleDataTypeMappings)

        run_test('TestCurrentBackendTable', TestCurrentBackendTable)

        run_test('TestOffloadFunctions', TestOffloadFunctions)
        run_test('TestOffloadTransportFunctions', TestOffloadTransportFunctions)
        run_test('TestOffloadMetadataFunctions', TestOffloadMetadataFunctions)

        run_test('TestIdaPredicateRenderToSQL', test_predicate_offload.TestIdaPredicateRenderToSQL)
        run_test('TestIdaPredicateRenderToDSL', test_predicate_offload.TestIdaPredicateRenderToDSL)
        run_test('TestIdaPredicateParse', test_predicate_offload.TestIdaPredicateParse)
        run_test('TestIdaPredicateDataTypes', test_predicate_offload.TestIdaPredicateDataTypes)
        run_test('TestIdaPredicateMethods', test_predicate_offload.TestIdaPredicateMethods)

        run_test('TestMSSQLOffloadSourceTable', TestMSSQLOffloadSourceTable)
        run_test('TestNetezzaOffloadSourceTable', TestNetezzaOffloadSourceTable)
        run_test('TestOracleOffloadSourceTable', TestOracleOffloadSourceTable)
        run_test('TestCurrentOffloadSourceTable', TestCurrentOffloadSourceTable)

        run_test('TestBigQueryBackendTestingApi', TestBigQueryBackendTestingApi)
        run_test('TestHiveBackendTestingApi', TestHiveBackendTestingApi)
        run_test('TestImpalaBackendTestingApi', TestImpalaBackendTestingApi)
        run_test('TestSnowflakeBackendTestingApi', TestSnowflakeBackendTestingApi)
        run_test('TestSynapseBackendTestingApi', TestSynapseBackendTestingApi)
        run_test('TestCurrentBackendTestingApi', TestCurrentBackendTestingApi)

        run_test('TestMSSQLFrontendTestingApi', TestMSSQLFrontendTestingApi)
        run_test('TestOracleFrontendTestingApi', TestOracleFrontendTestingApi)
        run_test('TestTeradataFrontendTestingApi', TestTeradataFrontendTestingApi)
        run_test('TestCurrentFrontendTestingApi', TestCurrentFrontendTestingApi)

        run_test('TestHybridViewService', TestHybridViewService)
        run_test('TestOffloadTransportRdbmsApi', TestOffloadTransportRdbmsApi)
        run_test('TestOperationDataTypeControls', TestOperationDataTypeControls)
        run_test('TestOperationNotNullColumns', TestOperationNotNullColumns)
    finally:
        test_teamcity_endtestsuite(options, test_constants.SET_PYTHON_UNIT)
