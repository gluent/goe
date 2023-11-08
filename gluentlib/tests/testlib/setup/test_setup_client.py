#! /usr/bin/env python3
""" TestSetupClient: Library to manage the setup of test objects.
    LICENSE_TEXT
"""

import logging

from gluentlib.offload.offload_constants import DBTYPE_HIVE, DBTYPE_SYNAPSE
from gluentlib.offload.offload_messages import VERBOSE, VVERBOSE
from gluentlib.orchestration import command_steps
from gluentlib.orchestration.command_steps import step_title
from gluentlib.orchestration.orchestration_runner import OrchestrationRunner
from tests.testlib.setup import setup_constants
from tests.testlib.test_framework import test_constants
from tests.testlib.test_framework.factory.backend_testing_api_factory import backend_testing_api_factory
from tests.testlib.test_framework.factory.frontend_testing_api_factory import frontend_testing_api_factory
from tests.testlib.test_framework.test_functions import get_data_db_for_schema, get_orchestration_options_object
from tests.integration.test_sets.offload_test_functions import get_suitable_offload_granularity_options
#from tests.integration.test_sets.sql_offload.sql_offload_tests import sql_offload_test_table_scripts, sql_offload_tpt_scripts


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################

def create_partition_functions(backend_api, udf_db, messages, table_name_re):
    if backend_api.gluent_partition_functions_supported():
        # Create UDFs for programmatic data test tables, this needs to be done before any offloading
        if not table_name_re or table_name_re.search('udf'):
            # "udf" is not much of a filter but keeps it out of the way when filtering for other tests
            messages.log(f'Creating test UDFs in {udf_db}')
            backend_api.create_test_partition_functions(udf_db)


###########################################################################
# TestSetupClient
###########################################################################

class TestSetupClient:
    def __init__(self, sh_test_user, sh_test_password, messages, config,
                 table_name_re=None, dry_run=False):
        self._sh_test_user = sh_test_user
        self._messages = messages
        self._config = config
        self._table_name_re = table_name_re
        self._dry_run = dry_run
        self._backend_api = backend_testing_api_factory(config.target, config, messages, dry_run=self._dry_run)
        self._frontend_api = frontend_testing_api_factory(config.db_type, config, messages, dry_run=self._dry_run)
        self._sh_test_api = self._frontend_api.create_new_connection(sh_test_user, sh_test_password)

        self._log(f'Running setup for: {sh_test_user}')
        self._log(f'Frontend connection: {config.db_type} {self._frontend_api.frontend_version()}')
        self._log('Backend connection: {} {}'.format(config.target, self._backend_api.backend_version() or ''))
        if self._dry_run:
            self._log('*** Non-execute mode ***')

        self._ascii_only = False
        self._all_chars_notnull = False
        if self._config.target == DBTYPE_SYNAPSE:
            self._ascii_only = True
            self._all_chars_notnull = True

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _debug(self, msg):
        self._messages.debug(msg)

    def _log(self, msg, detail=None, ansi_code=None):
        """ Write to offload log file """
        self._messages.log(msg, detail=detail, ansi_code=ansi_code)
        if detail == VVERBOSE:
            logger.debug(msg)
        else:
            logger.info(msg)

    def _offload_generated_table(self, table_name):
        owner_table = f'{self._sh_test_user}.{table_name}'
        self._messages.log(f'Offloading {owner_table}')
        offload_options = get_orchestration_options_object(log_path=self._config.log_path,
                                                           verbose=self._config.verbose,
                                                           vverbose=self._config.vverbose,
                                                           execute=bool(not self._dry_run))
        offload_options.reset_backend_table = True
        offload_options.owner_table = owner_table
        offload_options.allow_floating_point_conversions = True
        offload_options.allow_nanosecond_timestamp_columns = True
        offload_options.max_offload_chunk_size = '0.5G'
        # TODO Haven't devised a way to control partition offloads yet
        # if len(table_spec) > 5:
        #     # We have a value for a 90/10 offload
        #     if isinstance(table_spec[5], tuple):
        #         format_fn = lambda x: x.strftime('%Y-%m-%d') if isinstance(x, datetime) else str(x)
        #         offload_options.equal_to_values = [format_fn(_) for _ in table_spec[5]]
        #     else:
        #         offload_options.less_than_value = table_spec[5]
        (offload_options.offload_partition_columns, offload_options.offload_partition_granularity,
         offload_options.offload_partition_lower_value, offload_options.offload_partition_upper_value) = \
            get_suitable_offload_granularity_options(self._sh_test_user, table_name, self._config, self._messages)
        offload_options.synthetic_partition_digits = setup_constants.OFFLOAD_CUSTOM_DIGITS.get(table_name, '15')
        if table_name in setup_constants.PART_RANGE_PART_FUNCTIONS:
            udf_data_db = get_data_db_for_schema(self._sh_test_user, self._config)
            offload_options.offload_partition_functions = f'{udf_data_db}.{setup_constants.PART_RANGE_PART_FUNCTIONS[table_name]}'
        if table_name == test_constants.GL_TYPE_MAPPING:
            # This table has a specific test set for offloading but we still need to get it offloaded in setup
            # in case tests don't run in ideal sequence. The actual test set will still re-offload and verify outcome
            for offload_opt, offload_val in self._frontend_api.gl_type_mapping_offload_options(
                    self._backend_api.max_decimal_precision(), self._backend_api.max_decimal_scale(),
                    self._backend_api.max_decimal_integral_magnitude()
            ).items():
                setattr(offload_options, offload_opt, offload_val)
        if offload_options.target == DBTYPE_HIVE:
            # Attempt to avoid ERROR_STATE in Hive, this setup is a once a day task and not part of test cycles
            offload_options.skip = step_title(command_steps.STEP_VALIDATE_DATA) + ',' + step_title(command_steps.STEP_VALIDATE_CASTS)
        OrchestrationRunner(dry_run=True).offload(offload_options, reuse_log=True)

    def _passes_filter(self, table_name):
        return bool(not self._table_name_re or self._table_name_re.search(table_name))

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def create_generated_tables(self):
        #if self._passes_filter(test_constants.GL_CHARS):
        #    self._sh_test_api.create_generated_table(
        #        self._sh_test_user, test_constants.GL_CHARS, ascii_only=self._ascii_only,
        #        all_chars_notnull=self._all_chars_notnull
        #    )
        if self._passes_filter(test_constants.GL_TYPE_MAPPING):
            self._sh_test_api.create_gl_type_mapping(
                self._sh_test_user, ascii_only=self._ascii_only,
                max_backend_precision=self._backend_api.max_decimal_precision(),
                max_backend_scale=self._backend_api.max_decimal_scale(),
                max_decimal_integral_magnitude=self._backend_api.max_decimal_integral_magnitude(),
                all_chars_notnull=self._all_chars_notnull,
                supported_canonical_types=list(self._backend_api.expected_canonical_to_backend_type_map().keys())
            )
        #if self._passes_filter(test_constants.GL_TYPES):
        #    self._sh_test_api.create_generated_table(
        #        self._sh_test_user, test_constants.GL_TYPES, ascii_only=self._ascii_only,
        #        all_chars_notnull=self._all_chars_notnull,
        #        supported_canonical_types=list(self._backend_api.expected_canonical_to_backend_type_map().keys())
        #    )
        #if self._passes_filter(test_constants.GL_TYPES_QI):
        #    self._sh_test_api.create_generated_table(
        #        self._sh_test_user, test_constants.GL_TYPES_QI, ascii_only=self._ascii_only,
        #        all_chars_notnull=self._all_chars_notnull,
        #        supported_canonical_types=list(self._backend_api.expected_canonical_to_backend_type_map().keys())
        #    )
        #if self._passes_filter(test_constants.GL_WIDE):
        #    self._sh_test_api.create_generated_table(
        #        self._sh_test_user, test_constants.GL_WIDE, ascii_only=self._ascii_only,
        #        all_chars_notnull=self._all_chars_notnull,
        #        supported_canonical_types=list(self._backend_api.expected_canonical_to_backend_type_map().keys()),
        #        backend_max_test_column_count=self._backend_api.gl_wide_max_test_column_count()
        #    )

    def create_partition_functions(self):
        udf_data_db = get_data_db_for_schema(self._sh_test_user, self._config)
        create_partition_functions(self._backend_api, udf_data_db, self._messages, self._table_name_re)

    def create_sql_offload_tables(self):
        for table_name, script_path in sql_offload_test_table_scripts(self._config):
            if not self._passes_filter(table_name):
                self._debug(f'Rejecting {table_name}')
                continue
            self._sh_test_api.drop_table(self._sh_test_user, table_name)
            self._sh_test_api.run_sql_in_file(script_path)
        for table_name, script_path in sql_offload_tpt_scripts(self._config):
            if not self._passes_filter(table_name):
                continue
            self._sh_test_api.run_tpt_file(script_path, self._sh_test_user)

    def offload_generated_tables(self):
        #for table_name in (test_constants.GL_CHARS, test_constants.GL_TYPE_MAPPING, test_constants.GL_TYPES,
        #                   test_constants.GL_TYPES_QI, test_constants.GL_WIDE):
        for table_name in (test_constants.GL_TYPE_MAPPING, ):
            if not self._passes_filter(table_name):
                continue
            self._offload_generated_table(table_name)
