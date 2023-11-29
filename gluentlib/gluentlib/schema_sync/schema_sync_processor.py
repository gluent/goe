#! /usr/bin/env python3
""" SchemaSyncProcessor: Library for processing Schema Sync operations
    LICENSE_TEXT
"""

from typing import TYPE_CHECKING

from gluent import log
from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.schema_sync.schema_sync_constants import ADD_BACKEND_COLUMN,\
    PRESENT_TABLE, ADD_ORACLE_COLUMN
from gluentlib.util.misc_functions import double_quote_sandwich
from gluentlib.schema_sync.steps import step_builder

if TYPE_CHECKING:
    from gluentlib.offload.offload_messages import OffloadMessages
    from gluentlib.orchestration.execution_id import ExecutionId
    from gluentlib.persistence.orchestration_repo_client import OrchestrationRepoClientInterface


SUPPORTED_CHANGE_TYPES = [ADD_BACKEND_COLUMN, ADD_ORACLE_COLUMN]


class SchemaSyncProcessorException(Exception): pass


class SchemaSyncProcessor(object):
    """ Class for processing change steps (vectors) created by SchemaSyncAnalyzer
    """

    def __init__(self, options, messages: "OffloadMessages", execution_id: "ExecutionId",
                repo_client: "OrchestrationRepoClientInterface"):

        self._messages = messages
        self._options = options
        self._execution_id = execution_id
        self._repo_client = repo_client
        self._orchestration_options = OrchestrationConfig.from_dict(
            {'execute': options.execute,
             'verbose': options.verbose,
             'vverbose': options.vverbose}
        )

    def process_changes(self, table_owner, table_name, change, cmd_file):

        log('Process changes: %s.%s' % (double_quote_sandwich(table_owner), double_quote_sandwich(table_name)))

        table_exception = None
        table_commands = []

        if cmd_file:
            source_table = '%s.%s' % (double_quote_sandwich(change['offloaded_owner']),
                                      double_quote_sandwich(change['offloaded_table']))
            cmd_file.write_table_header(source_table)

        for vector in change['vectors']:

            if vector['type'] not in SUPPORTED_CHANGE_TYPES:
                raise NotImplementedError('Change operation not supported.')

            step = step_builder.build_schema_sync_step(vector['type'],
                                                       options=self._options,
                                                       orchestration_options=self._orchestration_options,
                                                       messages=self._messages,
                                                       execution_id=self._execution_id,
                                                       repo_client=self._repo_client)

            table_exception, table_command = step.run(vector)
            log('')

            if table_command and cmd_file:
                for command in table_command:
                    cmd_file.write_command(command)
                cmd_file.write('')

            if table_exception:
                # Step returned an exception so do not run any more steps
                break

        return table_exception, table_commands