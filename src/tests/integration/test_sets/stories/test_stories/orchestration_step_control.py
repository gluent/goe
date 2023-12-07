""" orchestration_step_control: Tests checking that options to skip steps and error before/after steps work as intended.
"""

import os

from test_sets.stories.story_globals import STORY_TYPE_LINK_ID, STORY_TYPE_OFFLOAD, STORY_SETUP_TYPE_ORACLE,\
    STORY_TYPE_SETUP, STORY_TYPE_SHELL_COMMAND
from test_sets.stories.story_setup_functions import drop_backend_test_table, \
    gen_hybrid_drop_ddl, gen_rdbms_dim_create_ddl, \
    STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_PYTHON
from test_sets.stories.story_assertion_functions import messages_step_executions, text_in_log
from test_sets.stories.test_stories.cli_api import get_bin_path, goe_shell_command

from goe.offload.offload_functions import convert_backend_identifier_case
from goe.offload.offload_messages import step_title_to_step_id, FORCED_EXCEPTION_TEXT
from goe.orchestration import command_steps
from goe.orchestration.command_steps import step_title


STEP_DIM = 'story_step_dim'
STEP_FACT = 'story_step_fact'


def orchestration_step_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api):
    bin_path = get_bin_path()
    dim_be = convert_backend_identifier_case(options, STEP_DIM)
    return [
        #
        # Offload tests
        {'id': 'offload_step_offload_dim_setup1',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % STEP_DIM,
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_rdbms_dim_create_ddl(frontend_api, schema, STEP_DIM),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, STEP_DIM),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, STEP_DIM)]}},
        {'id': 'offload_step_offload_dim_skip1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Skipping Step: %s' % step_title(command_steps.STEP_VALIDATE_DATA),
         'options': {'owner_table': schema + '.' + STEP_DIM,
                     'skip': [step_title_to_step_id(step_title(command_steps.STEP_VALIDATE_DATA))],
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'assertion_pairs': [
             (lambda test: messages_step_executions(test.offload_messages,
                                                    step_title(command_steps.STEP_VALIDATE_DATA)),
              lambda test: 0)]},
        {'id': 'offload_step_offload_dim_skip2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Skipping Step: %s' % step_title(command_steps.STEP_VALIDATE_CASTS),
         'options': {'owner_table': schema + '.' + STEP_DIM,
                     'skip': [step_title_to_step_id(step_title(command_steps.STEP_VALIDATE_CASTS))],
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'assertion_pairs': [
             (lambda test: messages_step_executions(test.offload_messages,
                                                    step_title(command_steps.STEP_VALIDATE_CASTS)),
              lambda test: 0)]
         },
        {'id': 'offload_step_offload_dim_skip3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Skipping Step: %s' % step_title(command_steps.STEP_VERIFY_EXPORTED_DATA),
         'options': {'owner_table': schema + '.' + STEP_DIM,
                     'skip': [step_title_to_step_id(step_title(command_steps.STEP_VERIFY_EXPORTED_DATA))],
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'assertion_pairs': [
             (lambda test: messages_step_executions(test.offload_messages,
                                                    step_title(command_steps.STEP_VERIFY_EXPORTED_DATA)),
              lambda test: 0)]
         },
        {'id': 'offload_step_offload_dim_skip4',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Skipping Step: %s' % step_title(command_steps.STEP_DEPENDENT_SCHEMA_OBJECTS),
         'options': {'owner_table': schema + '.' + STEP_DIM,
                     'skip': [step_title_to_step_id(step_title(command_steps.STEP_DEPENDENT_SCHEMA_OBJECTS))],
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'assertion_pairs': [
             (lambda test: messages_step_executions(test.offload_messages,
                                                    step_title(command_steps.STEP_DEPENDENT_SCHEMA_OBJECTS)),
              lambda test: 0)]
         },
        {'id': 'offload_step_offload_dim_skip5',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Skipping Step: %s' % step_title(command_steps.STEP_GENERAL_AAPD),
         'options': {'owner_table': schema + '.' + STEP_DIM,
                     'skip': [step_title_to_step_id(step_title(command_steps.STEP_GENERAL_AAPD))],
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'assertion_pairs': [
             (lambda test: messages_step_executions(test.offload_messages, step_title(command_steps.STEP_GENERAL_AAPD)),
              lambda test: 0)],
         },

        {'id': 'offload_step_offload_dim_skip6',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Skipping Step: %s' % step_title(command_steps.STEP_COUNT_AAPD),
         'options': {'owner_table': schema + '.' + STEP_DIM,
                     'skip': [step_title_to_step_id(step_title(command_steps.STEP_COUNT_AAPD))],
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'assertion_pairs': [
             (lambda test: messages_step_executions(test.offload_messages, step_title(command_steps.STEP_COUNT_AAPD)),
              lambda test: 0)]
         },
        {'id': 'offload_step_offload_dim_skip7_cli',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'Offload Skipping AAPD Steps',
         'narrative': 'If we skip both AAPD creation steps we should not see any rewrite rule creation steps',
         'shell_commands': [
             goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + STEP_DIM,
                                '--reset-backend-table', '-x',
                                '--skip-steps=' + step_title_to_step_id(step_title(command_steps.STEP_GENERAL_AAPD)) +
                                ',' + step_title_to_step_id(step_title(command_steps.STEP_COUNT_AAPD))])],
         'expected_status': False,
         'assertion_pairs': [
             (lambda test: text_in_log(step_title(command_steps.STEP_CREATE_REWRITE_RULES),
                                       '(offload_step_offload_dim_skip7_cli)'),
              lambda test: False)
         ]},
        {'id': 'offload_step_offload_dim_setup2',
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'offload_step_offload_dim_setup1'},
        {'id': 'offload_step_offload_dim_abort1_api',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Aborting Before Step: %s' % step_title(command_steps.STEP_CREATE_TABLE),
         'options': {'owner_table': schema + '.' + STEP_DIM,
                     'error_before_step': step_title(command_steps.STEP_CREATE_TABLE),
                     'reset_backend_table': True},
         'expected_exception_string': FORCED_EXCEPTION_TEXT},
        {'id': 'offload_step_offload_dim_abort1_cli',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'Offload Aborting Before Step: %s' % step_title(command_steps.STEP_CREATE_TABLE),
         'shell_commands': [
             goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + STEP_DIM,
                                '--reset-backend-table', '-x',
                                '--error-before-step=' + step_title_to_step_id(step_title(command_steps.STEP_CREATE_TABLE))])],
         'acceptable_return_codes': [1],
         'assertion_pairs': [
             # Table does not exist
             (lambda test: backend_api.table_exists(data_db, dim_be), lambda test: False)
         ]},
        {'id': 'offload_step_offload_dim_abort2_api',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Aborting Before Step: %s' % step_title(command_steps.STEP_FINAL_LOAD),
         'options': {'owner_table': schema + '.' + STEP_DIM,
                     'error_before_step': step_title(command_steps.STEP_FINAL_LOAD),
                     'reset_backend_table': True},
         'expected_exception_string': FORCED_EXCEPTION_TEXT},
        {'id': 'offload_step_offload_dim_abort2_cli',
         'type': STORY_TYPE_SHELL_COMMAND,
         'title': 'Offload Aborting Before Step: %s' % step_title(command_steps.STEP_FINAL_LOAD),
         'shell_commands': [
             goe_shell_command([os.path.join(bin_path, 'offload'), '-t', schema + '.' + STEP_DIM,
                                '--reset-backend-table', '-x',
                                '--error-before-step=' + step_title_to_step_id(step_title(command_steps.STEP_FINAL_LOAD))])],
         'acceptable_return_codes': [1],
         'assertion_pairs': [
             # Table exists
             (lambda test: backend_api.table_exists(data_db, dim_be), lambda test: True),
             # Table is empty
             (lambda test: backend_api.get_table_row_count(data_db, dim_be), lambda test: 0),
         ]},
        #
        # Present tests
        # TODO Add present tests
        ]
