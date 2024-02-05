# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from typing import TYPE_CHECKING

from goe.goe import get_common_options, log
from goe.schema_sync.schema_sync_analyzer import SchemaSyncAnalyzer
from goe.schema_sync.schema_sync_processor import SchemaSyncProcessor
from goe.schema_sync.schema_sync_command_file import SchemaSyncCommandFile
from goe.orchestration import command_steps
from goe.orchestration.orchestration_lock import (
    OrchestrationLockTimeout,
    orchestration_lock_for_table,
)
from goe.util.misc_functions import double_quote_sandwich

if TYPE_CHECKING:
    from goe.offload.offload_messages import OffloadMessages
    from goe.orchestration.execution_id import ExecutionId
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )

normal, verbose, vverbose = list(range(3))

SCHEMA_SYNC_LOCKED_MESSAGE_TEXT = "Another Schema Sync process has locked"


def get_schema_sync_opts():
    opt = get_common_options(usage="usage: %prog [options]")

    opt.add_option(
        "--include",
        dest="include",
        help="CSV list of schemas, schema.tables or tables to examine for change detection and evolution. Supports wildcards (using *). Example formats: SCHEMA1, SCHEMA*, SCHEMA1.TABLE1,SCHEMA1.TABLE2,SCHEMA2.TAB*, SCHEMA1.TAB*, *.TABLE1,*.TABLE2, *.TAB*",
    )
    opt.add_option(
        "--command-file",
        dest="command_file",
        help="Name of an additional log file to record the commands that have been applied (if the -x/--execute option has been used) or should be applied (if the -x/--execute option has not been used). Supplied as full or relative path",
    )

    remove_options = ["--skip-steps", "--log-path"]

    # Strip away the options we don't need
    [opt.remove_option(option) for option in remove_options]

    return opt


def normalise_schema_sync_options(options):
    if options.vverbose:
        options.verbose = True
    elif options.quiet:
        options.vverbose = options.verbose = False

    if not options.include:
        options.include = "*.*"


def schema_sync(
    options,
    messages: "OffloadMessages",
    execution_id: "ExecutionId",
    repo_client: "OrchestrationRepoClientInterface",
):
    messages.setup_offload_step(
        error_before_step=options.error_before_step,
        error_after_step=options.error_after_step,
        repo_client=repo_client,
    )

    schema_analyzer = SchemaSyncAnalyzer(options, messages)
    schema_processor = SchemaSyncProcessor(options, messages, execution_id, repo_client)
    exceptions = []
    cmd_file = None

    def display_exceptions(exceptions):
        column_header = "=" * 70
        log("%-70s %-70s" % ("Table", "Exception"))
        log("{0:70s} {0:70s}".format(column_header))

        for table, exception in exceptions:
            source_table = "%s.%s" % (
                double_quote_sandwich(table["offloaded_owner"]),
                double_quote_sandwich(table["offloaded_table"]),
            )
            log("{0:70s} {1:70s}".format(source_table, exception))

    tables_in_scope = messages.offload_step(
        command_steps.STEP_NORMALIZE_INCLUDES,
        lambda: schema_analyzer.expand_wildcard_scope(options.include),
        execute=options.execute,
    )

    if options.command_file:
        cmd_file = SchemaSyncCommandFile(options.command_file)

    for table in tables_in_scope:
        table_changes = schema_analyzer.compare_structures(table)
        if "vectors" in table_changes:
            table_owner, table_name = (
                table_changes["offloaded_owner"],
                table_changes["offloaded_table"],
            )
            try:
                with orchestration_lock_for_table(
                    table_owner, table_name, dry_run=bool(not options.execute)
                ):
                    log("Changes detected")
                    step_status, _ = messages.offload_step(
                        command_steps.STEP_PROCESS_TABLE_CHANGES,
                        lambda: schema_processor.process_changes(
                            table_owner, table_name, table_changes, cmd_file
                        ),
                        execute=options.execute,
                    )
                    if step_status:
                        exceptions.append((table, step_status))
            except OrchestrationLockTimeout:
                log("Changes detected but being applied by another Schema Sync process")
                messages.notice(
                    "%s %s.%s, skipping"
                    % (
                        SCHEMA_SYNC_LOCKED_MESSAGE_TEXT,
                        double_quote_sandwich(table_owner),
                        double_quote_sandwich(table_name),
                    )
                )
        elif "compare_exception" in table_changes:
            exceptions.append((table, table_changes["compare_exception"]))
        else:
            log("No changes detected")

    if options.command_file:
        if tables_in_scope and cmd_file.commands_in_file():
            messages.notice('Command file written to "%s"' % options.command_file)
        else:
            cmd_file.remove()
            messages.notice(
                'Command file "%s" not created (no commands to log)'
                % options.command_file
            )

    if messages.get_messages():
        messages.offload_step(
            command_steps.STEP_MESSAGES, messages.log_messages, execute=options.execute
        )

    if exceptions:
        messages.offload_step(
            command_steps.STEP_REPORT_EXCEPTIONS,
            lambda: display_exceptions(exceptions),
            execute=options.execute,
        )
        return 1
    else:
        return 0
