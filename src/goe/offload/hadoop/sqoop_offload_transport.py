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

import re
from typing import Union

from goe.config import orchestration_defaults
from goe.offload.factory.offload_transport_rdbms_api_factory import (
    offload_transport_rdbms_api_factory,
)
from goe.offload.frontend_api import FRONTEND_TRACE_MODULE
from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_ORACLE,
    FILE_STORAGE_FORMAT_AVRO,
)
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_transport import (
    OffloadTransportException,
    OffloadTransport,
    OFFLOAD_TRANSPORT_METHOD_SQOOP,
    OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY,
    TRANSPORT_CXT_BYTES,
    TRANSPORT_CXT_ROWS,
)
from goe.offload.offload_transport_rdbms_api import (
    TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN,
)
from goe.orchestration import command_steps

from goe.util.misc_functions import split_not_in_quotes, write_temp_file


# Sqoop constants
SQOOP_OPTIONS_FILE_PREFIX = "goe-sqoop-options-"
SQOOP_LOG_ROW_COUNT_PATTERN = (
    r"^.*mapreduce\.ImportJobBase: Retrieved (\d+) records\.\r?$"
)
SQOOP_LOG_MODULE_PATTERN = r"^.*dbms_application_info.set_module\(module_name => '(.+)', action_name => '(.+)'\)"


class OffloadTransportStandardSqoop(OffloadTransport):
    """Use standard Sqoop options to transport data"""

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        offload_options,
        messages,
        dfs_client,
        rdbms_columns_override=None,
    ):
        """CONSTRUCTOR"""
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SQOOP

        super(OffloadTransportStandardSqoop, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            rdbms_columns_override=rdbms_columns_override,
        )

        if offload_options.db_type == DBTYPE_MSSQL:
            if len(offload_source_table.get_primary_key_columns()) == 1:
                messages.warning(
                    "Downgrading Sqoop parallelism for %s table without a singleton primary key"
                    % offload_options.db_type
                )
            self._offload_transport_parallelism = 1

        self._sqoop_additional_options = offload_operation.sqoop_additional_options
        self._sqoop_mapreduce_map_memory_mb = (
            offload_operation.sqoop_mapreduce_map_memory_mb
        )
        self._sqoop_mapreduce_map_java_opts = (
            offload_operation.sqoop_mapreduce_map_java_opts
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _sqoop_cli_safe_value(self, cmd_option_string):
        return self._ssh_cli_safe_value(cmd_option_string)

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(return_as_list=True)

    def _remote_copy_sqoop_control_file(self, options_file_local_path, suffix=""):
        return self._remote_copy_transport_control_file(
            options_file_local_path,
            self._offload_transport_cmd_host,
            prefix=SQOOP_OPTIONS_FILE_PREFIX,
            suffix=suffix,
        )

    def _sqoop_by_table_options(self, partition_chunk):
        """Options dictating that Sqoop should process a table/partition list and split data between mappers as it sees fit"""
        sqoop_options = self._rdbms_api.sqoop_rdbms_specific_table_options(
            self._rdbms_owner, self._rdbms_table_name
        )
        jvm_options = self._rdbms_api.sqoop_rdbms_specific_jvm_table_options(
            partition_chunk,
            self._rdbms_partition_type,
            self._rdbms_is_iot,
            self._offload_transport_consistent_read,
        )
        return jvm_options, sqoop_options

    def _sqoop_source_options(self, partition_chunk):
        """Matches returns for same function in SqoopByQuery class"""
        options_file_local_path = None
        jvm_options, sqoop_options = self._sqoop_by_table_options(partition_chunk)
        return jvm_options, sqoop_options, options_file_local_path

    def _sqoop_memory_options(self):
        memory_flags = []
        if self._sqoop_mapreduce_map_memory_mb:
            memory_flags.append(
                "-Dmapreduce.map.memory.mb=%s" % self._sqoop_mapreduce_map_memory_mb
            )
        if self._sqoop_mapreduce_map_java_opts:
            memory_flags.append(
                "-Dmapreduce.map.java.opts=%s" % self._sqoop_mapreduce_map_java_opts
            )
        return memory_flags

    def _get_rows_imported_from_sqoop_log(self, log_string):
        """Search through the Sqoop log file and extract num rows imported"""
        self.debug("_get_rows_imported_from_sqoop_log()")
        if not log_string:
            return None
        assert isinstance(log_string, str)
        m = re.search(
            SQOOP_LOG_ROW_COUNT_PATTERN, log_string, re.IGNORECASE | re.MULTILINE
        )
        if m:
            return int(m.group(1))

    def _get_oracle_module_action_from_sqoop_log(self, log_string):
        """Search through the Sqoop log file and extract Oracle session module/action.
        Only works when OraOop is active, in some cases these will be inaccessible
        Example OraOop command:
            dbms_application_info.set_module(module_name => 'Data Connector for Oracle and Hadoop', action_name => 'import 20190122162424UTC')
        """
        self.debug("_get_oracle_module_action_from_sqoop_log()")
        if not log_string:
            return (None, None)
        assert isinstance(log_string, str)

        m = re.search(
            SQOOP_LOG_MODULE_PATTERN, log_string, re.IGNORECASE | re.MULTILINE
        )
        if m and len(m.groups()) == 2:
            return m.groups()
        else:
            self._messages.log(
                "Unable to identify Sqoop Oracle module/action", detail=VVERBOSE
            )
            return (None, None)

    def _get_sqoop_cli_auth_vars(self):
        extra_jvm_options = []
        no_log_password = []
        app_user_option = ["--username", self._offload_options.rdbms_app_user]
        if self._offload_transport_auth_using_oracle_wallet:
            app_user_option = []
            app_pass_option = []
        elif self._offload_transport_password_alias:
            app_pass_option = [
                "--password-alias",
                self._offload_transport_password_alias,
            ]
            if self._offload_transport_credential_provider_path:
                extra_jvm_options = [self._credential_provider_path_jvm_override()]
        elif self._offload_options.sqoop_password_file:
            app_pass_option = [
                "--password-file",
                self._offload_options.sqoop_password_file,
            ]
        else:
            sqoop_cli_safe_pass = self._sqoop_cli_safe_value(
                self._offload_options.rdbms_app_pass
            )
            no_log_password = [{"item": sqoop_cli_safe_pass, "prior": "--password"}]
            app_pass_option = ["--password", sqoop_cli_safe_pass]
        return app_user_option, app_pass_option, extra_jvm_options, no_log_password

    def _check_for_ora_errors(self, cmd_out):
        # Need to think about checks for non-Oracle rdbms
        ora_errors = [line for line in (cmd_out or "").splitlines() if "ORA-" in line]
        if ora_errors:
            self.log(
                "Sqoop exited cleanly, but the following errors were found in the output:"
            )
            self.log("\t" + "\n\t".join(ora_errors))
            self.log("This is a questionable Sqoop execution")
            raise OffloadTransportException("Errors in Sqoop output")

    def _get_common_jvm_overrides(self):
        std_d_options = []
        if self._offload_transport_jvm_overrides:
            std_d_options += split_not_in_quotes(
                self._offload_transport_jvm_overrides, exclude_empty_tokens=True
            )

        std_d_options += self._sqoop_memory_options()

        if self._offload_transport_queue_name:
            std_d_options += [
                "-Dmapreduce.job.queuename=%s" % self._offload_transport_queue_name
            ]
        return std_d_options

    def _sqoop_import(self, partition_chunk=None):
        self.debug("_sqoop_import()")
        sqoop_rm_commands = None
        self._refresh_rdbms_action()

        if self._nothing_to_do(partition_chunk):
            return 0

        if partition_chunk and partition_chunk.count() > 0:
            if (
                self._offload_options.sqoop_disable_direct
                or self._offload_transport_parallelism < 2
            ):
                self.log(
                    "\nWARNING: Hybrid/partition offload without OraOop direct mode enabled may offload all partitions"
                )

        if not self._offload_transport_consistent_read:
            self.log(
                "\nPerforming Sqoop import without consistent read.", detail=VVERBOSE
            )

        (
            oraoop_d_options,
            offload_source_options,
            options_file_local_path,
        ) = self._sqoop_source_options(partition_chunk)

        (
            app_user_option,
            app_pass_option,
            extra_jvm_options,
            no_log_password,
        ) = self._get_sqoop_cli_auth_vars()
        oraoop_d_options = extra_jvm_options + oraoop_d_options

        if options_file_local_path:
            (
                sqoop_rm_commands,
                options_file_remote_path,
            ) = self._remote_copy_sqoop_control_file(options_file_local_path)
            # Ignore the actual table options and redirect to the options file instead
            offload_source_options = ["--options-file", options_file_remote_path]

        self._run_os_cmd(
            self._ssh_cmd_prefix() + ["mkdir", "-p", self._offload_options.sqoop_outdir]
        )

        sqoop_cmd = ["sqoop", "import"]
        sqoop_cmd += (
            self._get_common_jvm_overrides()
            + self._sqoop_rdbms_specific_jvm_overrides()
            + oraoop_d_options
        )

        connect_string = self._sqoop_cli_safe_value(self._rdbms_api.jdbc_url())
        sqoop_parallel = ["-m" + str(self._offload_transport_parallelism)]
        rdbms_specific_opts = self._rdbms_api.sqoop_rdbms_specific_options()

        sqoop_cmd += (
            ["--connect", connect_string]
            + app_user_option
            + app_pass_option
            + ["--null-string", "''", "--null-non-string", "''"]
            + offload_source_options
            + ["--target-dir=" + self._staging_table_location, "--delete-target-dir"]
            + rdbms_specific_opts
            + sqoop_parallel
            + ["--fetch-size=%d" % int(self._offload_transport_fetch_size)]
            + self._column_type_read_remappings()
            + [
                (
                    "--as-avrodatafile"
                    if self._staging_format == FILE_STORAGE_FORMAT_AVRO
                    else "--as-parquetfile"
                ),
                "--outdir=" + self._offload_options.sqoop_outdir,
            ]
        )

        if self._offload_options.vverbose:
            sqoop_cmd.append("--verbose")

        if self._compress_load_table:
            sqoop_cmd += [
                "--compression-codec",
                "org.apache.hadoop.io.compress.SnappyCodec",
            ]

        sqoop_cmd += ["--class-name", self._get_transport_app_name()]

        if self._sqoop_additional_options:
            sqoop_cmd += split_not_in_quotes(
                self._sqoop_additional_options, exclude_empty_tokens=True
            )

        try:
            rc, cmd_out = self._run_os_cmd(
                self._ssh_cmd_prefix() + sqoop_cmd, no_log_items=no_log_password
            )
            self._check_for_ora_errors(cmd_out)

            rows_imported = None
            if not self._dry_run:
                rows_imported = self._get_rows_imported_from_sqoop_log(cmd_out)
                if self._offload_options.db_type == DBTYPE_ORACLE:
                    module, action = self._get_oracle_module_action_from_sqoop_log(
                        cmd_out
                    )
                    self._rdbms_api.log_sql_stats(
                        module,
                        action,
                        payload=None,
                        validation_polling_interval=self._validation_polling_interval,
                    )
            # In order to let Impala/Hive drop the load table in the future we need g+w
            self.log_dfs_cmd('chmod(%s, "g+w")' % self._staging_table_location)
            if not self._dry_run:
                self._dfs_client.chmod(self._staging_table_location, mode="g+w")
        except:
            # Even in a sqoop failure we still want to chmod the load directory - if it exists
            self.log_dfs_cmd(
                '(exception resilience) chmod(%s, "g+w")'
                % self._staging_table_location,
                detail=VVERBOSE,
            )
            if not self._dry_run:
                try:
                    self._dfs_client.chmod(self._staging_table_location, mode="g+w")
                except Exception as exc:
                    # if we fail while coping with a Sqoop failure then do nothing except log it
                    self.log(
                        "Unable to chmod(%s): %s"
                        % (self._staging_table_location, str(exc)),
                        detail=VVERBOSE,
                    )
            raise

        # if we created a Sqoop options file, let's remove it
        if sqoop_rm_commands:
            [self._run_os_cmd(_) for _ in sqoop_rm_commands]

        self._check_rows_imported(rows_imported)
        return rows_imported

    def _sqoop_rdbms_specific_jvm_overrides(self):
        return self._rdbms_api.sqoop_rdbms_specific_jvm_overrides(
            self._get_rdbms_session_setup_commands(
                include_fixed_sqoop=False, include_semi_colons=True
            )
        )

    def _verify_rdbms_connectivity(self):
        """Use a simple canary query for verification test"""
        rdbms_source_query = self._rdbms_api.get_rdbms_canary_query()

        (
            app_user_option,
            app_pass_option,
            oraoop_d_options,
            no_log_password,
        ) = self._get_sqoop_cli_auth_vars()

        oraoop_d_options = self._sqoop_rdbms_specific_jvm_overrides() + oraoop_d_options
        std_d_options = self._get_common_jvm_overrides()

        sqoop_cmd = ["sqoop", "eval"] + std_d_options + oraoop_d_options

        connect_string = self._sqoop_cli_safe_value(self._rdbms_api.jdbc_url())

        sqoop_cmd += (
            ["--connect", connect_string]
            + app_user_option
            + app_pass_option
            + ["--query", self._sqoop_cli_safe_value(rdbms_source_query)]
        )

        sqoop_cmd += self._rdbms_api.sqoop_rdbms_specific_options()

        if self._sqoop_additional_options:
            sqoop_cmd += split_not_in_quotes(
                self._sqoop_additional_options, exclude_empty_tokens=True
            )

        rc, cmd_out = self._run_os_cmd(
            self._ssh_cmd_prefix() + sqoop_cmd, no_log_items=no_log_password
        )
        self._check_for_ora_errors(cmd_out)
        # if we got this far then we're in good shape
        return True

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(self, partition_chunk=None) -> Union[int, None]:
        """Table centric Sqoop transport"""
        self._reset_transport_context()

        def step_fn():
            row_count = self._sqoop_import(partition_chunk)
            staged_bytes = self._check_and_log_transported_files(row_count)
            self._transport_context[TRANSPORT_CXT_BYTES] = staged_bytes
            self._transport_context[TRANSPORT_CXT_ROWS] = row_count
            self._target_table.post_transport_tasks(self._staging_file)
            return row_count

        return self._messages.offload_step(
            command_steps.STEP_STAGING_TRANSPORT,
            step_fn,
            execute=(not self._dry_run),
        )

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSqoopByQuery(OffloadTransportStandardSqoop):
    """Use Sqoop with a custom query as the row source to transport data
    At the moment this is identical to the parent class, I'm hoping to move some specific
    logic to overloads in this child class
    """

    def __init__(
        self,
        offload_source_table,
        offload_target_table,
        offload_operation,
        offload_options,
        messages,
        dfs_client,
        rdbms_columns_override=None,
    ):
        """CONSTRUCTOR"""
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SQOOP_BY_QUERY
        super(OffloadTransportSqoopByQuery, self).__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            rdbms_columns_override=rdbms_columns_override,
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _sqoop_by_query_options(self, partition_chunk, write_options_to_file=False):
        """Options dictating that Sqoop should process a GOE defined query which splits data into groups ready for
        distributing amongst mappers
        """
        # No double quotes below - they break Sqoop
        sqoop_d_options = ["-Dmapred.child.java.opts=-Duser.timezone=UTC"]
        colexpressions, colnames = self._build_offload_query_lists()
        # Column aliases included for correct AVRO identification
        sql_projection = self._sql_projection_from_offload_query_expression_list(
            colexpressions, colnames, no_newlines=True
        )
        split_by = ["--split-by", TRANSPORT_ROW_SOURCE_QUERY_SPLIT_COLUMN]
        split_row_source_by = self._get_transport_split_type(partition_chunk)

        boundary_query = [
            "--boundary-query",
            self._rdbms_api.sqoop_by_query_boundary_query(
                self._offload_transport_parallelism
            ),
        ]
        row_source = self._get_transport_row_source_query(
            split_row_source_by, partition_chunk
        )
        outer_hint = self._rdbms_api.get_rdbms_session_setup_hint(
            self._offload_transport_rdbms_session_parameters, self._get_max_ts_scale()
        )

        q = "SELECT %s %s FROM (%s) %s WHERE $CONDITIONS" % (
            outer_hint,
            sql_projection,
            row_source,
            self._rdbms_table.enclose_identifier(self._rdbms_table_name),
        )
        self.log("Sqoop custom query:\n%s" % q, detail=VVERBOSE)

        if not write_options_to_file:
            q = self._sqoop_cli_safe_value(q)

        offload_source_options = ["--query", q] + split_by + boundary_query

        options_file_local_path = None
        if write_options_to_file:
            options_file_local_path = write_temp_file(
                "\n".join(offload_source_options), prefix=SQOOP_OPTIONS_FILE_PREFIX
            )
            self.log(
                "Written query options to parameter file: %s" % options_file_local_path,
                detail=VVERBOSE,
            )

        return sqoop_d_options, offload_source_options, options_file_local_path

    def _sqoop_source_options(self, partition_chunk):
        """Matches returns for same function in StandardSqoop class"""
        (
            oraoop_d_options,
            offload_source_options,
            options_file_local_path,
        ) = self._sqoop_by_query_options(partition_chunk, write_options_to_file=True)
        if not options_file_local_path:
            raise OffloadTransportException(
                "Sqoop options file not created as expected"
            )
        return oraoop_d_options, offload_source_options, options_file_local_path


class OffloadTransportSqoopCanary(OffloadTransportStandardSqoop):
    """Validate Sqoop connectivity"""

    def __init__(self, offload_options, messages):
        """CONSTRUCTOR
        This does not call up the stack to parent constructor because we only want a subset of functionality
        """
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SQOOP

        self._create_basic_connectivity_attributes(offload_options)

        self._offload_transport_consistent_read = (
            orchestration_defaults.bool_option_from_string(
                "OFFLOAD_TRANSPORT_CONSISTENT_READ",
                orchestration_defaults.offload_transport_consistent_read_default(),
            )
        )
        self._offload_transport_fetch_size = (
            orchestration_defaults.offload_transport_fetch_size_default()
        )
        self._offload_transport_jvm_overrides = (
            orchestration_defaults.sqoop_overrides_default()
        )
        self._offload_transport_queue_name = (
            orchestration_defaults.sqoop_queue_name_default()
        )
        self._offload_transport_parallelism = 2
        self._validation_polling_interval = (
            orchestration_defaults.offload_transport_validation_polling_interval_default()
        )
        self._spark_config_properties = self._prepare_spark_config_properties(
            orchestration_defaults.offload_transport_spark_properties_default()
        )
        self._sqoop_additional_options = (
            orchestration_defaults.sqoop_additional_options_default()
        )
        self._sqoop_mapreduce_map_memory_mb = None
        self._sqoop_mapreduce_map_java_opts = None
        # No OraOop for canary queries
        self._offload_options.sqoop_disable_direct = True

        self._rdbms_api = offload_transport_rdbms_api_factory(
            "dummy_owner",
            "dummy_table",
            self._offload_options,
            self._messages,
            dry_run=self._dry_run,
        )

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

        # The canary is unaware of any tables
        self._target_table = None
        self._rdbms_table = None
        self._staging_format = None

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()
