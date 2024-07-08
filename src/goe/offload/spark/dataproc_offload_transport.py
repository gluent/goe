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

import json
import math
import re
from typing import Union, TYPE_CHECKING

from goe.config import orchestration_defaults
from goe.offload.factory.offload_transport_rdbms_api_factory import (
    offload_transport_rdbms_api_factory,
)
from goe.offload.frontend_api import FRONTEND_TRACE_MODULE
from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.offload.offload_transport import (
    OffloadTransportException,
    OffloadTransportSpark,
    MISSING_ROWS_SPARK_WARNING,
    OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD,
    OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD,
    OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE,
    SPARK_OPTIONS_FILE_PREFIX,
    TRANSPORT_CXT_BYTES,
    TRANSPORT_CXT_ROWS,
)
from goe.orchestration import command_steps
from goe.util.misc_functions import write_temp_file

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.offload.backend_table import BackendTableInterface
    from goe.offload.offload_messages import OffloadMessages
    from goe.offload.offload_source_table import OffloadSourceTableInterface


GCLOUD_PROPERTY_SEPARATOR = ",GSEP,"
GCLOUD_BATCHES_STATE_CANCELLED = "CANCELLED"
GCLOUD_BATCHES_STATE_FAILED = "FAILED"


class OffloadTransportSparkBatchesGcloud(OffloadTransportSpark):
    """Submit PySpark to Dataproc via gcloud to transport data."""

    def __init__(
        self,
        offload_source_table: "OffloadSourceTableInterface",
        offload_target_table: "BackendTableInterface",
        offload_operation,
        offload_options: "OrchestrationConfig",
        messages: "OffloadMessages",
        dfs_client,
        rdbms_columns_override=None,
    ):
        """CONSTRUCTOR"""
        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD
        super().__init__(
            offload_source_table,
            offload_target_table,
            offload_operation,
            offload_options,
            messages,
            dfs_client,
            rdbms_columns_override=rdbms_columns_override,
        )
        # For spark-submit we need to pass compression in as a config to the driver program
        self._load_table_compression_pyspark_settings()
        self._offload_fs_container = offload_options.offload_fs_container
        self._dataproc_cluster = offload_options.google_dataproc_cluster
        self._dataproc_project = offload_options.google_dataproc_project
        self._dataproc_region = offload_options.google_dataproc_region
        self._dataproc_service_account = offload_options.google_dataproc_service_account
        self._dataproc_batches_subnet = offload_options.google_dataproc_batches_subnet
        self._dataproc_batches_ttl = offload_options.google_dataproc_batches_ttl
        self._dataproc_batches_version = offload_options.google_dataproc_batches_version

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _spark_cli_safe_value(self, cmd_option_string):
        return self._ssh_cli_safe_value(cmd_option_string)

    def _column_type_read_remappings(self):
        return self._offload_transport_type_remappings(
            return_as_list=False, remap_sep="="
        )

    def _remote_copy_spark_control_file(self, options_file_local_path, suffix=""):
        return self._remote_copy_transport_control_file(
            options_file_local_path,
            self._offload_transport_cmd_host,
            prefix=SPARK_OPTIONS_FILE_PREFIX,
            suffix=suffix,
        )

    def _gcloud_dataproc_describe_command(self, batch_name: str) -> list:
        gcloud_cmd = [
            OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE,
            "dataproc",
            "batches",
            "describe",
            batch_name,
            "--format=json",
        ]
        if self._dataproc_project:
            gcloud_cmd.append(f"--project={self._dataproc_project}")
        if self._dataproc_region:
            gcloud_cmd.append(f"--region={self._dataproc_region}")
        return gcloud_cmd

    def _gcloud_dataproc_submit_command(self, id: str = None) -> list:
        gcloud_cmd = [
            OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE,
            "dataproc",
            "batches",
            "submit",
            "pyspark",
        ]
        if self._dataproc_project:
            gcloud_cmd.append(f"--project={self._dataproc_project}")
        if self._dataproc_region:
            gcloud_cmd.append(f"--region={self._dataproc_region}")
        gcloud_cmd.append(f"--batch={id}")
        if self._dataproc_service_account:
            gcloud_cmd.append(f"--service-account={self._dataproc_service_account}")
        if self._dataproc_batches_version:
            gcloud_cmd.append(f"--version={self._dataproc_batches_version}")
        if self._dataproc_batches_subnet:
            gcloud_cmd.append(f"--subnet={self._dataproc_batches_subnet}")
        if self._dataproc_batches_ttl:
            gcloud_cmd.append(f"--ttl={self._dataproc_batches_ttl}")
        gcloud_cmd.append(f"--deps-bucket={self._offload_fs_container}")
        return gcloud_cmd

    def _get_batch_name(self) -> str:
        """Return a Dataproc Batch name.

        Valid names only accept a simple set of characters and are 4-63 characters in length only.
        """
        batch_name_root = re.sub(
            r"[^a-zA-Z0-9\-]+", "", self._target_db_name + "-" + self._load_table_name
        )
        return self._get_transport_app_name(
            sep="-", ts=True, name_override=batch_name_root
        ).lower()[:64]

    def _get_spark_gcloud_command(self, pyspark_body, id: str = None):
        """Submit PySpark code via gcloud"""

        def cli_safe_the_password(k, v):
            if k == "spark.jdbc.password":
                return self._spark_cli_safe_value(v)
            else:
                return v

        self.log("PySpark: " + pyspark_body, detail=VVERBOSE)
        options_file_local_path = write_temp_file(
            pyspark_body, prefix=SPARK_OPTIONS_FILE_PREFIX, suffix="py"
        )
        py_rm_commands, options_file_remote_path = self._remote_copy_spark_control_file(
            options_file_local_path, suffix="py"
        )
        if self._spark_listener_included_in_config():
            spark_listener_jar_local_path = self._local_goe_listener_jar()
            spark_listener_jar_remote_path = self._remote_copy_transport_file(
                spark_listener_jar_local_path, self._offload_transport_cmd_host
            )
        else:
            spark_listener_jar_remote_path = None
        self.log("Written PySpark file: %s" % options_file_remote_path, detail=VVERBOSE)

        remote_spark_files_csv = self._remote_copy_transport_file_csv(
            self._spark_files_csv, self._offload_transport_cmd_host
        )
        remote_spark_jars_csv = self._remote_copy_transport_file_csv(
            self._spark_jars_csv, self._offload_transport_cmd_host
        )

        if spark_listener_jar_remote_path:
            if remote_spark_jars_csv:
                remote_spark_jars_csv = (
                    f"{spark_listener_jar_remote_path},{remote_spark_jars_csv}"
                )
            else:
                remote_spark_jars_csv = spark_listener_jar_remote_path

        gcloud_cmd = self._gcloud_dataproc_submit_command(id=id)

        spark_config_props, no_log_password = [], []
        [
            spark_config_props.extend(["%s=%s" % (k, cli_safe_the_password(k, v))])
            for k, v in self._spark_config_properties.items()
        ]
        if (
            "spark.jdbc.password" in self._spark_config_properties
            and not self._offload_transport_password_alias
        ):
            # If the rdbms app password is visible in the CLI then obscure it from any logging
            password_config_to_obscure = (
                "spark.jdbc.password=%s"
                % cli_safe_the_password(
                    "spark.jdbc.password",
                    self._spark_config_properties["spark.jdbc.password"],
                )
            )
            no_log_password = [{"item": password_config_to_obscure, "prior": "--conf"}]

        spark_config_props.extend(self._tune_dataproc_for_parallelism())

        if self._offload_transport_jvm_overrides:
            spark_config_props.extend(
                [
                    f"spark.driver.extraJavaOptions={self._offload_transport_jvm_overrides}",
                    f"spark.executor.extraJavaOptions={self._offload_transport_jvm_overrides}",
                ]
            )

        if spark_config_props:
            properties_clause = [
                f"--properties=^{GCLOUD_PROPERTY_SEPARATOR}^"
                + GCLOUD_PROPERTY_SEPARATOR.join(spark_config_props)
            ]
        else:
            properties_clause = []

        jars_opt = [f"--jars={remote_spark_jars_csv}"] if remote_spark_jars_csv else []
        files_opt = (
            [f"--files={remote_spark_files_csv}"] if remote_spark_files_csv else []
        )

        cmd = (
            gcloud_cmd
            + [options_file_remote_path]
            + jars_opt
            + files_opt
            + properties_clause
        )

        return cmd, no_log_password, py_rm_commands

    def _spark_gcloud_import(self, partition_chunk=None):
        self._refresh_rdbms_action()

        if self._nothing_to_do(partition_chunk):
            return 0

        rows_imported = None
        pyspark_body = self._get_pyspark_body(partition_chunk)
        batch_name = self._get_batch_name()
        (
            spark_gcloud_cmd,
            no_log_password,
            py_rm_commands,
        ) = self._get_spark_gcloud_command(pyspark_body, id=batch_name)

        self._start_validation_polling_thread()
        rc, cmd_out = self._run_os_cmd(
            self._ssh_cmd_prefix() + spark_gcloud_cmd, no_log_items=no_log_password
        )
        self._stop_validation_polling_thread()

        if not self._dry_run:
            self._verify_batch(batch_name)
            rows_imported = self._get_rows_imported_from_spark_log(cmd_out)
            rows_imported_from_sql_stats = self._rdbms_api.log_sql_stats(
                self._rdbms_module,
                self._rdbms_action,
                self._drain_validation_polling_thread_queue(),
                validation_polling_interval=self._validation_polling_interval,
            )
            if rows_imported is None:
                if rows_imported_from_sql_stats is None:
                    self.warning(MISSING_ROWS_SPARK_WARNING)
                else:
                    self.warning(
                        f"{MISSING_ROWS_SPARK_WARNING}, falling back on RDBMS SQL statistics"
                    )
                    rows_imported = rows_imported_from_sql_stats

        # Remove any pyspark scripts we created
        if py_rm_commands:
            [self._run_os_cmd(_) for _ in py_rm_commands]

        self._check_rows_imported(rows_imported)
        return rows_imported

    def _tune_dataproc_for_parallelism(self) -> list:
        """Modify Spark Dataproc settings to cater for Offload parallelism.

        As of 2024-05-01 the default value for spark.executor.cores is 4 and value values are 4, 8 and 16 only.
        This is specifying cores per executor instance, spark.executor.instances, which defaults to 2.

        Ensures spark.executor.cores covers the requested parallelism when <= 32.
        Ensures spark.executor.instances covers the requested parallelism when > 32.

        Returns:
            list: A list of extra Spark properties or an empty list.
        """

        def executor_cores() -> int:
            if self._offload_transport_parallelism > 8:
                return 16
            elif self._offload_transport_parallelism > 4:
                return 8
            else:
                return None

        def executor_instances() -> int:
            if self._offload_transport_parallelism <= 32:
                # With cores at 16 then default of two instances is adequate.
                return None
            return math.ceil(self._offload_transport_parallelism / 16)

        if (
            "spark.executor.cores" not in self._spark_config_properties
            and "spark.executor.instances" not in self._spark_config_properties
            and self._offload_transport_parallelism
        ):
            # If the user has not configured spark.executor.cores/instances then
            # increase them from defaults to cater for offload_transport_parallelism.
            # This only applies to Dataproc Batches and therefore is not injected
            # into self._spark_config_properties.
            props = []
            if executor_cores():
                props.append(f"spark.executor.cores={executor_cores()}")
            if executor_instances():
                props.append(f"spark.executor.instances={executor_instances()}")
            return props
        else:
            return []

    def _verify_batch(self, batch_name: str):
        """Check for issues/errors in the batch that should trigger us to stop at this point."""
        describe_cmd = self._gcloud_dataproc_describe_command(batch_name)
        # Command below is optional because we don't want to fail a job if the describe command fails.
        # Only if the describe command successfully tells us the batch failed.
        rc, describe_cmd_output = self._run_os_cmd(
            self._ssh_cmd_prefix() + describe_cmd, optional=True
        )
        if rc == 0 and describe_cmd_output:
            self._verify_batch_describe_response(describe_cmd_output)

    def _verify_batch_describe_response(self, describe_output: str) -> bool:
        """Verify a batch based on the output of the describe command.

        Returns:
            True is we checked the status, False if we were unable to check.
        """
        try:
            reponse_dict = json.loads(describe_output)
        except Exception as exc:
            # If we can't decode the output then log it and fall back to submit output checking.
            self.log(f"Exception describing Dataproc batch: {str(exc)}", detail=VERBOSE)
            return False
        state = reponse_dict.get("state")
        if state and state in [
            GCLOUD_BATCHES_STATE_CANCELLED,
            GCLOUD_BATCHES_STATE_FAILED,
        ]:
            if state == GCLOUD_BATCHES_STATE_CANCELLED:
                raise OffloadTransportException(
                    "Dataproc Batch is incomplete due to TTL, increase GOOGLE_DATAPROC_BATCHES_TTL"
                )
            else:
                raise OffloadTransportException(
                    f"Dataproc batch failed with state: {state}"
                )

    def _verify_rdbms_connectivity(self):
        """Use a simple canary query for verification test"""
        rdbms_source_query = "(%s) v" % self._rdbms_api.get_rdbms_canary_query()
        pyspark_body = self._get_pyspark_body(canary_query=rdbms_source_query)
        self.log("PySpark: " + pyspark_body, detail=VVERBOSE)
        batch_name = self._get_batch_name()
        (
            spark_gcloud_cmd,
            no_log_password,
            py_rm_commands,
        ) = self._get_spark_gcloud_command(pyspark_body, id=batch_name)
        rc, cmd_out = self._run_os_cmd(
            self._ssh_cmd_prefix() + spark_gcloud_cmd, no_log_items=no_log_password
        )
        # Remove any pyspark scripts we created
        if py_rm_commands:
            [self._run_os_cmd(_) for _ in py_rm_commands]
        self._verify_batch(batch_name)
        # If we got this far then we're in good shape
        return True

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def transport(self, partition_chunk=None) -> Union[int, None]:
        """Spark by gcloud batches transport"""
        self._reset_transport_context()

        def step_fn():
            row_count = self._spark_gcloud_import(partition_chunk)
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


class OffloadTransportSparkBatchesGcloudCanary(OffloadTransportSparkBatchesGcloud):
    """Validate Spark Dataproc Serverless connectivity"""

    def __init__(self, offload_options, messages):
        """CONSTRUCTOR
        This does not call up the stack to parent constructor because we only want a subset of functionality.
        """
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_BATCHES_GCLOUD

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
            orchestration_defaults.offload_transport_spark_overrides_default()
        )
        self._offload_transport_queue_name = (
            orchestration_defaults.offload_transport_spark_queue_name_default()
        )
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = (
            orchestration_defaults.offload_transport_validation_polling_interval_default()
        )
        self._spark_config_properties = self._prepare_spark_config_properties(
            orchestration_defaults.offload_transport_spark_properties_default()
        )

        self._rdbms_api = offload_transport_rdbms_api_factory(
            "dummy_owner",
            "dummy_table",
            self._offload_options,
            self._messages,
            dry_run=self._dry_run,
        )

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

        self._offload_fs_container = offload_options.offload_fs_container
        self._dataproc_cluster = offload_options.google_dataproc_cluster
        self._dataproc_project = offload_options.google_dataproc_project
        self._dataproc_region = offload_options.google_dataproc_region
        self._dataproc_service_account = offload_options.google_dataproc_service_account
        self._dataproc_batches_subnet = offload_options.google_dataproc_batches_subnet
        self._dataproc_batches_ttl = offload_options.google_dataproc_batches_ttl
        self._dataproc_batches_version = offload_options.google_dataproc_batches_version
        self._spark_files_csv = offload_options.offload_transport_spark_files
        self._spark_jars_csv = offload_options.offload_transport_spark_jars

        # The canary is unaware of any tables
        self._target_table = None
        self._rdbms_table = None
        self._staging_format = None

    def _get_batch_name(self) -> str:
        """Return a Dataproc Batch name for canary check."""
        # Dataproc batch names only accept a simple set of characters and 4-63 characters in length
        return self._get_transport_app_name(
            sep="-", ts=True, name_override="canary"
        ).lower()[:64]

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()


class OffloadTransportSparkDataprocGcloud(OffloadTransportSparkBatchesGcloud):
    """Submit PySpark to Dataproc via gcloud to transport data."""

    def _gcloud_dataproc_submit_command(self, id: str = None) -> list:
        gcloud_cmd = [
            OFFLOAD_TRANSPORT_SPARK_GCLOUD_EXECUTABLE,
            "dataproc",
            "jobs",
            "submit",
            "pyspark",
        ]
        if not self._dataproc_cluster:
            raise OffloadTransportException(
                "Missing mandatory configuration: GOOGLE_DATAPROC_CLUSTER"
            )
        gcloud_cmd.append(f"--cluster={self._dataproc_cluster}")
        if self._dataproc_project:
            gcloud_cmd.append(f"--project={self._dataproc_project}")
        if self._dataproc_region:
            gcloud_cmd.append(f"--region={self._dataproc_region}")
        if self._dataproc_service_account:
            gcloud_cmd.append(
                f"--impersonate-service-account={self._dataproc_service_account}"
            )
        return gcloud_cmd

    def _tune_dataproc_for_parallelism(self) -> list:
        # No-op when not Dataproc Batches.
        return []

    def _verify_batch(self, batch_name: str):
        # No-op when not Dataproc Batches.
        pass


class OffloadTransportSparkDataprocGcloudCanary(OffloadTransportSparkDataprocGcloud):
    """Validate Dataproc connectivity"""

    def __init__(self, offload_options, messages):
        """CONSTRUCTOR
        This does not call up the stack to parent constructor because we only want a subset of functionality.
        """
        self._offload_options = offload_options
        self._messages = messages
        self._dry_run = False

        self._offload_transport_method = OFFLOAD_TRANSPORT_METHOD_SPARK_DATAPROC_GCLOUD

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
            orchestration_defaults.offload_transport_spark_overrides_default()
        )
        self._offload_transport_queue_name = (
            orchestration_defaults.offload_transport_spark_queue_name_default()
        )
        self._offload_transport_parallelism = 1
        self._validation_polling_interval = (
            orchestration_defaults.offload_transport_validation_polling_interval_default()
        )
        self._spark_config_properties = self._prepare_spark_config_properties(
            orchestration_defaults.offload_transport_spark_properties_default()
        )

        self._rdbms_api = offload_transport_rdbms_api_factory(
            "dummy_owner",
            "dummy_table",
            self._offload_options,
            self._messages,
            dry_run=self._dry_run,
        )

        self._rdbms_module = FRONTEND_TRACE_MODULE
        self._rdbms_action = self._rdbms_api.generate_transport_action()

        self._dataproc_cluster = offload_options.google_dataproc_cluster
        self._dataproc_project = offload_options.google_dataproc_project
        self._dataproc_region = offload_options.google_dataproc_region
        self._dataproc_service_account = offload_options.google_dataproc_service_account
        self._spark_files_csv = offload_options.offload_transport_spark_files
        self._spark_jars_csv = offload_options.offload_transport_spark_jars

        # The canary is unaware of any tables
        self._target_table = None
        self._rdbms_table = None
        self._staging_format = None

    def _get_batch_name(self) -> str:
        """Return a Dataproc Batch name for canary check."""
        # Dataproc batch names only accept a simple set of characters and 4-63 characters in length
        return self._get_transport_app_name(
            sep="-", ts=True, name_override="canary"
        ).lower()[:64]

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def ping_source_rdbms(self):
        return self._verify_rdbms_connectivity()
