#! /usr/bin/env python3

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

""" OrchestrationConfig: Class of configuration attributes for Orchestration commands.
    These are not attributes we expect an end user to change/provide on a command by command basis,
    it is config coming from configuration files.
"""

import logging
from typing import Optional

from goe.config import config_file, orchestration_defaults
from goe.config.config_validation_functions import (
    OrchestrationConfigException,
    check_offload_fs_scheme_supported_in_backend,
    normalise_backend_session_parameters,
    normalise_backend_options,
    normalise_db_prefix_and_paths,
    normalise_filesystem_options,
    normalise_listener_options,
    normalise_offload_transport_config,
    normalise_rdbms_options,
    normalise_rdbms_wallet_options,
    normalise_size_option,
)
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
    HADOOP_BASED_BACKEND_DISTRIBUTIONS,
)
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_transport_functions import hs2_connection_log_message
from goe.util.password_tools import PasswordToolsException


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###########################################################################
# CONSTANTS
###########################################################################

EXPECTED_CONFIG_ARGS = [
    "ansi",
    "backend_distribution",
    "backend_identifier_case",
    "backend_odbc_driver_name",
    "backend_session_parameters",
    "bigquery_dataset_location",
    "bigquery_dataset_project",
    "ca_cert",
    "db_name_pattern",
    "db_name_prefix",
    "db_type",
    "dev_log_level",
    "error_on_token",
    "frontend_odbc_driver_name",
    "google_dataproc_batches_subnet",
    "google_dataproc_batches_ttl",
    "google_dataproc_batches_version",
    "google_dataproc_cluster",
    "google_dataproc_project",
    "google_dataproc_region",
    "google_dataproc_service_account",
    "google_kms_key_ring_location",
    "google_kms_key_ring_name",
    "google_kms_key_ring_project",
    "google_kms_key_name",
    "hadoop_host",
    "hadoop_port",
    "hadoop_ssh_user",
    "webhdfs_verify_ssl",
    "hash_distribution_threshold",
    "hiveserver2_auth_mechanism",
    "hdfs_data",
    "hdfs_home",
    "hdfs_host",
    "hdfs_load",
    "hdfs_db_path_suffix",
    "hive_max_dynamic_partitions",
    "hive_max_dynamic_partitions_pernode",
    "hive_optimize_sort_dynamic_partition",
    "hive_timeout_s",
    "hiveserver2_http_path",
    "hiveserver2_http_transport",
    "ldap_password",
    "ldap_password_file",
    "ldap_user",
    "load_db_name_pattern",
    "listener_host",
    "listener_port",
    "listener_heartbeat_interval",
    "listener_shared_token",
    "listener_redis_host",
    "listener_redis_port",
    "listener_redis_db",
    "listener_redis_username",
    "listener_redis_password",
    "listener_redis_ssl_cert",
    "listener_redis_use_ssl",
    "log_path",
    "log_level",
    "kerberos_principal",
    "kerberos_service",
    "mssql_dsn",
    "mssql_app_user",
    "mssql_app_pass",
    "not_null_propagation",
    "offload_fs_container",
    "offload_fs_prefix",
    "offload_fs_scheme",
    "offload_fs_azure_account_name",
    "offload_fs_azure_account_domain",
    "offload_fs_azure_account_key",
    "offload_staging_format",
    "offload_transport",
    "offload_transport_auth_using_oracle_wallet",
    "offload_transport_cmd_host",
    "offload_transport_credential_provider_path",
    "offload_transport_dsn",
    "offload_transport_livy_api_url",
    "offload_transport_livy_api_verify_ssl",
    "offload_transport_livy_max_sessions",
    "offload_transport_livy_idle_session_timeout",
    "offload_transport_rdbms_session_parameters",
    "offload_transport_password_alias",
    "offload_transport_spark_files",
    "offload_transport_spark_jars",
    "offload_transport_spark_overrides",
    "offload_transport_spark_queue_name",
    "offload_transport_spark_submit_executable",
    "offload_transport_spark_submit_master_url",
    "offload_transport_spark_thrift_host",
    "offload_transport_spark_thrift_port",
    "offload_transport_user",
    "ora_adm_user",
    "ora_adm_pass",
    "ora_app_user",
    "ora_app_pass",
    "ora_repo_user",
    "oracle_dsn",
    "oracle_adm_dsn",
    "password_key_file",
    "quiet",
    "rdbms_app_user",
    "rdbms_app_pass",
    "rdbms_dsn",
    "snowflake_user",
    "snowflake_pass",
    "snowflake_pem_file",
    "snowflake_pem_passphrase",
    "snowflake_account",
    "snowflake_database",
    "snowflake_file_format_prefix",
    "snowflake_integration",
    "snowflake_role",
    "snowflake_stage",
    "snowflake_warehouse",
    "sqoop_disable_direct",
    "sqoop_outdir",
    "sqoop_password_file",
    "sqoop_queue_name",
    "sqoop_overrides",
    "suppress_stdout",
    "synapse_auth_mechanism",
    "synapse_collation",
    "synapse_database",
    "synapse_data_source",
    "synapse_file_format",
    "synapse_msi_client_id",
    "synapse_pass",
    "synapse_port",
    "synapse_role",
    "synapse_server",
    "synapse_service_principal_id",
    "synapse_service_principal_secret",
    "synapse_user",
    "target",
    "teradata_adm_user",
    "teradata_adm_pass",
    "teradata_app_user",
    "teradata_app_pass",
    "teradata_server",
    "teradata_repo_user",
    "udf_db",
    "use_ssl",
    "use_oracle_wallet",
    "webhdfs_host",
    "webhdfs_port",
    "verbose",
    "version",
    "vverbose",
]


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


###########################################################################
# OrchestrationConfig
###########################################################################


class OrchestrationConfig:
    """OrchestrationConfig: Class of configuration attributes for Orchestration commands.
    These are not attributes we expect an end user to change/provide on a command by command basis,
    it is config coming from configuration files.
    """

    ansi: bool
    backend_distribution: Optional[str]
    backend_identifier_case: str
    backend_odbc_driver_name: Optional[str]
    backend_session_parameters: Optional[str]
    bigquery_dataset_location: Optional[str]
    bigquery_dataset_project: Optional[str]
    ca_cert: Optional[str]
    db_name_pattern: str
    db_name_prefix: Optional[str]
    db_type: str
    dev_log_level: str
    error_on_token: Optional[str]
    frontend_odbc_driver_name: Optional[str]
    google_dataproc_batches_subnet: Optional[str]
    google_dataproc_batches_ttl: Optional[str]
    google_dataproc_batches_version: Optional[str]
    google_dataproc_cluster: Optional[str]
    google_dataproc_project: Optional[str]
    google_dataproc_region: Optional[str]
    google_dataproc_service_account: Optional[str]
    google_kms_key_ring_project: Optional[str]
    google_kms_key_ring_location: Optional[str]
    google_kms_key_ring_name: Optional[str]
    google_kms_key_name: Optional[str]
    hadoop_host: Optional[str]
    hadoop_port: Optional[int]
    hadoop_ssh_user: Optional[str]
    webhdfs_verify_ssl: Optional[str]
    hash_distribution_threshold: Optional[str]
    hiveserver2_auth_mechanism: Optional[str]
    hdfs_data: Optional[str]
    hdfs_home: Optional[str]
    hdfs_host: Optional[str]
    hdfs_load: Optional[str]
    hdfs_db_path_suffix: Optional[str]
    listener_host: Optional[str]
    listener_port: Optional[int]
    listener_heartbeat_interval: int
    listener_shared_token: Optional[str]
    listener_redis_db: int
    listener_redis_host: Optional[str]
    listener_redis_port: Optional[int]
    listener_redis_username: Optional[str]
    listener_redis_password: Optional[str]
    listener_redis_ssl_cert: Optional[str]
    listener_redis_use_ssl: Optional[bool]
    log_level: Optional[str]
    log_path: str
    not_null_propagation: Optional[str]
    offload_fs_azure_account_key: Optional[str]
    offload_fs_azure_account_name: Optional[str]
    offload_fs_azure_account_domain: Optional[str]
    offload_fs_container: str
    offload_fs_prefix: Optional[str]
    offload_fs_scheme: str
    offload_staging_format: str
    offload_transport: str
    offload_transport_cmd_host: str
    offload_transport_user: str
    offload_transport_spark_submit_executable: Optional[str]
    offload_transport_spark_thrift_host: Optional[str]
    use_oracle_wallet: bool

    def __init__(self, do_not_connect=False, **kwargs):
        """Do not expect to construct directly via __init__.
        Expected route is via from_dict() or as_defaults().
        """
        unexpected_keys = [k for k in kwargs if k not in EXPECTED_CONFIG_ARGS]
        assert not unexpected_keys, (
            "Unexpected OrchestrationConfig keys: %s" % unexpected_keys
        )
        vars(self).update(kwargs)

        self._do_not_connect = do_not_connect

        normalise_backend_session_parameters(self)
        normalise_filesystem_options(self)
        try:
            normalise_backend_options(self)
            normalise_rdbms_options(self)
            frontend_api = (
                self._get_frontend_connection()
                if self._frontend_api_required()
                else None
            )
            normalise_rdbms_wallet_options(self, frontend_api=frontend_api)
            normalise_listener_options(self)
        except PasswordToolsException as exc:
            raise OrchestrationConfigException(
                "PASSWORD_KEY_FILE enabled, ensure passwords/secrets are encrypted in environment"
            ) from exc
        normalise_offload_transport_config(self)
        normalise_db_prefix_and_paths(self, frontend_api=frontend_api)
        self.not_null_propagation = (
            self.not_null_propagation.upper() if self.not_null_propagation else None
        )
        self.hash_distribution_threshold = normalise_size_option(
            self.hash_distribution_threshold,
            binary_sizes=True,
            strict_name="HASH_DISTRIBUTION_THRESHOLD",
        )

    @staticmethod
    def as_defaults(do_not_connect=False):
        return OrchestrationConfig.from_dict({}, do_not_connect=do_not_connect)

    @staticmethod
    def from_dict(config_dict: dict, do_not_connect=False):
        assert isinstance(config_dict, dict)
        unexpected_keys = [k for k in config_dict if k not in EXPECTED_CONFIG_ARGS]
        assert not unexpected_keys, (
            "Unexpected OrchestrationConfig keys: %s" % unexpected_keys
        )
        # Load environment for defaults.
        config_file.load_env()
        # Build config from config_dict.
        return OrchestrationConfig(
            do_not_connect=do_not_connect,
            ansi=config_dict.get("ansi", orchestration_defaults.ansi_default()),
            backend_distribution=config_dict.get(
                "backend_distribution",
                orchestration_defaults.backend_distribution_default(),
            ),
            backend_identifier_case=config_dict.get(
                "backend_identifier_case",
                orchestration_defaults.backend_identifier_case_default(),
            ),
            backend_odbc_driver_name=config_dict.get(
                "backend_odbc_driver_name",
                orchestration_defaults.backend_odbc_driver_name_default(),
            ),
            backend_session_parameters=config_dict.get(
                "backend_session_parameters",
                orchestration_defaults.backend_session_parameters_default(),
            ),
            bigquery_dataset_location=config_dict.get(
                "bigquery_dataset_location",
                orchestration_defaults.bigquery_dataset_location_default(),
            ),
            bigquery_dataset_project=config_dict.get(
                "bigquery_dataset_project",
                orchestration_defaults.bigquery_dataset_project_default(),
            ),
            ca_cert=config_dict.get(
                "ca_cert", orchestration_defaults.ca_cert_default()
            ),
            db_name_pattern=config_dict.get(
                "db_name_pattern", orchestration_defaults.db_name_pattern_default()
            ),
            db_name_prefix=config_dict.get(
                "db_name_prefix", orchestration_defaults.db_name_prefix_default()
            ),
            db_type=config_dict.get(
                "db_type", orchestration_defaults.frontend_db_type_default()
            ),
            dev_log_level=config_dict.get(
                "dev_log_level", orchestration_defaults.dev_log_level_default()
            ),
            error_on_token=config_dict.get("error_on_token"),
            frontend_odbc_driver_name=config_dict.get(
                "frontend_odbc_driver_name",
                orchestration_defaults.frontend_odbc_driver_name_default(),
            ),
            google_dataproc_batches_subnet=config_dict.get(
                "google_dataproc_batches_subnet",
                orchestration_defaults.google_dataproc_batches_subnet_default(),
            ),
            google_dataproc_batches_ttl=config_dict.get(
                "google_dataproc_batches_ttl",
                orchestration_defaults.google_dataproc_batches_ttl_default(),
            ),
            google_dataproc_batches_version=config_dict.get(
                "google_dataproc_batches_version",
                orchestration_defaults.google_dataproc_batches_version_default(),
            ),
            google_dataproc_cluster=config_dict.get(
                "google_dataproc_cluster",
                orchestration_defaults.google_dataproc_cluster_default(),
            ),
            google_dataproc_project=config_dict.get(
                "google_dataproc_project",
                orchestration_defaults.google_dataproc_project_default(),
            ),
            google_dataproc_region=config_dict.get(
                "google_dataproc_region",
                orchestration_defaults.google_dataproc_region_default(),
            ),
            google_dataproc_service_account=config_dict.get(
                "google_dataproc_service_account",
                orchestration_defaults.google_dataproc_service_account_default(),
            ),
            google_kms_key_ring_location=config_dict.get(
                "google_kms_key_ring_location",
                orchestration_defaults.google_kms_key_ring_location_default(),
            ),
            google_kms_key_ring_name=config_dict.get(
                "google_kms_key_ring_name",
                orchestration_defaults.google_kms_key_ring_name_default(),
            ),
            google_kms_key_ring_project=config_dict.get(
                "google_kms_key_ring_project",
                orchestration_defaults.google_kms_key_ring_project_default(),
            ),
            google_kms_key_name=config_dict.get(
                "google_kms_key_name",
                orchestration_defaults.google_kms_key_name_default(),
            ),
            hadoop_host=config_dict.get(
                "hadoop_host", orchestration_defaults.hadoop_host_default()
            ),
            hadoop_port=config_dict.get(
                "hadoop_port", orchestration_defaults.hadoop_port_default()
            ),
            hadoop_ssh_user=config_dict.get(
                "hadoop_ssh_user", orchestration_defaults.hadoop_ssh_user_default()
            ),
            webhdfs_verify_ssl=config_dict.get(
                "webhdfs_verify_ssl",
                orchestration_defaults.webhdfs_verify_ssl_default(),
            ),
            hash_distribution_threshold=config_dict.get(
                "hash_distribution_threshold",
                orchestration_defaults.hash_distribution_threshold_default(),
            ),
            hiveserver2_auth_mechanism=config_dict.get(
                "hiveserver2_auth_mechanism",
                orchestration_defaults.hiveserver2_auth_mechanism_default(),
            ),
            hdfs_data=config_dict.get(
                "hdfs_data", orchestration_defaults.hdfs_data_default()
            ),
            hdfs_home=config_dict.get(
                "hdfs_home", orchestration_defaults.hdfs_home_default()
            ),
            hdfs_host=config_dict.get(
                "hdfs_host", orchestration_defaults.hdfs_host_default()
            ),
            hdfs_load=config_dict.get(
                "hdfs_load", orchestration_defaults.hdfs_load_default()
            ),
            hdfs_db_path_suffix=config_dict.get(
                "hdfs_db_path_suffix",
                orchestration_defaults.hdfs_db_path_suffix_default(),
            ),
            hive_max_dynamic_partitions=config_dict.get(
                "hive_max_dynamic_partitions",
                orchestration_defaults.hive_max_dynamic_partitions_default(),
            ),
            hive_max_dynamic_partitions_pernode=config_dict.get(
                "hive_max_dynamic_partitions_pernode",
                orchestration_defaults.hive_max_dynamic_partitions_pernode_default(),
            ),
            hive_optimize_sort_dynamic_partition=config_dict.get(
                "hive_optimize_sort_dynamic_partition"
            ),
            hive_timeout_s=config_dict.get(
                "hive_timeout_s", orchestration_defaults.hive_timeout_s_default()
            ),
            kerberos_principal=config_dict.get(
                "kerberos_principal",
                orchestration_defaults.kerberos_principal_default(),
            ),
            kerberos_service=config_dict.get(
                "kerberos_service", orchestration_defaults.kerberos_service_default()
            ),
            load_db_name_pattern=config_dict.get(
                "load_db_name_pattern",
                orchestration_defaults.load_db_name_pattern_default(),
            ),
            offload_staging_format=config_dict.get(
                "offload_staging_format",
                orchestration_defaults.offload_staging_format_default(),
            ),
            listener_host=config_dict.get(
                "listener_host", orchestration_defaults.listener_host_default()
            ),
            listener_port=config_dict.get(
                "listener_port", orchestration_defaults.listener_port_default()
            ),
            listener_heartbeat_interval=config_dict.get(
                "listener_heartbeat_interval",
                orchestration_defaults.listener_heartbeat_interval_default(),
            ),
            listener_shared_token=config_dict.get(
                "listener_shared_token",
                orchestration_defaults.listener_shared_token_default(),
            ),
            listener_redis_db=config_dict.get(
                "listener_redis_db", orchestration_defaults.listener_redis_db_default()
            ),
            listener_redis_host=config_dict.get(
                "listener_redis_host",
                orchestration_defaults.listener_redis_host_default(),
            ),
            listener_redis_port=config_dict.get(
                "listener_redis_port",
                orchestration_defaults.listener_redis_port_default(),
            ),
            listener_redis_username=config_dict.get(
                "listener_redis_username",
                orchestration_defaults.listener_redis_username_default(),
            ),
            listener_redis_password=config_dict.get(
                "listener_redis_password",
                orchestration_defaults.listener_redis_password_default(),
            ),
            listener_redis_ssl_cert=config_dict.get(
                "listener_redis_ssl_cert",
                orchestration_defaults.listener_redis_ssl_cert_default(),
            ),
            listener_redis_use_ssl=config_dict.get(
                "listener_redis_use_ssl",
                orchestration_defaults.listener_redis_use_ssl_default(),
            ),
            log_level=config_dict.get(
                "log_level", orchestration_defaults.log_level_default()
            ),
            log_path=config_dict.get(
                "log_path", orchestration_defaults.log_path_default()
            ),
            mssql_app_user=config_dict.get(
                "mssql_app_user", orchestration_defaults.mssql_app_user_default()
            ),
            mssql_app_pass=config_dict.get(
                "mssql_app_pass", orchestration_defaults.mssql_app_pass_default()
            ),
            mssql_dsn=config_dict.get(
                "mssql_dsn", orchestration_defaults.mssql_dsn_default()
            ),
            not_null_propagation=config_dict.get(
                "not_null_propagation",
                orchestration_defaults.not_null_propagation_default(),
            ),
            offload_fs_azure_account_key=config_dict.get(
                "offload_fs_azure_account_key",
                orchestration_defaults.offload_fs_azure_account_key_default(),
            ),
            offload_fs_azure_account_name=config_dict.get(
                "offload_fs_azure_account_name",
                orchestration_defaults.offload_fs_azure_account_name_default(),
            ),
            offload_fs_azure_account_domain=config_dict.get(
                "offload_fs_azure_account_domain",
                orchestration_defaults.offload_fs_azure_account_domain_default(),
            ),
            offload_fs_container=config_dict.get(
                "offload_fs_container",
                orchestration_defaults.offload_fs_container_default(),
            ),
            offload_fs_prefix=config_dict.get(
                "offload_fs_prefix", orchestration_defaults.offload_fs_prefix_default()
            ),
            offload_fs_scheme=config_dict.get(
                "offload_fs_scheme", orchestration_defaults.offload_fs_scheme_default()
            ),
            offload_transport=config_dict.get(
                "offload_transport", orchestration_defaults.offload_transport_default()
            ),
            use_oracle_wallet=config_dict.get(
                "use_oracle_wallet",
                orchestration_defaults.bool_option_from_string(
                    "USE_ORACLE_WALLET",
                    orchestration_defaults.use_oracle_wallet_default(),
                ),
            ),
            offload_transport_auth_using_oracle_wallet=config_dict.get(
                "offload_transport_auth_using_oracle_wallet",
                orchestration_defaults.bool_option_from_string(
                    "OFFLOAD_TRANSPORT_AUTH_USING_ORACLE_WALLET",
                    orchestration_defaults.offload_transport_auth_using_oracle_wallet_default(),
                ),
            ),
            offload_transport_cmd_host=config_dict.get(
                "offload_transport_cmd_host",
                orchestration_defaults.offload_transport_cmd_host_default(),
            ),
            offload_transport_credential_provider_path=config_dict.get(
                "offload_transport_credential_provider_path",
                orchestration_defaults.offload_transport_credential_provider_path_default(),
            ),
            offload_transport_dsn=config_dict.get(
                "offload_transport_dsn",
                orchestration_defaults.offload_transport_dsn_default(),
            ),
            offload_transport_livy_max_sessions=config_dict.get(
                "offload_transport_livy_max_sessions",
                orchestration_defaults.offload_transport_livy_max_sessions_default(),
            ),
            offload_transport_livy_idle_session_timeout=config_dict.get(
                "offload_transport_livy_idle_session_timeout",
                orchestration_defaults.offload_transport_livy_idle_session_timeout_default(),
            ),
            offload_transport_livy_api_url=config_dict.get(
                "offload_transport_livy_api_url",
                orchestration_defaults.offload_transport_livy_api_url_default(),
            ),
            offload_transport_livy_api_verify_ssl=config_dict.get(
                "offload_transport_livy_api_verify_ssl",
                orchestration_defaults.offload_transport_livy_api_verify_ssl_default(),
            ),
            offload_transport_rdbms_session_parameters=config_dict.get(
                "offload_transport_rdbms_session_parameters",
                orchestration_defaults.offload_transport_rdbms_session_parameters_default(),
            ),
            offload_transport_password_alias=config_dict.get(
                "offload_transport_password_alias",
                orchestration_defaults.offload_transport_password_alias_default(),
            ),
            offload_transport_spark_files=config_dict.get(
                "offload_transport_spark_files",
                orchestration_defaults.offload_transport_spark_files_default(),
            ),
            offload_transport_spark_jars=config_dict.get(
                "offload_transport_spark_jars",
                orchestration_defaults.offload_transport_spark_jars_default(),
            ),
            offload_transport_spark_overrides=config_dict.get(
                "offload_transport_spark_overrides",
                orchestration_defaults.offload_transport_spark_overrides_default(),
            ),
            offload_transport_spark_queue_name=config_dict.get(
                "offload_transport_spark_queue_name",
                orchestration_defaults.offload_transport_spark_queue_name_default(),
            ),
            offload_transport_spark_submit_executable=config_dict.get(
                "offload_transport_spark_submit_executable",
                orchestration_defaults.offload_transport_spark_submit_executable_default(),
            ),
            offload_transport_spark_submit_master_url=config_dict.get(
                "offload_transport_spark_submit_master_url",
                orchestration_defaults.offload_transport_spark_submit_master_url_default(),
            ),
            offload_transport_spark_thrift_host=config_dict.get(
                "offload_transport_spark_thrift_host",
                orchestration_defaults.offload_transport_spark_thrift_host_default(),
            ),
            offload_transport_spark_thrift_port=config_dict.get(
                "offload_transport_spark_thrift_port",
                orchestration_defaults.offload_transport_spark_thrift_port_default(),
            ),
            offload_transport_user=config_dict.get(
                "offload_transport_user",
                orchestration_defaults.offload_transport_user_default(),
            ),
            ora_adm_pass=config_dict.get(
                "ora_adm_pass", orchestration_defaults.ora_adm_pass_default()
            ),
            ora_adm_user=config_dict.get(
                "ora_adm_user", orchestration_defaults.ora_adm_user_default()
            ),
            ora_app_pass=config_dict.get(
                "ora_app_pass", orchestration_defaults.ora_app_pass_default()
            ),
            ora_app_user=config_dict.get(
                "ora_app_user", orchestration_defaults.ora_app_user_default()
            ),
            ora_repo_user=config_dict.get(
                "ora_repo_user", orchestration_defaults.ora_repo_user_default()
            ),
            oracle_adm_dsn=config_dict.get(
                "oracle_adm_dsn", orchestration_defaults.oracle_adm_dsn_default()
            ),
            oracle_dsn=config_dict.get(
                "oracle_dsn", orchestration_defaults.oracle_dsn_default()
            ),
            rdbms_app_pass=config_dict.get("rdbms_app_pass"),
            rdbms_app_user=config_dict.get("rdbms_app_user"),
            rdbms_dsn=config_dict.get("rdbms_dsn"),
            password_key_file=config_dict.get(
                "password_key_file", orchestration_defaults.password_key_file_default()
            ),
            quiet=config_dict.get("quiet", orchestration_defaults.quiet_default()),
            snowflake_account=config_dict.get(
                "snowflake_account", orchestration_defaults.snowflake_account_default()
            ),
            snowflake_database=config_dict.get(
                "snowflake_database",
                orchestration_defaults.snowflake_database_default(),
            ),
            snowflake_user=config_dict.get(
                "snowflake_user", orchestration_defaults.snowflake_user_default()
            ),
            snowflake_pass=config_dict.get(
                "snowflake_pass", orchestration_defaults.snowflake_pass_default()
            ),
            snowflake_pem_file=config_dict.get(
                "snowflake_pem_file",
                orchestration_defaults.snowflake_pem_file_default(),
            ),
            snowflake_pem_passphrase=config_dict.get(
                "snowflake_pem_passphrase",
                orchestration_defaults.snowflake_pem_passphrase_default(),
            ),
            snowflake_file_format_prefix=config_dict.get(
                "snowflake_file_format_prefix",
                orchestration_defaults.snowflake_file_format_prefix_default(),
            ),
            snowflake_integration=config_dict.get(
                "snowflake_integration",
                orchestration_defaults.snowflake_integration_default(),
            ),
            snowflake_role=config_dict.get(
                "snowflake_role", orchestration_defaults.snowflake_role_default()
            ),
            snowflake_stage=config_dict.get(
                "snowflake_stage", orchestration_defaults.snowflake_stage_default()
            ),
            snowflake_warehouse=config_dict.get(
                "snowflake_warehouse",
                orchestration_defaults.snowflake_warehouse_default(),
            ),
            sqoop_disable_direct=config_dict.get(
                "sqoop_disable_direct",
                orchestration_defaults.sqoop_disable_direct_default(),
            ),
            sqoop_outdir=config_dict.get(
                "sqoop_outdir", orchestration_defaults.sqoop_outdir_default()
            ),
            sqoop_password_file=config_dict.get(
                "sqoop_password_file",
                orchestration_defaults.sqoop_password_file_default(),
            ),
            sqoop_queue_name=config_dict.get(
                "sqoop_queue_name", orchestration_defaults.sqoop_queue_name_default()
            ),
            sqoop_overrides=config_dict.get(
                "sqoop_overrides", orchestration_defaults.sqoop_overrides_default()
            ),
            suppress_stdout=config_dict.get(
                "suppress_stdout", orchestration_defaults.suppress_stdout_default()
            ),
            synapse_auth_mechanism=config_dict.get(
                "synapse_auth_mechanism",
                orchestration_defaults.synapse_auth_mechanism_default(),
            ),
            synapse_collation=config_dict.get(
                "synapse_collation", orchestration_defaults.synapse_collation_default()
            ),
            synapse_database=config_dict.get(
                "synapse_database", orchestration_defaults.synapse_database_default()
            ),
            synapse_data_source=config_dict.get(
                "synapse_data_source",
                orchestration_defaults.synapse_data_source_default(),
            ),
            synapse_file_format=config_dict.get(
                "synapse_file_format",
                orchestration_defaults.synapse_file_format_default(),
            ),
            synapse_msi_client_id=config_dict.get(
                "synapse_msi_client_id",
                orchestration_defaults.synapse_msi_client_id_default(),
            ),
            synapse_pass=config_dict.get(
                "synapse_pass", orchestration_defaults.synapse_pass_default()
            ),
            synapse_port=config_dict.get(
                "synapse_port", orchestration_defaults.synapse_port_default()
            ),
            synapse_role=config_dict.get(
                "synapse_role", orchestration_defaults.synapse_role_default()
            ),
            synapse_server=config_dict.get(
                "synapse_server", orchestration_defaults.synapse_server_default()
            ),
            synapse_service_principal_id=config_dict.get(
                "synapse_service_principal_id",
                orchestration_defaults.synapse_service_principal_id_default(),
            ),
            synapse_service_principal_secret=config_dict.get(
                "synapse_service_principal_secret",
                orchestration_defaults.synapse_service_principal_secret_default(),
            ),
            synapse_user=config_dict.get(
                "synapse_user", orchestration_defaults.synapse_user_default()
            ),
            target=config_dict.get(
                "target", orchestration_defaults.query_engine_default()
            ),
            teradata_server=config_dict.get(
                "teradata_server", orchestration_defaults.teradata_server_default()
            ),
            teradata_adm_user=config_dict.get(
                "teradata_adm_user", orchestration_defaults.teradata_adm_user_default()
            ),
            teradata_adm_pass=config_dict.get(
                "teradata_adm_pass", orchestration_defaults.teradata_adm_pass_default()
            ),
            teradata_app_user=config_dict.get(
                "teradata_app_user", orchestration_defaults.teradata_app_user_default()
            ),
            teradata_app_pass=config_dict.get(
                "teradata_app_pass", orchestration_defaults.teradata_app_pass_default()
            ),
            teradata_repo_user=config_dict.get(
                "teradata_repo_user",
                orchestration_defaults.teradata_repo_user_default(),
            ),
            udf_db=config_dict.get("udf_db", orchestration_defaults.udf_db_default()),
            use_ssl=config_dict.get(
                "use_ssl", orchestration_defaults.use_ssl_default()
            ),
            hiveserver2_http_path=config_dict.get(
                "hiveserver2_http_path",
                orchestration_defaults.hiveserver2_http_path_default(),
            ),
            hiveserver2_http_transport=config_dict.get(
                "hiveserver2_http_transport",
                orchestration_defaults.hiveserver2_http_transport_default(),
            ),
            verbose=config_dict.get(
                "verbose", orchestration_defaults.verbose_default()
            ),
            version=config_dict.get(
                "version", orchestration_defaults.version_default()
            ),
            vverbose=config_dict.get(
                "vverbose", orchestration_defaults.vverbose_default()
            ),
            webhdfs_host=config_dict.get(
                "webhdfs_host", orchestration_defaults.webhdfs_host_default()
            ),
            webhdfs_port=config_dict.get(
                "webhdfs_port", orchestration_defaults.webhdfs_port_default()
            ),
            ldap_user=config_dict.get(
                "ldap_user", orchestration_defaults.ldap_user_default()
            ),
            ldap_password=config_dict.get(
                "ldap_password", orchestration_defaults.ldap_password_default()
            ),
            ldap_password_file=config_dict.get(
                "ldap_password_file",
                orchestration_defaults.ldap_password_file_default(),
            ),
        )

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _frontend_api_required(self):
        return bool(
            not self._do_not_connect
            and (
                (self.db_type == DBTYPE_ORACLE and self.use_oracle_wallet)
                or self.db_name_prefix is None
            )
        )

    def _get_frontend_connection(self):
        """We cannot run some validations until after this object has been constructed because it is needed to create
        a frontend connection. This method can be called once we have a fully formed object.
        NJ@2022-01-13 I'm really not keen on this, getting config should be a lightweight operations, but I don't
                      see a way to maintain previous functionality without creating a connection.
        """
        messages = OffloadMessages.from_options(self)
        frontend_api = frontend_api_factory(
            self.db_type,
            self,
            messages,
            dry_run=True,
            trace_action=self.__class__.__name__,
        )
        return frontend_api

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def check_backend_support(self, backend_api):
        """We cannot run some validations until after this object has been constructed because it is needed to create
        a backend connection. This method can be called once we have a connection.
        """
        check_offload_fs_scheme_supported_in_backend(
            self.offload_fs_scheme, backend_api
        )

    def log_connectivity_messages(self, log_fn):
        """Pick up useful connectivity configuration and log them using log_fn"""

        if self.db_type == DBTYPE_ORACLE:
            log_fn(
                "Connecting to Oracle as %s (%s)" % (self.ora_adm_user, self.oracle_dsn)
            )
        elif self.db_type == DBTYPE_MSSQL:
            log_fn(
                "Connecting to Microsoft SQL Server as %s (%s)"
                % (self.mssql_app_user, self.mssql_dsn)
            )
        elif self.db_type == DBTYPE_TERADATA:
            log_fn(
                "Connecting to Teradata as %s (%s)"
                % (self.teradata_adm_user, self.teradata_server)
            )

        if self.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
            log_fn(
                hs2_connection_log_message(
                    self.hadoop_host, self.hadoop_port, self, service_name="HiveServer2"
                )
            )

            if not self.webhdfs_host or not self.webhdfs_port:
                log_fn(
                    "WebHDFS host/port not supplied, using shell commands for HDFS operations (hdfs dfs, scp, etc)"
                )
            else:
                log_fn(
                    "HDFS operations will use WebHDFS (%s:%s) %s"
                    % (
                        self.webhdfs_host,
                        self.webhdfs_port,
                        "using Kerberos" if self.kerberos_service else "unsecured",
                    )
                )

    def override_offload_dfs_config(
        self, offload_fs_scheme, offload_fs_container, offload_fs_prefix
    ):
        # NJ@2022-01-19. I'm not happy having to do this but we needed CLI overrides for these option but enough
        #                code treated these as config to make it hard. So I cheated and allowed an override instead
        #                of moving these to OffloadOperation.
        if offload_fs_scheme:
            self.offload_fs_scheme = offload_fs_scheme.lower()
        if offload_fs_container is not None:
            self.offload_fs_container = offload_fs_container
        if offload_fs_prefix is not None:
            self.offload_fs_prefix = offload_fs_prefix
        normalise_filesystem_options(self)
