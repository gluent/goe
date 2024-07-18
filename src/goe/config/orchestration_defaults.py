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

""" OrchestrationDefaults: Library of functions providing default values for command line options.
    This is a temporary measure in order to get the code out of goe.py and into a single location, in the
    future we expect to refactor all option processing, including defaults, and this module will hopefully become
    redundant at that time.
"""

# Standard Library
import logging
import os
from typing import Optional

from goe.offload.offload_constants import (
    BACKEND_DISTRO_CDH,
    BACKEND_DISTRO_GCP,
    BACKEND_DISTRO_SNOWFLAKE,
    BACKEND_DISTRO_MSAZURE,
    DBTYPE_BIGQUERY,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_MSSQL,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
    FILE_STORAGE_FORMAT_AVRO,
    FILE_STORAGE_FORMAT_PARQUET,
    HADOOP_BASED_BACKEND_DISTRIBUTIONS,
    LIVY_IDLE_SESSION_TIMEOUT,
    LIVY_MAX_SESSIONS,
    NOT_NULL_PROPAGATION_AUTO,
    OFFLOAD_STATS_METHOD_COPY,
    OFFLOAD_STATS_METHOD_NATIVE,
    OFFLOAD_TRANSPORT_AUTO,
    PRESENT_OP_NAME,
    SORT_COLUMNS_NO_CHANGE,
)
from goe.util.misc_functions import is_pos_int

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


class OrchestrationDefaultsException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def bool_option_from_string(opt_name, opt_val):
    if isinstance(opt_val, bool):
        return opt_val
    if opt_val.strip().lower() not in ("true", "false"):
        raise OrchestrationDefaultsException(f"Invalid value for {opt_name}: {opt_val}")
    return bool(opt_val.strip().lower() == "true")


def posint_option_from_string(opt_name, opt_val, allow_zero=False):
    if is_pos_int(opt_val, allow_zero=allow_zero):
        return int(opt_val)
    else:
        raise OrchestrationDefaultsException(
            f"Invalid positive integer value {opt_name}: {opt_val}"
        )


def time_in_seconds_from_string(opt_name, opt_val: str):
    if opt_val.endswith("ms"):
        # Anything that ends in ms we round down to 0 seconds.
        return 0
    return int(opt_val)


###########################################################################
# GOE DEFAULTS
###########################################################################


def allow_decimal_scale_rounding_default() -> bool:
    return False


def allow_floating_point_conversions_default() -> bool:
    return False


def allow_nanosecond_timestamp_columns_default() -> bool:
    return False


def ansi_default() -> bool:
    return True


def backend_distribution_default():
    if os.environ.get("BACKEND_DISTRIBUTION"):
        return os.environ["BACKEND_DISTRIBUTION"].upper()
    elif os.environ.get("QUERY_ENGINE", "").lower() == DBTYPE_IMPALA:
        return BACKEND_DISTRO_CDH
    elif os.environ.get("QUERY_ENGINE", "").lower() == DBTYPE_BIGQUERY:
        return BACKEND_DISTRO_GCP
    else:
        return None


def backend_identifier_case_default() -> str:
    return os.environ.get("BACKEND_IDENTIFIER_CASE", "LOWER").upper() or "LOWER"


def backend_odbc_driver_name_default() -> Optional[str]:
    return os.environ.get("BACKEND_ODBC_DRIVER_NAME")


def backend_session_parameters_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_BACKEND_SESSION_PARAMETERS")


def bigquery_dataset_location_default() -> Optional[str]:
    return os.environ.get("BIGQUERY_DATASET_LOCATION")


def bigquery_dataset_project_default() -> Optional[str]:
    return os.environ.get("BIGQUERY_DATASET_PROJECT")


def ca_cert_default() -> Optional[str]:
    return os.environ.get("SSL_TRUSTED_CERTS")


def create_backend_db_default() -> bool:
    return False


def data_sample_parallelism_default():
    return os.environ.get("DATA_SAMPLE_PARALLELISM", 0)


def data_sample_pct_default() -> str:
    return "AUTO"


def db_name_pattern_default() -> str:
    return "%s"


def db_name_prefix_default() -> Optional[str]:
    return os.environ.get("DB_NAME_PREFIX")


def decimal_padding_digits_default() -> int:
    return 2


def dev_log_level_default() -> str:
    return "WARN"


def execute_default() -> bool:
    return False


def force_default():
    return False


def frontend_db_type_default():
    # Referencing SRCDB_VENDOR for backward compatibility
    frontend_distro = (
        os.environ.get("FRONTEND_DISTRIBUTION") or os.environ.get("SRCDB_VENDOR") or ""
    ).lower()
    if frontend_distro in [
        DBTYPE_MSSQL,
        DBTYPE_ORACLE,
        DBTYPE_TERADATA,
    ]:
        return frontend_distro
    else:
        return DBTYPE_ORACLE


def frontend_odbc_driver_name_default() -> Optional[str]:
    return os.environ.get("FRONTEND_ODBC_DRIVER_NAME")


def get_load_db_pattern():
    return "%s_load"


def google_dataproc_batches_subnet_default() -> Optional[str]:
    return os.environ.get("GOOGLE_DATAPROC_BATCHES_SUBNET")


def google_dataproc_batches_ttl_default() -> Optional[str]:
    return os.environ.get("GOOGLE_DATAPROC_BATCHES_TTL")


def google_dataproc_batches_version_default() -> Optional[str]:
    return os.environ.get("GOOGLE_DATAPROC_BATCHES_VERSION")


def google_dataproc_cluster_default() -> Optional[str]:
    return os.environ.get("GOOGLE_DATAPROC_CLUSTER")


def google_dataproc_project_default() -> Optional[str]:
    return os.environ.get("GOOGLE_DATAPROC_PROJECT")


def google_dataproc_region_default() -> Optional[str]:
    return os.environ.get("GOOGLE_DATAPROC_REGION")


def google_dataproc_service_account_default() -> Optional[str]:
    return os.environ.get("GOOGLE_DATAPROC_SERVICE_ACCOUNT")


def google_kms_key_name_default() -> Optional[str]:
    return os.environ.get("GOOGLE_KMS_KEY_NAME")


def google_kms_key_ring_location_default() -> Optional[str]:
    return os.environ.get("GOOGLE_KMS_KEY_RING_LOCATION")


def google_kms_key_ring_name_default() -> Optional[str]:
    return os.environ.get("GOOGLE_KMS_KEY_RING_NAME")


def google_kms_key_ring_project_default() -> Optional[str]:
    return os.environ.get("GOOGLE_KMS_KEY_RING_PROJECT")


def hdfs_data_default() -> Optional[str]:
    return os.environ.get("HDFS_DATA")


def hdfs_load_default() -> Optional[str]:
    return os.environ.get("HDFS_LOAD")


def hdfs_home_default() -> Optional[str]:
    return os.environ.get("HDFS_HOME")


def load_db_name_pattern_default():
    return get_load_db_pattern()


def log_level_default():
    log_level = os.environ.get("LOG_LEVEL")
    return log_level.lower() if log_level else log_level


def log_path_default():
    log_path = os.environ.get("OFFLOAD_LOGDIR")
    if not log_path and os.environ.get("OFFLOAD_HOME"):
        log_path = os.path.join(os.environ.get("OFFLOAD_HOME"), "log")
    log_path = log_path or "."
    return log_path


def max_offload_chunk_size_default():
    return os.environ.get("MAX_OFFLOAD_CHUNK_SIZE") or "16G"


def max_offload_chunk_count_default():
    return os.environ.get("MAX_OFFLOAD_CHUNK_COUNT") or "100"


def hash_distribution_threshold_default() -> str:
    return os.environ.get("HASH_DISTRIBUTION_THRESHOLD") or "1G"


def num_bytes_fudge_default():
    return 0.25


def offload_distribute_enabled_default():
    return bool(os.environ.get("OFFLOAD_DISTRIBUTE_ENABLED", "false").lower() == "true")


def not_null_propagation_default():
    return (
        os.environ.get("OFFLOAD_NOT_NULL_PROPAGATION") or NOT_NULL_PROPAGATION_AUTO
    ).upper()


def offload_predicate_modify_hybrid_view_default():
    return True


def offload_stats_method_default(operation_name=None):
    if (
        operation_name == PRESENT_OP_NAME
        and os.environ.get("OFFLOAD_STATS_METHOD") == OFFLOAD_STATS_METHOD_COPY
    ):
        # COPY is not valid for present so ignore any COPY setting in the env file which would be in place for offload
        return OFFLOAD_STATS_METHOD_NATIVE
    else:
        return os.environ.get("OFFLOAD_STATS_METHOD") or OFFLOAD_STATS_METHOD_NATIVE


def optional_default():
    return ""


def password_key_file_default():
    return os.environ.get("PASSWORD_KEY_FILE")


def purge_backend_table_default():
    return False


def query_engine_default():
    if os.environ.get("QUERY_ENGINE") is not None:
        return os.environ.get("QUERY_ENGINE").lower()
    else:
        return (
            DBTYPE_HIVE
            if os.environ.get("HIVE_SERVER_PORT") == "10000"
            else DBTYPE_IMPALA
        )


def quiet_default():
    return False


def skip_default():
    return ""


def sort_columns_default():
    return SORT_COLUMNS_NO_CHANGE


def storage_format_default():
    return None


def storage_compression_default():
    return "MED"


def suppress_stdout_default():
    return False


def synthetic_partition_digits_default():
    return 15


def ver_check_default():
    return True


def verbose_default():
    return False


def verify_parallelism_default():
    return os.environ.get("OFFLOAD_VERIFY_PARALLELISM")


def verify_row_count_default():
    return "minus"


def version_default():
    return False


def vverbose_default():
    return False


###########################################################################
# BACKEND HADOOP DEFAULTS
###########################################################################


def hadoop_host_default():
    return os.environ.get("HIVE_SERVER_HOST")


def hadoop_port_default():
    return os.environ.get("HIVE_SERVER_PORT")


def hadoop_ssh_user_default():
    return os.environ.get("HADOOP_SSH_USER")


def hdfs_host_default():
    return os.environ.get("HDFS_CMD_HOST")


def hdfs_db_path_suffix_default():
    if (
        os.environ.get("BACKEND_DISTRIBUTION", BACKEND_DISTRO_CDH).upper()
        not in HADOOP_BASED_BACKEND_DISTRIBUTIONS
    ):
        return ""
    return os.environ.get("HDFS_DB_PATH_SUFFIX", ".db")


def hive_column_stats_default():
    return False


def hive_max_dynamic_partitions_default():
    return 2000


def hive_max_dynamic_partitions_pernode_default():
    return 1000


def hive_timeout_s_default():
    return posint_option_from_string(
        "HIVE_SERVER_TIMEOUT", os.environ.get("HIVE_SERVER_TIMEOUT") or "3600"
    )


def hiveserver2_auth_mechanism_default():
    return os.environ.get("HIVE_SERVER_AUTH_MECHANISM")


def hiveserver2_http_path_default():
    return os.environ.get("HIVE_SERVER_HTTP_PATH")


def hiveserver2_http_transport_default():
    return bool(os.environ.get("HIVE_SERVER_HTTP_TRANSPORT", "false").lower() == "true")


def kerberos_principal_default():
    return os.environ.get("KERBEROS_PRINCIPAL")


def kerberos_service_default():
    return os.environ.get("KERBEROS_SERVICE")


def ldap_user_default():
    return os.environ.get("HIVE_SERVER_USER")


def ldap_password_default():
    return os.environ.get("HIVE_SERVER_PASS")


def ldap_password_file_default():
    return os.environ.get("HIVE_SERVER_LDAP_PASSWORD_FILE")


def udf_db_default():
    return os.environ.get("OFFLOAD_UDF_DB")


def use_ssl_default():
    return bool(os.environ.get("SSL_ACTIVE", "").lower() == "true" or ca_cert_default())


def webhdfs_host_default():
    return os.environ.get("WEBHDFS_HOST")


def webhdfs_port_default():
    return os.environ.get("WEBHDFS_PORT")


def webhdfs_verify_ssl_default():
    return os.environ.get("WEBHDFS_VERIFY_SSL")


###########################################################################
# BACKEND SNOWFLAKE DEFAULTS
###########################################################################


def snowflake_database_default():
    return os.environ.get("SNOWFLAKE_DATABASE")


def snowflake_pass_default():
    return os.environ.get("SNOWFLAKE_PASS")


def snowflake_pem_file_default():
    return os.environ.get("SNOWFLAKE_PEM_FILE")


def snowflake_pem_passphrase_default():
    return os.environ.get("SNOWFLAKE_PEM_PASSPHRASE")


def snowflake_account_default():
    return os.environ.get("SNOWFLAKE_ACCOUNT")


def snowflake_user_default():
    return os.environ.get("SNOWFLAKE_USER")


def snowflake_file_format_prefix_default():
    return os.environ.get("SNOWFLAKE_FILE_FORMAT_PREFIX")


def snowflake_integration_default():
    return os.environ.get("SNOWFLAKE_INTEGRATION")


def snowflake_role_default():
    return os.environ.get("SNOWFLAKE_ROLE")


def snowflake_stage_default():
    return os.environ.get("SNOWFLAKE_STAGE")


def snowflake_warehouse_default():
    return os.environ.get("SNOWFLAKE_WAREHOUSE")


###########################################################################
# BACKEND SYNAPSE DEFAULTS
###########################################################################


def synapse_auth_mechanism_default():
    return os.environ.get("SYNAPSE_AUTH_MECHANISM")


def synapse_collation_default():
    return os.environ.get("SYNAPSE_COLLATION")


def synapse_database_default():
    return os.environ.get("SYNAPSE_DATABASE")


def synapse_data_source_default():
    return os.environ.get("SYNAPSE_DATA_SOURCE")


def synapse_file_format_default():
    return os.environ.get("SYNAPSE_FILE_FORMAT")


def synapse_msi_client_id_default():
    return os.environ.get("SYNAPSE_MSI_CLIENT_ID")


def synapse_pass_default():
    return os.environ.get("SYNAPSE_PASS")


def synapse_port_default():
    return os.environ.get("SYNAPSE_PORT", "1433")


def synapse_role_default():
    return os.environ.get("SYNAPSE_ROLE")


def synapse_server_default():
    return os.environ.get("SYNAPSE_SERVER")


def synapse_service_principal_id_default():
    return os.environ.get("SYNAPSE_SERVICE_PRINCIPAL_ID")


def synapse_service_principal_secret_default():
    return os.environ.get("SYNAPSE_SERVICE_PRINCIPAL_SECRET")


def synapse_user_default():
    return os.environ.get("SYNAPSE_USER")


###########################################################################
# FRONTEND ORACLE DEFAULTS
###########################################################################


def ora_adm_pass_default():
    return os.environ.get("ORA_ADM_PASS")


def ora_adm_user_default():
    return os.environ.get("ORA_ADM_USER")


def ora_app_user_default():
    return os.environ.get("ORA_APP_USER")


def ora_app_pass_default():
    return os.environ.get("ORA_APP_PASS")


def ora_repo_user_default():
    return os.environ.get("ORA_REPO_USER")


def oracle_dsn_default():
    return os.environ.get("ORA_CONN")


###########################################################################
# FRONTEND MSSQL DEFAULTS
###########################################################################


def mssql_app_user_default():
    return os.environ.get("MSSQL_APP_USER")


def mssql_app_pass_default():
    return os.environ.get("MSSQL_APP_PASS")


def mssql_dsn_default():
    return os.environ.get("MSSQL_CONN")


###########################################################################
# FRONTEND TERADATA DEFAULTS
###########################################################################


def teradata_adm_pass_default():
    return os.environ.get("TERADATA_ADM_PASS")


def teradata_adm_user_default():
    return os.environ.get("TERADATA_ADM_USER")


def teradata_app_pass_default():
    return os.environ.get("TERADATA_APP_PASS")


def teradata_app_user_default():
    return os.environ.get("TERADATA_APP_USER")


def teradata_server_default():
    return os.environ.get("TERADATA_SERVER")


def teradata_repo_user_default():
    return os.environ.get("TERADATA_REPO_USER")


###########################################################################
# OFFLOAD DFS DEFAULTS
###########################################################################


def offload_fs_scheme_default() -> str:
    return os.environ.get("OFFLOAD_FS_SCHEME", "inherit")


def offload_fs_prefix_default() -> str:
    return os.environ.get("OFFLOAD_FS_PREFIX", "").lower()


def offload_fs_container_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_FS_CONTAINER")


def offload_fs_azure_account_domain_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_FS_AZURE_ACCOUNT_DOMAIN")


def offload_fs_azure_account_name_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_FS_AZURE_ACCOUNT_NAME")


def offload_fs_azure_account_key_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_FS_AZURE_ACCOUNT_KEY")


###########################################################################
# OFFLOAD TRANSPORT DEFAULTS
###########################################################################


def compress_load_table_default() -> bool:
    return bool(
        os.environ.get("OFFLOAD_COMPRESS_LOAD_TABLE", "false").lower() == "true"
    )


def compute_load_table_stats_default() -> bool:
    return False


def offload_staging_format_default() -> str:
    if os.environ.get("OFFLOAD_STAGING_FORMAT"):
        return os.environ["OFFLOAD_STAGING_FORMAT"].upper()
    elif os.environ.get("BACKEND_DISTRIBUTION", BACKEND_DISTRO_CDH).upper() in [
        BACKEND_DISTRO_SNOWFLAKE,
        BACKEND_DISTRO_MSAZURE,
    ]:
        return FILE_STORAGE_FORMAT_PARQUET
    else:
        return FILE_STORAGE_FORMAT_AVRO


def offload_transport_default() -> str:
    return (os.environ.get("OFFLOAD_TRANSPORT") or OFFLOAD_TRANSPORT_AUTO).upper()


def use_oracle_wallet_default() -> bool:
    str_val = os.environ.get("USE_ORACLE_WALLET") or "false"
    return bool_option_from_string("USE_ORACLE_WALLET", str_val)


def offload_transport_auth_using_oracle_wallet_default() -> bool:
    str_val = os.environ.get("OFFLOAD_TRANSPORT_AUTH_USING_ORACLE_WALLET") or "false"
    return bool_option_from_string(
        "OFFLOAD_TRANSPORT_AUTH_USING_ORACLE_WALLET", str_val
    )


def offload_transport_cmd_host_default():
    # for backward compatibility we still use SQOOP_CMD_HOST as a fallback
    return os.environ.get("OFFLOAD_TRANSPORT_CMD_HOST") or os.environ.get(
        "SQOOP_CMD_HOST"
    )


def offload_transport_consistent_read_default():
    str_val = os.environ.get("OFFLOAD_TRANSPORT_CONSISTENT_READ", "true")
    return str_val.lower() if str_val else str_val


def offload_transport_fetch_size_default() -> str:
    str_val = os.environ.get("OFFLOAD_TRANSPORT_FETCH_SIZE") or "5000"
    return str_val


def offload_transport_livy_max_sessions_default():
    str_val = os.environ.get("OFFLOAD_TRANSPORT_LIVY_MAX_SESSIONS") or str(
        LIVY_MAX_SESSIONS
    )
    return str_val


def offload_transport_livy_idle_session_timeout_default() -> str:
    str_val = os.environ.get("OFFLOAD_TRANSPORT_LIVY_IDLE_SESSION_TIMEOUT") or str(
        LIVY_IDLE_SESSION_TIMEOUT
    )
    return str_val


def offload_transport_livy_api_url_default():
    return os.environ.get("OFFLOAD_TRANSPORT_LIVY_API_URL")


def offload_transport_livy_api_verify_ssl_default():
    """None and '' are valid valuess for this option therefore we cannot use bool_option_from_string() here"""
    return os.environ.get("OFFLOAD_TRANSPORT_LIVY_API_VERIFY_SSL")


def offload_transport_parallelism_default() -> str:
    # still including SQOOP_PARALLELISM as a fallback for backwards compatibility
    return (
        os.environ.get("OFFLOAD_TRANSPORT_PARALLELISM")
        or os.environ.get("SQOOP_PARALLELISM")
        or "2"
    )


def offload_transport_password_alias_default() -> Optional[str]:
    # still including SQOOP_PASSWORD_ALIAS as a fallback for backwards compatibility
    return os.environ.get("OFFLOAD_TRANSPORT_PASSWORD_ALIAS") or os.environ.get(
        "SQOOP_PASSWORD_ALIAS"
    )


def offload_transport_credential_provider_path_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_CREDENTIAL_PROVIDER_PATH")


def offload_transport_rdbms_session_parameters_default() -> str:
    return os.environ.get("OFFLOAD_TRANSPORT_RDBMS_SESSION_PARAMETERS") or "{}"


def offload_transport_small_table_threshold_default() -> str:
    # 20M default is based on approximate crossover between startup overhead of Sqoop vs QueryImport elapsed time
    return os.environ.get("OFFLOAD_TRANSPORT_SMALL_TABLE_THRESHOLD") or "20M"


def offload_transport_spark_files_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_SPARK_FILES")


def offload_transport_spark_jars_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_SPARK_JARS")


def offload_transport_spark_overrides_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_SPARK_OVERRIDES")


def offload_transport_spark_properties_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_SPARK_PROPERTIES")


def offload_transport_spark_queue_name_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_SPARK_QUEUE_NAME")


def offload_transport_spark_submit_executable_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE")


def offload_transport_spark_submit_master_url_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_SPARK_SUBMIT_MASTER_URL")


def offload_transport_spark_thrift_host_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_SPARK_THRIFT_HOST")


def offload_transport_spark_thrift_port_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_SPARK_THRIFT_PORT")


def offload_transport_dsn_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_TRANSPORT_DSN")


def oracle_adm_dsn_default() -> Optional[str]:
    return os.environ.get("ORA_ADM_CONN")


def offload_transport_user_default() -> Optional[str]:
    # Includes HADOOP_SSH_USER for backwards compatibility
    return os.environ.get("OFFLOAD_TRANSPORT_USER") or os.environ.get("HADOOP_SSH_USER")


def offload_transport_validation_polling_interval_default() -> str:
    return os.environ.get("OFFLOAD_TRANSPORT_VALIDATION_POLLING_INTERVAL") or "0"


def preserve_load_table_default() -> bool:
    return False


def sqoop_additional_options_default() -> Optional[str]:
    return os.environ.get("SQOOP_ADDITIONAL_OPTIONS")


def sqoop_disable_direct_default() -> bool:
    return bool(
        os.environ.get("SQOOP_DISABLE_DIRECT", "FALSE").upper() in ["YES", "TRUE"]
    )


def sqoop_outdir_default() -> Optional[str]:
    return os.environ.get("SQOOP_OUTDIR") or ".goesqoop"


def sqoop_overrides_default() -> Optional[str]:
    return os.environ.get("SQOOP_OVERRIDES")


def sqoop_password_file_default() -> Optional[str]:
    return os.environ.get("SQOOP_PASSWORD_FILE")


def sqoop_queue_name_default() -> Optional[str]:
    return os.environ.get("SQOOP_QUEUE_NAME")


###########################################################################
# GOE LISTENER DEFAULTS
###########################################################################


def cache_enabled() -> bool:
    return True if os.environ.get("OFFLOAD_LISTENER_REDIS_HOST") else False


def listener_host_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_LISTENER_HOST")


def listener_port_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_LISTENER_PORT")


def listener_heartbeat_interval_default() -> int:
    return int(os.environ.get("OFFLOAD_LISTENER_HEARTBEAT_INTERVAL") or 30)


def listener_shared_token_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_LISTENER_SHARED_TOKEN")


def listener_redis_username_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_LISTENER_REDIS_USERNAME")


def listener_redis_password_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_LISTENER_REDIS_PASSWORD")


def listener_redis_host_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_LISTENER_REDIS_HOST")


def listener_redis_port_default() -> int:
    return int(os.environ.get("OFFLOAD_LISTENER_REDIS_PORT") or 6379)


def listener_redis_db_default() -> int:
    return int(os.environ.get("OFFLOAD_LISTENER_REDIS_DB") or 0)


def listener_redis_ssl_cert_default() -> Optional[str]:
    return os.environ.get("OFFLOAD_LISTENER_REDIS_SSL_CERT")


def listener_redis_use_ssl_default() -> bool:
    return bool(os.environ.get("OFFLOAD_LISTENER_REDIS_SSL", "false").lower() == "true")


def listener_redis_use_sentinel_default() -> int:
    return bool(
        os.environ.get("OFFLOAD_LISTENER_REDIS_USE_SENTINEL", "false").lower() == "true"
    )
