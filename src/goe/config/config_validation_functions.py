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

""" config_validation_functions: Library of functions used in goe.py and OrchestrationConfig
"""

import json
import random
import re

from goe.config import orchestration_defaults
from goe.filesystem.goe_dfs import (
    AZURE_OFFLOAD_FS_SCHEMES,
    OFFLOAD_FS_SCHEME_MAPRFS,
    VALID_OFFLOAD_FS_SCHEMES,
    OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER,
)
from goe.offload.microsoft.synapse_constants import (
    SYNAPSE_AUTH_MECHANISM_AD_SERVICE_PRINCIPAL,
    SYNAPSE_VALID_AUTH_MECHANISMS,
    SYNAPSE_USER_PASS_AUTH_MECHANISMS,
)
from goe.offload.offload_constants import (
    BACKEND_DISTRO_GCP,
    BACKEND_DISTRO_MAPR,
    BACKEND_DISTRO_SNOWFLAKE,
    BACKEND_DISTRO_MSAZURE,
    DBTYPE_HIVE,
    DBTYPE_IMPALA,
    DBTYPE_MSSQL,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
    HADOOP_BASED_BACKEND_DISTRIBUTIONS,
)
from goe.offload.offload_transport import (
    derive_rest_api_verify_value_from_url,
    SPARK_SUBMIT_SPARK_YARN_MASTER,
    VALID_OFFLOAD_TRANSPORTS,
    VALID_SPARK_SUBMIT_EXECUTABLES,
)
from goe.util.goe_log_fh import is_valid_path_for_logs
from goe.util.misc_functions import human_size_to_bytes
from goe.util.password_tools import PasswordTools


class OrchestrationConfigException(Exception):
    pass


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def verify_json_option(option_name, option_value):
    if option_value:
        try:
            properties = json.loads(option_value)

            invalid_props = [
                k for k, v in properties.items() if type(v) not in (str, int, float)
            ]
            if invalid_props:
                prop_details = "\n".join(
                    f"Invalid property value for key/value pair: {k}: {properties[k]}"
                    for k in invalid_props
                )
                raise OrchestrationConfigException(
                    "Invalid property value in {} for keys: {}\n{}".format(
                        option_name, str(invalid_props), prop_details
                    )
                )
        except ValueError as ve:
            raise OrchestrationConfigException(
                "Invalid JSON value for %s: %s" % (option_name, str(ve))
            ) from ve


###########################################################################
# NORMALISATION FUNCTIONS
# For the time being these are a lift and shift from goe.py while we
# migrate to OrchestrationConfig.
###########################################################################


def normalise_backend_options(options):
    normalise_bigquery_options(options)
    normalise_hadoop_options(options)
    normalise_snowflake_options(options)
    normalise_synapse_options(options)

    if options.backend_identifier_case:
        options.backend_identifier_case = options.backend_identifier_case.upper()
    else:
        options.backend_identifier_case = (
            orchestration_defaults.backend_identifier_case_default()
        )
    if options.backend_identifier_case not in ["UPPER", "LOWER", "NO_MODIFY"]:
        raise OrchestrationConfigException(
            "Invalid value for BACKEND_IDENTIFIER_CASE, valid values: UPPER|LOWER|NO_MODIFY"
        )


def normalise_db_prefix_and_paths(options, frontend_api=None):
    if options.db_name_prefix is None:
        assert frontend_api
        options.db_name_prefix = frontend_api.get_db_unique_name()

    if options.db_name_prefix and options.db_name_prefix != "":
        options.db_name_pattern = (
            options.db_name_prefix.lower() + "_" + options.db_name_pattern
        )
        options.load_db_name_pattern = (
            options.db_name_prefix.lower() + "_" + options.load_db_name_pattern
        )

    if not is_valid_path_for_logs(options.log_path):
        raise OrchestrationConfigException(
            f"Invalid value for OFFLOAD_LOGDIR: {options.log_path}"
        )


def normalise_bigquery_options(options, exc_cls=OrchestrationConfigException):
    if options.backend_distribution != BACKEND_DISTRO_GCP:
        return
    if options.google_dataproc_batches_version:
        if not re.match(
            r"^[1-9]\.[0-9](\.[0-9])?$", options.google_dataproc_batches_version
        ):
            raise exc_cls(
                f"Invalid value for GOOGLE_DATAPROC_BATCHES_VERSION: {options.google_dataproc_batches_version}"
            )
    if options.google_dataproc_batches_ttl:
        if not re.match(r"^[1-9][0-9]*[mhd]$", options.google_dataproc_batches_ttl):
            raise exc_cls(
                f"Invalid value for GOOGLE_DATAPROC_BATCHES_TTL: {options.google_dataproc_batches_ttl}"
            )


def normalise_hadoop_options(options, exc_cls=OrchestrationConfigException):
    if options.backend_distribution not in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
        return

    if not options.hadoop_host:
        raise exc_cls("HIVE_SERVER_HOST is mandatory")

    if options.target == DBTYPE_HIVE:
        if not options.hiveserver2_auth_mechanism:
            options.hiveserver2_auth_mechanism = "PLAIN"
    elif options.target == DBTYPE_IMPALA:
        if not options.hiveserver2_auth_mechanism:
            options.hiveserver2_auth_mechanism = "NOSASL"

    # Select hadoop_host from (potentially) comma-separated list
    options.hadoop_host = random.choice(options.hadoop_host.split(","))
    options.hdfs_host = options.hdfs_host or options.hadoop_host

    normalise_webhdfs(options)


def normalise_backend_session_parameters(opts):
    if not opts.backend_session_parameters:
        opts.backend_session_parameters = {}
    elif isinstance(opts.backend_session_parameters, str):
        verify_json_option(
            "OFFLOAD_BACKEND_SESSION_PARAMETERS", opts.backend_session_parameters
        )
        opts.backend_session_parameters = json.loads(opts.backend_session_parameters)


def normalise_filesystem_options(options, exc_cls=OrchestrationConfigException):
    options.offload_fs_scheme = (
        options.offload_fs_scheme.lower()
        if options.offload_fs_scheme
        else options.offload_fs_scheme
    )
    if (
        options.offload_fs_scheme
        and options.offload_fs_scheme not in VALID_OFFLOAD_FS_SCHEMES
    ):
        raise exc_cls(
            "OFFLOAD_FS_SCHEME/--offload-fs-scheme must be one of: %s"
            % VALID_OFFLOAD_FS_SCHEMES
        )
    if (
        options.offload_fs_scheme in OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER
        and not options.offload_fs_container
    ):
        raise exc_cls(
            "OFFLOAD_FS_CONTAINER/--offload-fs-container required for scheme: %s"
            % options.offload_fs_scheme
        )
    if (
        options.offload_fs_scheme == OFFLOAD_FS_SCHEME_MAPRFS
        and options.backend_distribution != BACKEND_DISTRO_MAPR
    ):
        raise exc_cls(
            "OFFLOAD_FS_SCHEME/--offload-fs-scheme %s not valid for this backend system"
            % options.offload_fs_scheme
        )

    if options.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
        if not options.hdfs_data:
            raise exc_cls("HDFS_DATA environment variable is mandatory")
        if not options.hdfs_load:
            raise exc_cls("HDFS_LOAD environment variable is mandatory")

    # Not ideal to hardcode SNOWFLAKE here but we don't have a backend_api in normalise* to get the supported schemes
    if options.backend_distribution == BACKEND_DISTRO_SNOWFLAKE:
        if (
            options.offload_fs_scheme in AZURE_OFFLOAD_FS_SCHEMES
            and not options.offload_fs_azure_account_name
        ):
            raise exc_cls(
                "OFFLOAD_FS_AZURE_ACCOUNT_NAME required for scheme: %s"
                % options.offload_fs_azure_account_name
            )
        if (
            options.offload_fs_scheme in AZURE_OFFLOAD_FS_SCHEMES
            and not options.offload_fs_azure_account_key
        ):
            raise exc_cls(
                "OFFLOAD_FS_AZURE_ACCOUNT_KEY required for scheme: %s"
                % options.offload_fs_azure_account_key
            )
        if (
            options.offload_fs_azure_account_domain
            and not options.offload_fs_azure_account_domain.startswith(".")
        ):
            options.offload_fs_azure_account_domain = (
                "." + options.offload_fs_azure_account_domain
            )


def normalise_listener_options(options):
    # Listener options
    if options.listener_port:
        options.listener_port = orchestration_defaults.posint_option_from_string(
            "OFFLOAD_LISTENER_PORT", options.listener_port
        )
    if options.listener_heartbeat_interval:
        options.listener_heartbeat_interval = (
            orchestration_defaults.posint_option_from_string(
                "OFFLOAD_LISTENER_HEARTBEAT_INTERVAL",
                options.listener_heartbeat_interval,
            )
        )
    if options.password_key_file:
        pass_tool = PasswordTools()
        goe_key = pass_tool.get_password_key_from_key_file(options.password_key_file)
        if options.listener_shared_token:
            options.listener_shared_token = pass_tool.b64decrypt(
                options.listener_shared_token, goe_key
            )
        if options.listener_redis_password:
            options.listener_redis_password = pass_tool.b64decrypt(
                options.listener_redis_password, goe_key
            )


def normalise_offload_transport_config(options, exc_cls=OrchestrationConfigException):
    def simple_file_csv(var_val, var_name):
        """Check for invalid characters that could be a shell injection risk."""
        if not re.match(r"^[a-z0-9\/_\-:.,]+$", var_val):
            raise exc_cls(f"Invalid characters found in {var_name}: {var_val}")

    # Spark config
    verify_json_option(
        "OFFLOAD_TRANSPORT_RDBMS_SESSION_PARAMETERS",
        options.offload_transport_rdbms_session_parameters,
    )

    if (
        options.offload_transport_spark_submit_executable
        and options.offload_transport_spark_submit_executable
        not in VALID_SPARK_SUBMIT_EXECUTABLES
    ):
        raise exc_cls(
            "Invalid value for OFFLOAD_TRANSPORT_SPARK_SUBMIT_EXECUTABLE: %s"
            % options.offload_transport_spark_submit_executable
        )
    if options.offload_transport_spark_submit_master_url and not (
        options.offload_transport_spark_submit_master_url
        == SPARK_SUBMIT_SPARK_YARN_MASTER
        or re.match(
            r"^local(\[[0-9]\])?$", options.offload_transport_spark_submit_master_url
        )
        or re.match(
            r"^(spark|mesos|k8s)://.+$",
            options.offload_transport_spark_submit_master_url,
        )
    ):
        raise exc_cls(
            "Invalid value for OFFLOAD_TRANSPORT_SPARK_SUBMIT_MASTER_URL: %s"
            % options.offload_transport_spark_submit_master_url
        )

    if options.offload_transport_spark_files:
        simple_file_csv(
            options.offload_transport_spark_files, "OFFLOAD_TRANSPORT_SPARK_FILES"
        )

    if options.offload_transport_spark_jars:
        simple_file_csv(
            options.offload_transport_spark_jars, "OFFLOAD_TRANSPORT_SPARK_JARS"
        )

    # Spark thriftserver config
    if options.offload_transport_spark_thrift_host:
        options.offload_transport_spark_thrift_host = random.choice(
            options.offload_transport_spark_thrift_host.split(",")
        )
    if options.offload_transport_spark_thrift_port:
        options.offload_transport_spark_thrift_port = (
            orchestration_defaults.posint_option_from_string(
                "OFFLOAD_TRANSPORT_SPARK_THRIFT_PORT",
                options.offload_transport_spark_thrift_port,
            )
        )

    # Livy config
    if (
        type(options.offload_transport_livy_api_verify_ssl) is not bool
        and options.offload_transport_livy_api_verify_ssl is not None
    ):
        options.offload_transport_livy_api_verify_ssl = str(
            options.offload_transport_livy_api_verify_ssl
        ).strip()
        if options.offload_transport_livy_api_verify_ssl == "":
            options.offload_transport_livy_api_verify_ssl = None
        elif options.offload_transport_livy_api_verify_ssl.lower() in ["true", "false"]:
            options.offload_transport_livy_api_verify_ssl = bool(
                options.offload_transport_livy_api_verify_ssl.lower() == "true"
            )
    if options.offload_transport_livy_api_verify_ssl is None:
        options.offload_transport_livy_api_verify_ssl = (
            derive_rest_api_verify_value_from_url(
                options.offload_transport_livy_api_url
            )
        )
    options.offload_transport_livy_max_sessions = (
        orchestration_defaults.posint_option_from_string(
            "OFFLOAD_TRANSPORT_LIVY_MAX_SESSIONS",
            options.offload_transport_livy_max_sessions,
        )
    )
    options.offload_transport_livy_idle_session_timeout = (
        orchestration_defaults.posint_option_from_string(
            "OFFLOAD_TRANSPORT_LIVY_IDLE_SESSION_TIMEOUT",
            options.offload_transport_livy_idle_session_timeout,
        )
    )

    # Generic transport config
    if options.offload_transport:
        options.offload_transport = options.offload_transport.upper()
    if (
        not options.offload_transport
        or options.offload_transport not in VALID_OFFLOAD_TRANSPORTS
    ):
        raise exc_cls(
            "Invalid value for OFFLOAD_TRANSPORT/--offload-transport: %s"
            % options.offload_transport
        )
    options.offload_transport_cmd_host = (
        options.offload_transport_cmd_host or options.hdfs_host
    )

    if not options.offload_transport_dsn:
        if options.db_type == DBTYPE_MSSQL:
            options.offload_transport_dsn = options.mssql_dsn
        elif options.db_type == DBTYPE_ORACLE:
            options.offload_transport_dsn = options.oracle_dsn
        elif options.db_type == DBTYPE_TERADATA:
            options.offload_transport_dsn = options.teradata_server

    if options.offload_staging_format:
        options.offload_staging_format = options.offload_staging_format.upper()

    # For backward compatibility
    options.offload_transport_user = (
        options.offload_transport_user or options.hadoop_ssh_user
    )
    if not options.offload_transport_user:
        raise exc_cls("OFFLOAD_TRANSPORT_USER is mandatory")


def normalise_rdbms_options(options, exc_cls=OrchestrationConfigException):
    if not options.db_type:
        options.db_type = orchestration_defaults.frontend_db_type_default()
    if getattr(options, "use_oracle_wallet", None) is None:
        options.use_oracle_wallet = False
    # Source vendor specifics
    if options.db_type == DBTYPE_MSSQL:
        normalise_mssql_options(options, exc_cls=exc_cls)
    elif options.db_type == DBTYPE_ORACLE:
        normalise_rdbms_oracle_options(options, exc_cls=exc_cls)
    elif options.db_type == DBTYPE_TERADATA:
        normalise_teradata_options(options, exc_cls=exc_cls)


def normalise_rdbms_oracle_options(options, exc_cls=OrchestrationConfigException):
    """Process Oracle connection options
    silent is required when this is called before options has been defined, we don't want to try logging anything
    before logging is ready.
    """
    if not options.oracle_dsn:
        raise exc_cls("Oracle connection options required")
    elif not options.use_oracle_wallet and (
        not options.ora_adm_user
        or not options.ora_adm_pass
        or not options.ora_app_user
        or not options.ora_app_pass
    ):
        raise exc_cls(
            "Oracle username and password must be supplied for app and admin users unless Oracle Wallet is used"
        )
    elif not options.ora_repo_user:
        raise exc_cls("Oracle repository username required")

    options.rdbms_app_user = options.ora_app_user
    options.rdbms_app_pass = options.ora_app_pass
    if options.oracle_adm_dsn:
        options.rdbms_dsn = options.oracle_adm_dsn
    else:
        options.rdbms_dsn = options.oracle_dsn

    if not options.password_key_file:
        return
    pass_tool = PasswordTools()
    goe_key = pass_tool.get_password_key_from_key_file(options.password_key_file)
    options.rdbms_app_pass = options.ora_app_pass = pass_tool.b64decrypt(
        options.rdbms_app_pass, goe_key
    )
    options.ora_adm_pass = pass_tool.b64decrypt(options.ora_adm_pass, goe_key)


def normalise_rdbms_wallet_options(options, frontend_api=None):
    if options.use_oracle_wallet:
        if frontend_api:
            # We need to set/override ora_adm_user when using Oracle Wallet
            options.ora_adm_user = frontend_api.get_session_user()
        options.offload_transport_auth_using_oracle_wallet = True


def normalise_mssql_options(options, exc_cls=OrchestrationConfigException):
    if (
        not options.mssql_app_user
        or not options.mssql_app_pass
        or not options.mssql_dsn
    ):
        raise exc_cls("Microsoft SQL Server connection options required")

    options.rdbms_app_user = options.mssql_app_user
    options.rdbms_app_pass = options.mssql_app_pass
    options.rdbms_dsn = options.mssql_dsn
    options.ora_adm_user = None
    options.ora_adm_pass = None
    options.ora_repo_user = None
    options.sqoop_password_file = None


def normalise_teradata_options(options, exc_cls=OrchestrationConfigException):
    if (
        not options.teradata_adm_user
        or not options.teradata_adm_pass
        or not options.teradata_server
    ):
        raise exc_cls("Teradata connection options required")
    if not options.teradata_app_user or not options.teradata_app_pass:
        raise exc_cls("Teradata connection options required")
    if not options.teradata_repo_user:
        raise exc_cls("Teradata repository username required")

    options.rdbms_app_user = options.teradata_app_user
    options.rdbms_app_pass = options.teradata_app_pass
    options.rdbms_dsn = options.teradata_server

    if not options.frontend_odbc_driver_name:
        raise exc_cls("FRONTEND_ODBC_DRIVER_NAME is required for Teradata connections")

    if not options.password_key_file:
        return
    pass_tool = PasswordTools()
    goe_key = pass_tool.get_password_key_from_key_file(options.password_key_file)
    options.rdbms_app_pass = options.teradata_app_pass = pass_tool.b64decrypt(
        options.teradata_app_pass, goe_key
    )
    options.teradata_adm_pass = pass_tool.b64decrypt(options.teradata_adm_pass, goe_key)


def normalise_size_option(
    opt_val, binary_sizes=False, strict_name=None, exc_cls=OrchestrationConfigException
):
    """strict_name can be used to raise an exception if incorrectly formatted inputs are
    passed in. Otherwise we just return 0.
    strict_name is the name of the option to include in the exception
    """
    if opt_val and type(opt_val) is str:
        new_val = opt_val.upper() if opt_val else None
    else:
        new_val = opt_val
    new_val = human_size_to_bytes(new_val, binary_sizes=binary_sizes)
    if strict_name and new_val is None:
        raise exc_cls("Invalid %s: %s" % (strict_name, opt_val))
    return new_val or 0


def normalise_snowflake_options(options, exc_cls=OrchestrationConfigException):
    if options.backend_distribution != BACKEND_DISTRO_SNOWFLAKE:
        return
    if not options.snowflake_user:
        raise exc_cls("SNOWFLAKE_USER is mandatory")
    if not options.snowflake_pass and not options.snowflake_pem_file:
        raise exc_cls("Either SNOWFLAKE_PASS or SNOWFLAKE_PEM_FILE is required")
    if not options.snowflake_account:
        raise exc_cls("SNOWFLAKE_ACCOUNT is mandatory")
    if not options.snowflake_database:
        raise exc_cls("SNOWFLAKE_DATABASE is mandatory")
    if not options.snowflake_file_format_prefix:
        raise exc_cls("SNOWFLAKE_FILE_FORMAT_PREFIX is mandatory")
    if not options.snowflake_integration:
        raise exc_cls("SNOWFLAKE_INTEGRATION is mandatory")
    if not options.snowflake_role:
        raise exc_cls("SNOWFLAKE_ROLE is mandatory")
    if not options.snowflake_stage:
        raise exc_cls("SNOWFLAKE_STAGE is mandatory")
    if not options.snowflake_warehouse:
        raise exc_cls("SNOWFLAKE_WAREHOUSE is mandatory")


def normalise_synapse_options(options, exc_cls=OrchestrationConfigException):
    if options.backend_distribution != BACKEND_DISTRO_MSAZURE:
        return
    if not options.synapse_auth_mechanism:
        raise exc_cls("SYNAPSE_AUTH_MECHANISM is mandatory")
    if options.synapse_auth_mechanism not in SYNAPSE_VALID_AUTH_MECHANISMS:
        raise exc_cls(
            "Invalid value for SYNAPSE_AUTH_MECHANISM: %s. Must be one of: %s"
            % (options.synapse_auth_mechanism, ", ".join(SYNAPSE_VALID_AUTH_MECHANISMS))
        )
    if options.synapse_auth_mechanism in SYNAPSE_USER_PASS_AUTH_MECHANISMS:
        if not options.synapse_user:
            raise exc_cls("SYNAPSE_USER is mandatory")
        if not options.synapse_pass:
            raise exc_cls("SYNAPSE_PASS is mandatory")
    if options.synapse_auth_mechanism == SYNAPSE_AUTH_MECHANISM_AD_SERVICE_PRINCIPAL:
        if not options.synapse_service_principal_id:
            raise exc_cls("SYNAPSE_SERVICE_PRINCIPAL_ID is mandatory")
        if not options.synapse_service_principal_secret:
            raise exc_cls("SYNAPSE_SERVICE_PRINCIPAL_SECRET is mandatory")
    if not options.synapse_server:
        raise exc_cls("SYNAPSE_SERVER is mandatory")
    if not options.synapse_database:
        raise exc_cls("SYNAPSE_DATABASE is mandatory")
    if not options.synapse_port:
        raise exc_cls("SYNAPSE_PORT is mandatory")
    if not options.synapse_data_source:
        raise exc_cls("SYNAPSE_DATA_SOURCE is mandatory")
    if not options.synapse_file_format:
        raise exc_cls("SYNAPSE_FILE_FORMAT is mandatory")
    if not options.synapse_role:
        raise exc_cls("SYNAPSE_ROLE is mandatory")
    if not options.backend_odbc_driver_name:
        raise exc_cls("BACKEND_ODBC_DRIVER_NAME is mandatory")


def normalise_webhdfs(options):
    def webhdfs_default_port(webhdfs_verify_ssl):
        default_http_port = 50070
        default_https_port = 50470
        return default_http_port if webhdfs_verify_ssl is None else default_https_port

    if isinstance(options.webhdfs_verify_ssl, bool):
        return
    if options.webhdfs_verify_ssl is not None:
        if options.webhdfs_verify_ssl.lower() in ["true", "false"]:
            options.webhdfs_verify_ssl = (
                True if options.webhdfs_verify_ssl.lower() == "true" else False
            )
        elif not str(options.webhdfs_verify_ssl).strip():
            options.webhdfs_verify_ssl = None

    options.webhdfs_port = (
        options.webhdfs_port
        or orchestration_defaults.webhdfs_port_default()
        or webhdfs_default_port(options.webhdfs_verify_ssl)
    )


def check_offload_fs_scheme_supported_in_backend(offload_fs_scheme, backend_api):
    if offload_fs_scheme not in backend_api.valid_offload_fs_schemes():
        display_version = backend_api.target_version()
        display_version = " ({})".format(display_version) if display_version else ""
        raise OrchestrationConfigException(
            "Offload to %s%s does not support filesystem scheme: %s"
            % (backend_api.backend_db_name(), display_version, offload_fs_scheme)
        )
