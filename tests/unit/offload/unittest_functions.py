#! /usr/bin/env python3
"""
    Helper functions for other unit test modules.
"""

import os
from unittest import mock

from goe.config.orchestration_config import OrchestrationConfig
from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_NETEZZA,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
)
from goe.offload.offload_messages import OffloadMessages
from goe.persistence.orchestration_metadata import OrchestrationMetadata


def build_non_connecting_options(db_type):
    base_dict = {
        "execute": False,
        "verbose": False,
        "db_type": db_type,
        "password_key_file": None,
        "frontend_odbc_driver_name": "Driver Name",
    }
    if db_type == DBTYPE_ORACLE:
        base_dict.update(
            {
                "ora_adm_user": "adm",
                "ora_adm_pass": "adm",
                "ora_app_user": "app",
                "ora_app_pass": "app",
                "ora_repo_user": "repo",
                "oracle_dsn": "host/TEST",
            }
        )
    elif db_type == DBTYPE_MSSQL:
        base_dict.update(
            {
                "mssql_app_user": "adm",
                "mssql_app_pass": "adm",
                "mssql_dsn": "blah:blah;blah=blah",
            }
        )
    elif db_type == DBTYPE_NETEZZA:
        base_dict.update(
            {
                "netezza_app_user": "adm",
                "netezza_app_pass": "adm",
                "netezza_dsn": "blah:blah;blah=blah",
            }
        )
    elif db_type == DBTYPE_TERADATA:
        base_dict.update(
            {
                "teradata_adm_user": "adm",
                "teradata_adm_pass": "adm",
                "teradata_app_user": "app",
                "teradata_app_pass": "app",
                "teradata_repo_user": "repo",
                "teradata_server": "host",
            }
        )
    return OrchestrationConfig.from_dict(base_dict)


def get_real_frontend_schema_and_table(
    hybrid_view, orchestration_config, messages=None
):
    """Try to find an offloaded hybrid_view via metadata"""
    if not messages:
        messages = OffloadMessages()
    for hybrid_schema in [get_default_test_user(hybrid=True), "SH_H"]:
        metadata = OrchestrationMetadata.from_name(
            hybrid_schema,
            hybrid_view,
            connection_options=orchestration_config,
            messages=messages,
        )
        if metadata:
            return metadata.offloaded_owner, metadata.offloaded_table
    # We shouldn't get to here in a correctly configured environment
    raise Exception(
        f"{hybrid_view} test table is missing, please configure your environment"
    )
