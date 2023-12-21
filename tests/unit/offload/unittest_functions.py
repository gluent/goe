#! /usr/bin/env python3
"""
    Helper functions for other unit test modules.
"""

from goe.config.orchestration_config import OrchestrationConfig
from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_NETEZZA,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
)


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
