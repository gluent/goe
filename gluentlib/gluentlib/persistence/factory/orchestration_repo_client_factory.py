#! /usr/bin/env python3
""" LICENSE_TEXT
"""

from gluentlib.offload.offload_constants import DBTYPE_MSSQL, DBTYPE_NETEZZA, DBTYPE_ORACLE, DBTYPE_TERADATA


def orchestration_repo_client_factory(connection_options, messages, dry_run=None):
    if dry_run is None:
        if hasattr(connection_options, 'execute'):
            dry_run = bool(not connection_options.execute)
        else:
            dry_run = False
    if connection_options.db_type == DBTYPE_ORACLE:
        from gluentlib.persistence.oracle.oracle_orchestration_repo_client import OracleOrchestrationRepoClient
        return OracleOrchestrationRepoClient(connection_options, messages, dry_run=dry_run)
    elif connection_options.db_type == DBTYPE_TERADATA:
        from gluentlib.persistence.teradata.teradata_orchestration_repo_client import TeradataOrchestrationRepoClient
        return TeradataOrchestrationRepoClient(connection_options, messages, dry_run=dry_run)
    else:
        raise NotImplementedError('Unsupported RDBMS: %s' % connection_options.db_type)
