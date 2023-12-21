#! /usr/bin/env python3
""" LICENSE_TEXT
"""

import logging

from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_NETEZZA,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
)


logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


class OffloadSourceTable(object):
    """OffloadSourceTable sits in front of third party database specific implementations.
    Depending on value in connection_options.db_type the construct method will return
    the appropriate class for the DB in use.
    We import frontend classes inside the factory function to avoid loading libraries we'll never use.
    """

    @staticmethod
    def create(
        schema_name,
        table_name,
        connection_options,
        messages,
        dry_run=False,
        offload_by_subpartition=False,
        conn=None,
        do_not_connect=False,
    ):
        logger.info(
            "OffloadSourceTable constructing for %s" % connection_options.db_type
        )
        if connection_options.db_type == DBTYPE_ORACLE:
            from goe.offload.oracle.oracle_offload_source_table import OracleSourceTable

            rdbms_table = OracleSourceTable(
                schema_name,
                table_name,
                connection_options,
                messages,
                dry_run=dry_run,
                conn=conn,
                do_not_connect=do_not_connect,
            )
            if offload_by_subpartition:
                rdbms_table.enable_offload_by_subpartition()
            return rdbms_table
        elif connection_options.db_type == DBTYPE_MSSQL:
            from goe.offload.microsoft.mssql_offload_source_table import (
                MSSQLSourceTable,
            )

            return MSSQLSourceTable(
                schema_name,
                table_name,
                connection_options,
                messages,
                dry_run=dry_run,
                do_not_connect=do_not_connect,
            )
        elif connection_options.db_type == DBTYPE_NETEZZA:
            from goe.offload.netezza.netezza_offload_source_table import (
                NetezzaSourceTable,
            )

            return NetezzaSourceTable(
                schema_name,
                table_name,
                connection_options,
                messages,
                dry_run=dry_run,
                do_not_connect=do_not_connect,
            )
        elif connection_options.db_type == DBTYPE_TERADATA:
            from goe.offload.teradata.teradata_offload_source_table import (
                TeradataSourceTable,
            )

            return TeradataSourceTable(
                schema_name,
                table_name,
                connection_options,
                messages,
                dry_run=dry_run,
                do_not_connect=do_not_connect,
            )
