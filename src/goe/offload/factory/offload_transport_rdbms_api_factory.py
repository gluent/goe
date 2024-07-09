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

from goe.offload.offload_constants import (
    DBTYPE_MSSQL,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
)


def offload_transport_rdbms_api_factory(
    rdbms_owner,
    rdbms_table_name,
    offload_options,
    messages,
    dry_run=False,
):
    """Constructs and returns an appropriate data transport object based on source RDBMS"""
    if offload_options.db_type == DBTYPE_ORACLE:
        from goe.offload.oracle.oracle_offload_transport_rdbms_api import (
            OffloadTransportOracleApi,
        )

        return OffloadTransportOracleApi(
            rdbms_owner,
            rdbms_table_name,
            offload_options,
            messages,
            dry_run=dry_run,
        )
    elif offload_options.db_type == DBTYPE_MSSQL:
        from goe.offload.microsoft.mssql_offload_transport_rdbms_api import (
            OffloadTransportMSSQLApi,
        )

        return OffloadTransportMSSQLApi(
            rdbms_owner, rdbms_table_name, offload_options, messages, dry_run=dry_run
        )
    elif offload_options.db_type == DBTYPE_TERADATA:
        from goe.offload.teradata.teradata_offload_transport_rdbms_api import (
            OffloadTransportTeradataApi,
        )

        return OffloadTransportTeradataApi(
            rdbms_owner, rdbms_table_name, offload_options, messages, dry_run=dry_run
        )
    else:
        raise NotImplementedError(
            "Offload transport source RDBMS not implemented: %s"
            % offload_options.db_type
        )
