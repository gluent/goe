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
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
)


def orchestration_repo_client_factory(
    connection_options, messages, dry_run=None, trace_action=None
):
    if dry_run is None:
        if hasattr(connection_options, "execute"):
            dry_run = bool(not connection_options.execute)
        else:
            dry_run = False
    if connection_options.db_type == DBTYPE_ORACLE:
        from goe.persistence.oracle.oracle_orchestration_repo_client import (
            OracleOrchestrationRepoClient,
        )

        return OracleOrchestrationRepoClient(
            connection_options, messages, dry_run=dry_run, trace_action=trace_action
        )
    elif connection_options.db_type == DBTYPE_TERADATA:
        from goe.persistence.teradata.teradata_orchestration_repo_client import (
            TeradataOrchestrationRepoClient,
        )

        return TeradataOrchestrationRepoClient(
            connection_options, messages, dry_run=dry_run, trace_action=trace_action
        )
    else:
        raise NotImplementedError("Unsupported RDBMS: %s" % connection_options.db_type)
