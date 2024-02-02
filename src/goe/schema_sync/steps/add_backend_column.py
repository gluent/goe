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

import traceback

from goe.goe import log
from goe.offload.offload_messages import VVERBOSE
from goe.offload.offload_constants import BACKEND_DISTRO_GCP
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.util.misc_functions import double_quote_sandwich
from .. import schema_sync_constants
from ..schema_sync_step import SchemaSyncStep

normal, verbose, vverbose = list(range(3))


class AddBackendColumn(SchemaSyncStep):
    def __init__(
        self, options, orchestration_options, messages, execution_id, repo_client
    ):
        super().__init__(
            schema_sync_constants.ADD_BACKEND_COLUMN,
            options,
            orchestration_options,
            messages,
            execution_id,
            repo_client,
        )

    def _do_run(self, run_params):
        rdbms_owner = run_params["rdbms_owner"]
        rdbms_table_name = run_params["rdbms_table_name"]
        backend_owner = run_params["backend_owner"]
        backend_table_name = run_params["backend_table_name"]

        offload_source_table = OffloadSourceTable.create(
            rdbms_owner, rdbms_table_name, self._orchestration_options, self._messages
        )

        log(
            "Adding columns to %s.%s table"
            % (
                self._backend_api.enclose_identifier(backend_owner),
                self._backend_api.enclose_identifier(backend_table_name),
            )
        )

        new_backend_cols = []
        for rdbms_column in run_params["columns"]:
            canonical_column = offload_source_table.to_canonical_column(rdbms_column)
            log(
                "Mapped column %s to canonical column: %s"
                % (rdbms_column.name.upper(), str(canonical_column)),
                detail=VVERBOSE,
            )
            backend_column = self._backend_api.from_canonical_column(canonical_column)
            data_type = backend_column.format_data_type()
            log(
                "Canonical to backend mapping: %s->%s"
                % (canonical_column.data_type, data_type),
                detail=VVERBOSE,
            )
            new_backend_cols.append((rdbms_column.name, data_type))

        sqls = []
        try:
            sqls = self._backend_api.add_columns(
                backend_owner, backend_table_name, new_backend_cols, sync=True
            )
            if self._orchestration_options.backend_distribution == BACKEND_DISTRO_GCP:
                sqls = ["BigQuery call: %s" % sql for sql in sqls]
                sqls.insert(
                    -1,
                    "Missing columns should be added via the API or the BigQuery console. The API call is shown below:\n",
                )
            return None, sqls

        except Exception as exc:
            source_name = "%s.%s" % (
                double_quote_sandwich(rdbms_owner.upper()),
                double_quote_sandwich(rdbms_table_name.upper()),
            )
            log(
                "%s\n%s"
                % (
                    schema_sync_constants.EXCEPTION_ADD_BACKEND_COLUMN,
                    traceback.format_exc(),
                ),
                verbose,
            )
            self._messages.warning(
                "%s for table %s"
                % (schema_sync_constants.EXCEPTION_ADD_BACKEND_COLUMN, source_name),
                ansi_code="red",
            )
            return schema_sync_constants.EXCEPTION_ADD_BACKEND_COLUMN, sqls
