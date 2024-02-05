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
from goe.util.misc_functions import double_quote_sandwich
from goe.util.ora_query import get_oracle_connection
from .. import schema_sync_constants
from ..schema_sync_step import SchemaSyncStep

normal, verbose, vverbose = list(range(3))


class AddOracleColumn(SchemaSyncStep):
    def __init__(
        self, options, orchestration_options, messages, execution_id, repo_client
    ):
        if not orchestration_options.db_type == "oracle":
            raise NotImplementedError(
                'Database type "%s" not supported' % orchestration_options.db_type
            )

        super().__init__(
            schema_sync_constants.ADD_ORACLE_COLUMN,
            options,
            orchestration_options,
            messages,
            execution_id,
            repo_client,
        )

    def _do_run(self, run_params):
        rdbms_owner = run_params["rdbms_owner"].upper()
        rdbms_table_name = run_params["rdbms_table_name"].upper()
        hybrid_owner = run_params["hybrid_owner"].upper()

        log(
            "Adding columns to %s.%s table"
            % (
                double_quote_sandwich(rdbms_owner),
                double_quote_sandwich(rdbms_table_name),
            )
        )

        new_rdbms_cols = [
            "%s %s" % (double_quote_sandwich(_.name), _.format_data_type())
            for _ in run_params["columns"]
        ]
        sql = "ALTER TABLE %s.%s ADD (%s)" % (
            double_quote_sandwich(rdbms_owner),
            double_quote_sandwich(rdbms_table_name),
            ", ".join(new_rdbms_cols),
        )
        log("Oracle sql: " + sql, verbose)

        try:
            if self._options.execute:
                conn = get_oracle_connection(
                    self._orchestration_options.ora_adm_user,
                    self._orchestration_options.ora_adm_pass,
                    self._orchestration_options.rdbms_dsn,
                    self._orchestration_options.use_oracle_wallet,
                    ora_proxy_user=hybrid_owner,
                )
                cursor = conn.cursor()
                cursor.execute(sql)
                cursor.close()

            return None, [sql]

        except Exception as exc:
            source_name = "%s.%s" % (
                double_quote_sandwich(rdbms_owner),
                double_quote_sandwich(rdbms_table_name),
            )
            log(
                "%s\n%s"
                % (
                    schema_sync_constants.EXCEPTION_ADD_ORACLE_COLUMN,
                    traceback.format_exc(),
                ),
                verbose,
            )
            self._messages.warning(
                "%s for table %s"
                % (schema_sync_constants.EXCEPTION_ADD_ORACLE_COLUMN, source_name),
                ansi_code="red",
            )
            return schema_sync_constants.EXCEPTION_ADD_ORACLE_COLUMN, [sql]
