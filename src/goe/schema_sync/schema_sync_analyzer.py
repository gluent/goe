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

""" SchemaSyncAnalyzer: Library for analyzing Schema Sync operations
"""

import copy
import datetime
import pprint
import traceback
from operator import itemgetter

from goe.goe import log
from goe.config.orchestration_config import OrchestrationConfig
import goe.schema_sync.schema_sync_constants as schema_sync_constants
from goe.offload.column_metadata import get_column_names
from goe.offload.factory.backend_api_factory import backend_api_factory
from goe.offload.factory.backend_table_factory import backend_table_factory
from goe.offload.factory.offload_source_table_factory import OffloadSourceTable
from goe.offload.offload_messages import VERBOSE
from goe.util.misc_functions import double_quote_sandwich
from goe.util.ora_query import OracleQuery
from goe.util.ora_query import get_oracle_connection

dense = pprint.PrettyPrinter(indent=2)
normal, verbose, vverbose = list(range(3))
c_name, c_data_type = list(range(2))
owner, object_name, object_type, last_ddl_time = list(range(4))
DATETIME_CONSTANT = datetime.datetime(1977, 4, 19, 0, 0)


def ddl_compare(cursor, source_object, target_object):
    """Compare the LAST_DDL_TIME of the source_object (tuple: (owner,object_name,object_type))
    with that of the target_object (tuple: (owner,object_name,object_type)).

    Return True if:

        source_object: LAST_DDL_TIME > target_object: LAST_DDL_TIME

        or

        target_object does not exist

    Note:   This select statement must use dba_objects, because all_objects requires the user to
            have the CREATE ANY TRIGGER or DEBUG ANY PROCEDURE privilege for triggers to be visible,
            and the GOE ADM user does not possess these privileges.
    """

    sql = """
        SELECT  owner
        ,       object_name
        ,       object_type
        ,       last_ddl_time
        FROM    dba_objects
        WHERE   (owner,object_name,object_type) IN ({source},{target})""".format(
        source=source_object, target=target_object
    )

    result = cursor.execute(sql).fetchall()

    source_exists = [
        ddl[last_ddl_time]
        for ddl in result
        if ddl[object_name] == source_object[object_name].upper()
    ]
    source_last_ddl_time = source_exists[0] if source_exists else DATETIME_CONSTANT

    target_exists = [
        ddl[last_ddl_time]
        for ddl in result
        if ddl[object_name] == target_object[object_name].upper()
    ]
    target_last_ddl_time = target_exists[0] if target_exists else DATETIME_CONSTANT

    return bool(source_last_ddl_time > target_last_ddl_time or not target_exists)


class SchemaSyncAnalyzerException(Exception):
    pass


class SchemaSyncAnalyzer(object):
    """Class for comparing rdbms and backend structures"""

    def __init__(self, options, messages):
        self._options = options
        self._messages = messages
        self._orchestration_options = OrchestrationConfig.from_dict(
            {
                "verbose": options.verbose,
                "vverbose": options.vverbose,
            }
        )

        cx_conn = get_oracle_connection(
            self._orchestration_options.ora_adm_user,
            self._orchestration_options.ora_adm_pass,
            self._orchestration_options.rdbms_dsn,
            self._orchestration_options.use_oracle_wallet,
        )
        self._ora_adm_conn = OracleQuery.fromconnection(cx_conn)
        self._backend_api = backend_api_factory(
            self._orchestration_options.target,
            self._orchestration_options,
            self._messages,
            dry_run=(not self._options.execute),
        )

    def backend_enclose(self, identifier):
        return self._backend_api.enclose_identifier(identifier)

    def expand_wildcard_scope(self, includes):
        def all_offload_objects_sql():
            sql = """
                SELECT  offloaded_owner
                ,       offloaded_table
                ,       hadoop_owner
                ,       hadoop_table
                ,       hybrid_owner
                ,       hybrid_view
                ,       hybrid_external_table
                FROM    offload_objects
                WHERE   hybrid_view_type = 'GOE_OFFLOAD_HYBRID_VIEW'
                AND     offloaded_owner IS NOT NULL
                AND     offloaded_table IS NOT NULL"""
            binds = None
            return sql, binds

        def schema_offload_objects_sql(owner):
            sql = (
                all_offload_objects_sql()[0]
                + """
                AND     offloaded_owner LIKE UPPER(:OWNER)
            """
            )
            binds = {"OWNER": owner}
            return sql, binds

        def table_offload_objects_sql(owner, table_name):
            sql = (
                schema_offload_objects_sql(owner)[0]
                + "    AND     offloaded_table LIKE UPPER(:TABLE_NAME)"
            )
            binds = {"OWNER": owner, "TABLE_NAME": table_name}
            return sql, binds

        expanded = []

        for include in includes.split(","):
            if "." in include:
                schema, table = include.split(".", 1)
                if schema == "*" and table == "*":
                    sql, binds = all_offload_objects_sql()
                else:
                    if schema == "*":
                        schema = "%"
                    if table == "*":
                        table = "%"
                    owner = [schema[:-1] + "%" if schema[-1] == "*" else schema][0]
                    table_name = [table[:-1] + "%" if table[-1] == "*" else table][0]
                    sql, binds = table_offload_objects_sql(owner, table_name)
            else:
                if include == "*":
                    include = "%"
                owner = [include[:-1] + "%" if include[-1] == "*" else include][0]
                sql, binds = schema_offload_objects_sql(owner)

            result = self._ora_adm_conn.execute(
                sql, binds=binds, cursor_fn=lambda c: c.fetchall(), as_dict=True
            )
            if result:
                # convert dictionary keys returned above to lowercase (GOE-992)
                expanded += [dict((k.lower(), v) for k, v in r.items()) for r in result]
            else:
                self._messages.warning("No Hybrid View found for %s" % include)

        log(
            'Expanded --include: "%s" to %i table(s)' % (includes, len(expanded)),
            verbose,
        )

        return sorted(expanded, key=itemgetter("offloaded_owner", "offloaded_table"))

    def normalize_rdbms_structure(self, owner, table_name):
        try:
            offload_source_table = OffloadSourceTable.create(
                owner, table_name, self._orchestration_options, self._messages
            )
            return offload_source_table.columns
        except Exception:
            self._messages.warning(
                "Unable to retrieve details for RDBMS table %s.%s" % (owner, table_name)
            )
            return None

    def normalize_backend_structure(self, owner, table_name):
        try:
            return self._backend_api.get_non_synthetic_columns(owner, table_name)
        except Exception:
            self._messages.log(
                "Exception retrieving details for backend table %s.%s:\n%s"
                % (owner, table_name, traceback.format_exc()),
                detail=VERBOSE,
            )
            self._messages.warning(
                "Unable to retrieve details for backend table %s.%s"
                % (owner, table_name)
            )
            return None

    def check_valid_rdbms_columns(self, owner, table_name, columns):
        """
        New column rules

            We skip the table if these are not met:

                Must be visible
                Must have a datatype that is supported by offload

            We show a warning but continue if these are not met:

                Are assumed to be NULL
                Default values are not considered

            Back-population is not considered

            If this is an incremental enabled table, then disallow INTERVAL YEAR TO MONTH (GOE-1002)
        """
        is_valid = True
        offload_source_table = OffloadSourceTable.create(
            owner, table_name, self._orchestration_options, self._messages
        )

        for column in columns:
            if column.is_hidden():
                self._messages.warning(
                    'Hidden column "%s" for RDBMS table "%s.%s" not supported. Skipping table'
                    % (column.name, owner, table_name)
                )
                is_valid = False

            if column.data_default:
                self._messages.warning(
                    'Column "%s" for RDBMS table "%s.%s" has default value "%s". Existing column data will not be synchronized'
                    % (column.name, owner, table_name, column.data_default.strip())
                )

            if not column.is_nullable():
                self._messages.warning(
                    'Column "%s" for RDBMS table "%s.%s" is not nullable. Existing column data will not be synchronized'
                    % (column.name, owner, table_name)
                )

            if column.data_type not in offload_source_table.supported_data_types():
                self._messages.warning(
                    'Column "%s" has unsupported datatype "%s" for RDBMS table "%s.%s". Skipping table'
                    % (column.name, column.data_type, owner, table_name)
                )
                is_valid = False

        return is_valid

    def compare_table_columns(self, first_table, second_table):
        """
        Compare the columns in first_table with those in second_table, return differences only
        if the first_table has columns not in the second_table.

        first_table, second_table params are tuples in the form (owner,table_name,source)

        Example 1:

            first_table  = {1, 2, 3, 4, 5}
            second_table = {4, 5, 6, 7, 8}

            difference: {1, 2, 3}
            returns: {1, 2, 3} from first_table

        Example 2:

            first_table  = {1, 2, 3}
            second_table = {1, 2, 3, 7, 8}

            difference: {}
            returns: None
        """
        assert isinstance(first_table, tuple), "first_table parameter must be a tuple"
        assert isinstance(second_table, tuple), "second_table parameter must be a tuple"
        assert (
            len(first_table) == 3
        ), "first_table parameter tuple must contain 3 elements: (owner, table_name, source)"
        assert (
            len(second_table) == 3
        ), "second_table parameter tuple must contain 3 elements: (owner, table_name, source)"

        f_owner, f_table_name, f_source = first_table
        s_owner, s_table_name, s_source = second_table

        assert f_source in [
            schema_sync_constants.SOURCE_TYPE_RDBMS,
            schema_sync_constants.SOURCE_TYPE_BACKEND,
        ], 'first_table tuple "source" element must be one of: "%s", "%s"' % (
            schema_sync_constants.SOURCE_TYPE_RDBMS,
            schema_sync_constants.SOURCE_TYPE_BACKEND,
        )
        assert s_source in [
            schema_sync_constants.SOURCE_TYPE_RDBMS,
            schema_sync_constants.SOURCE_TYPE_BACKEND,
        ], 'second_table tuple "source" element must be one of: "%s", "%s"' % (
            schema_sync_constants.SOURCE_TYPE_RDBMS,
            schema_sync_constants.SOURCE_TYPE_BACKEND,
        )

        if f_source == schema_sync_constants.SOURCE_TYPE_RDBMS:
            f_structure = self.normalize_rdbms_structure(f_owner, f_table_name)
        elif f_source == schema_sync_constants.SOURCE_TYPE_BACKEND:
            f_structure = self.normalize_backend_structure(f_owner, f_table_name)

        if s_source == schema_sync_constants.SOURCE_TYPE_RDBMS:
            s_structure = self.normalize_rdbms_structure(s_owner, s_table_name)
        elif s_source == schema_sync_constants.SOURCE_TYPE_BACKEND:
            s_structure = self.normalize_backend_structure(s_owner, s_table_name)

        comp_f_structure = (
            set(get_column_names(f_structure, conv_fn=lambda x: x.upper()))
            if f_structure
            else None
        )
        comp_s_structure = (
            set(get_column_names(s_structure, conv_fn=lambda x: x.upper()))
            if s_structure
            else None
        )

        if not comp_f_structure or not comp_s_structure:
            delta_cols = None
        else:
            if comp_f_structure.difference(comp_s_structure):
                delta_cols = [
                    col
                    for col in f_structure
                    if col.name in comp_f_structure.difference(comp_s_structure)
                ]
            else:
                delta_cols = None

        return delta_cols

    def compare_structures(self, include):
        """Construct and return change steps (vectors) for schema.table (include)

        Each comparison is a self-contained unit of work that performs these functions:

        1. Perform comparison to ascertain if a difference exists
        2. Prepare the vector details to be used by the processor which passes them as parameters to a vector['type'] step module
        3. Handles logging

        This function must be re-runnable such that, e.g. if there are 9 comparisons and on the first execution
        6 are successful but the 7th abends, then a re-run will perform the 6 checks and realise there is nothing to
        do, and then attempt the 7th once again.

        This is possible because the processing of steps is sequential and halts when a step abends.
        """
        assert include["offloaded_owner"], "No RDBMS table owner detected"
        assert include["offloaded_table"], "No RDBMS table detected"
        assert include["hadoop_owner"], "No backend table owner detected"
        assert include["hadoop_table"], "No backend table detected"
        assert include["hybrid_view"], "No hybrid view detected"
        assert include["hybrid_owner"], "No hybrid owner detected"
        assert include["hybrid_external_table"], "No hybrid external table detected"

        rdbms_owner = include["offloaded_owner"]
        rdbms_table_name = include["offloaded_table"]
        backend_owner = include["hadoop_owner"]
        backend_table_name = include["hadoop_table"]
        hybrid_owner = include["hybrid_owner"]
        hybrid_view_name = include["hybrid_view"]
        hybrid_ext_table_name = include["hybrid_external_table"]

        backend_table = backend_table_factory(
            backend_owner,
            backend_table_name,
            self._orchestration_options.target,
            self._orchestration_options,
            self._messages,
            dry_run=bool(not self._options.execute),
            frontend_owner_override=rdbms_owner,
            frontend_name_override=rdbms_table_name,
        )

        if not backend_table.schema_evolution_supported():
            raise SchemaSyncAnalyzerException(
                "%s: %s"
                % (
                    schema_sync_constants.EXCEPTION_SCHEMA_EVOLUTION_NOT_SUPPORTED,
                    backend_table.backend_db_name(),
                )
            )

        change = {}
        vectors = []
        exception = None

        log("")
        log(
            "Comparing RDBMS with backend for %s.%s"
            % (
                double_quote_sandwich(rdbms_owner),
                double_quote_sandwich(rdbms_table_name),
            )
        )

        # TODO ss@2020-05-20 this check is in place due to GOE-1030
        if (
            rdbms_owner.lower() + rdbms_table_name.lower()
            != backend_owner.lower().replace(
                self._orchestration_options.db_name_pattern.lower() % "", ""
            )
            + backend_table_name.lower()
        ):
            source_name = "%s.%s" % (
                double_quote_sandwich(include["offloaded_owner"]),
                double_quote_sandwich(include["offloaded_table"]),
            )
            self._messages.warning(
                "%s for table %s"
                % (
                    schema_sync_constants.EXCEPTION_ORACLE_HIVE_NAME_MISMATCH,
                    source_name,
                ),
                ansi_code="red",
            )
        else:
            """Non Incremental Update table

            Run comparisons in the following order:

                1. Compare base Oracle columns with base table offloaded columns (excluding GOE-operational columns)
                2. Compare base Oracle columns with Hybrid External Table and Hybrid View

            """

            # 1. Compare base Oracle columns with backend offloaded columns (excluding GOE-operational columns)
            #
            delta_base_oracle_cols = self.compare_table_columns(
                (
                    rdbms_owner,
                    rdbms_table_name,
                    schema_sync_constants.SOURCE_TYPE_RDBMS,
                ),
                (
                    backend_owner,
                    backend_table_name,
                    schema_sync_constants.SOURCE_TYPE_BACKEND,
                ),
            )

            if delta_base_oracle_cols:
                if self.check_valid_rdbms_columns(
                    rdbms_owner, rdbms_table_name, delta_base_oracle_cols
                ):
                    vector = {
                        "type": schema_sync_constants.ADD_BACKEND_COLUMN,
                        "rdbms_owner": rdbms_owner,
                        "rdbms_table_name": rdbms_table_name,
                        "backend_owner": backend_owner,
                        "backend_table_name": backend_table_name,
                        "columns": delta_base_oracle_cols,
                    }
                    vectors.append(vector)

                    log(
                        "%s.%s columns not aligned with backend %s.%s: %s"
                        % (
                            double_quote_sandwich(rdbms_owner),
                            double_quote_sandwich(rdbms_table_name),
                            backend_owner,
                            backend_table_name,
                            get_column_names(delta_base_oracle_cols),
                        ),
                        vverbose,
                    )
                else:
                    log(
                        "%s.%s columns not aligned with backend %s.%s, but invalid columns detected"
                        % (
                            double_quote_sandwich(rdbms_owner),
                            double_quote_sandwich(rdbms_table_name),
                            backend_owner,
                            backend_table_name,
                        ),
                        vverbose,
                    )
            else:
                log(
                    "%s.%s columns aligned with backend %s.%s, skipping"
                    % (
                        double_quote_sandwich(rdbms_owner),
                        double_quote_sandwich(rdbms_table_name),
                        backend_owner,
                        backend_table_name,
                    ),
                    vverbose,
                )

            # 2. Compare base Oracle columns with Hybrid External Table and Hybrid View. Compare LAST_DDL_TIME of AGG and CNT_AGG Hybrid View with base Hybrid View
            #
            delta_hybrid_view_cols = self.compare_table_columns(
                (
                    rdbms_owner,
                    rdbms_table_name,
                    schema_sync_constants.SOURCE_TYPE_RDBMS,
                ),
                (
                    hybrid_owner,
                    hybrid_view_name,
                    schema_sync_constants.SOURCE_TYPE_RDBMS,
                ),
            )
            delta_hybrid_ext_cols = self.compare_table_columns(
                (
                    rdbms_owner,
                    rdbms_table_name,
                    schema_sync_constants.SOURCE_TYPE_RDBMS,
                ),
                (
                    hybrid_owner,
                    hybrid_ext_table_name,
                    schema_sync_constants.SOURCE_TYPE_RDBMS,
                ),
            )

            vector = {}
            if delta_hybrid_ext_cols:
                if self.check_valid_rdbms_columns(
                    rdbms_owner, rdbms_table_name, delta_hybrid_ext_cols
                ):
                    vector["type"] = schema_sync_constants.PRESENT_TABLE
                    vector["rdbms_owner"] = rdbms_owner
                    vector["rdbms_table_name"] = rdbms_table_name
                    vector["backend_owner"] = backend_owner
                    vector["backend_table_name"] = backend_table_name
                    vectors.append(vector)

                    log(
                        "%s.%s columns not aligned with %s.%s: %s"
                        % (
                            double_quote_sandwich(rdbms_owner),
                            double_quote_sandwich(rdbms_table_name),
                            double_quote_sandwich(hybrid_owner),
                            double_quote_sandwich(hybrid_ext_table_name),
                            get_column_names(delta_hybrid_ext_cols),
                        ),
                        vverbose,
                    )
                else:
                    log(
                        "%s.%s columns not aligned with %s.%s, but invalid columns detected"
                        % (
                            double_quote_sandwich(rdbms_owner),
                            double_quote_sandwich(rdbms_table_name),
                            double_quote_sandwich(hybrid_owner),
                            double_quote_sandwich(hybrid_ext_table_name),
                        ),
                        vverbose,
                    )
            else:
                log(
                    "%s.%s columns aligned with %s.%s, skipping"
                    % (
                        double_quote_sandwich(rdbms_owner),
                        double_quote_sandwich(rdbms_table_name),
                        double_quote_sandwich(hybrid_owner),
                        double_quote_sandwich(hybrid_ext_table_name),
                    ),
                    vverbose,
                )

            if delta_hybrid_view_cols:
                if self.check_valid_rdbms_columns(
                    rdbms_owner, rdbms_table_name, delta_hybrid_view_cols
                ):
                    if (
                        "type" in vector
                        and schema_sync_constants.PRESENT_TABLE not in vector["type"]
                    ) or "type" not in vector:
                        vector["type"] = schema_sync_constants.PRESENT_TABLE
                        vector["rdbms_owner"] = rdbms_owner
                        vector["rdbms_table_name"] = rdbms_table_name
                        vector["backend_owner"] = backend_owner
                        vector["backend_table_name"] = backend_table_name
                        vectors.append(vector)
                    log(
                        "%s.%s columns not aligned with %s.%s: %s"
                        % (
                            double_quote_sandwich(rdbms_owner),
                            double_quote_sandwich(rdbms_table_name),
                            double_quote_sandwich(hybrid_owner),
                            double_quote_sandwich(hybrid_view_name),
                            get_column_names(delta_hybrid_view_cols),
                        ),
                        vverbose,
                    )
                else:
                    log(
                        "%s.%s columns not aligned with %s.%s, but invalid columns detected"
                        % (
                            double_quote_sandwich(rdbms_owner),
                            double_quote_sandwich(rdbms_table_name),
                            double_quote_sandwich(hybrid_owner),
                            double_quote_sandwich(hybrid_view_name),
                        ),
                        vverbose,
                    )
            else:
                log(
                    "%s.%s columns aligned with %s.%s, skipping"
                    % (
                        double_quote_sandwich(rdbms_owner),
                        double_quote_sandwich(rdbms_table_name),
                        double_quote_sandwich(hybrid_owner),
                        double_quote_sandwich(hybrid_view_name),
                    ),
                    vverbose,
                )

        if vectors or exception:
            change["offloaded_owner"] = include["offloaded_owner"]
            change["offloaded_table"] = include["offloaded_table"]
            change["hadoop_owner"] = include["hadoop_owner"]
            change["hadoop_table"] = include["hadoop_table"]
            change["hybrid_owner"] = include["hybrid_owner"]
            change["hybrid_view"] = include["hybrid_view"]
            change["hybrid_external_table"] = include["hybrid_external_table"]
            if vectors:
                change["vectors"] = vectors
            if exception:
                change["compare_exception"] = exception

        log_change = copy.deepcopy(change)
        if "vectors" in log_change:
            log_vectors = log_change["vectors"]
            for log_vector in log_vectors:
                if "columns" in log_vector:
                    log_vector["columns"] = get_column_names(log_vector["columns"])
            log_change["vectors"] = log_vectors

        log("", vverbose)
        log("Change:", vverbose)
        log(dense.pformat(log_change), vverbose)
        return change
