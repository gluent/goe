# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
TestOrchestrationRepoClient: Unit test Orchestration Repo client library.
Excludes metadata specific methods, they are tested in TestOrchestrationMetadata.
"""

import datetime
import decimal
from unittest import TestCase, main, mock
import uuid

import oracledb

from goe.offload.offload_source_data import OffloadSourcePartition
from goe.persistence.oracle.oracle_orchestration_repo_client import (
    OracleOrchestrationRepoClient,
)
from goe.persistence.orchestration_metadata import (
    COMMAND_EXECUTION,
    HADOOP_OWNER,
    HADOOP_TABLE,
    INCREMENTAL_HIGH_VALUE,
    INCREMENTAL_KEY,
    INCREMENTAL_PREDICATE_TYPE,
    INCREMENTAL_PREDICATE_VALUE,
    INCREMENTAL_RANGE,
    OFFLOAD_BUCKET_COLUMN,
    OFFLOAD_PARTITION_FUNCTIONS,
    OFFLOAD_SNAPSHOT,
    OFFLOAD_SORT_COLUMNS,
    OFFLOAD_TYPE,
    OFFLOADED_OWNER,
    OFFLOADED_TABLE,
)
from goe.persistence.orchestration_repo_client import type_safe_json_dumps
from goe.offload.predicate_offload import GenericPredicate
from goe.orchestration.execution_id import ExecutionId

GB = 1024**3


class TestOrchestrationRepoClient(TestCase):
    """
    TestOrchestrationRepoClient: Unit test Orchestration Repo client library.
    Excludes metadata specific methods, they are tested in TestOrchestrationMetadata.
    """

    def test_orchestration_execution_id(self):
        i = ExecutionId()
        self.assertIsInstance(i.id, uuid.UUID)
        self.assertIsInstance(i.as_str(), str)
        self.assertIsInstance(i.as_bytes(), bytes)
        self.assertIsInstance(str(i), str)
        self.assertIsInstance(bytes(i), bytes)

        i2 = ExecutionId.from_str(str(i))
        self.assertEqual(i, i2)

        i2 = ExecutionId.from_bytes(bytes(i))
        self.assertEqual(i, i2)

        i2 = ExecutionId.from_uuid(i.id)
        self.assertEqual(i, i2)

    def test_orchestration_execution_id_from_uuid_invalid(self):
        with self.assertRaises(AssertionError):
            ExecutionId.from_uuid("not-a-uuid")
        with self.assertRaises(AssertionError):
            ExecutionId(from_uuid=12345)

    def test_type_safe_json_dumps(self):
        """Ensure we can serialize any types we might find in an options object to JSON"""
        option_dict = {
            "owner_table": "acme.unit_test_table",
            "execute": True,
            "skip": ["step_to_skip"],
            "data_sample_pct": decimal.Decimal(100),
            "older_than_date": datetime.datetime(2011, 4, 1, 0, 0),
            "verify_parallelism": None,
            "offload_predicate": GenericPredicate("column(col1) = numeric(123)"),
        }
        self.assertIsInstance(type_safe_json_dumps(option_dict), str)

    def test_oracle_udt_clob_attributes_and_partition_ntt(self):
        """Ensure CLOB attributes on Oracle UDTs use createlob() and each partition gets a distinct UDT instance."""
        client = OracleOrchestrationRepoClient.__new__(OracleOrchestrationRepoClient)
        client._repo_user = "GOE_REPO"
        mock_conn = mock.MagicMock()
        mock_conn.createlob.side_effect = lambda db_type, val: ("LOB", db_type, val)
        mock_frontend_api = mock.MagicMock()
        mock_frontend_api.get_oracle_connection_object.return_value = mock_conn
        mock_frontend_api.oracle_get_type_object.side_effect = (
            lambda _: mock.MagicMock()
        )
        client._frontend_client = mock_frontend_api

        metadata_dict = {
            OFFLOADED_OWNER: "SH",
            OFFLOADED_TABLE: "SALES",
            HADOOP_OWNER: "sh",
            HADOOP_TABLE: "sales",
            OFFLOAD_TYPE: "INCREMENTAL",
            INCREMENTAL_RANGE: "PARTITION",
            INCREMENTAL_KEY: "TIME_ID",
            INCREMENTAL_HIGH_VALUE: "TO_DATE('2020-01-01')",
            INCREMENTAL_PREDICATE_TYPE: "RANGE",
            INCREMENTAL_PREDICATE_VALUE: ["column(col1) = numeric(123)"],
            OFFLOAD_SNAPSHOT: None,
            OFFLOAD_BUCKET_COLUMN: None,
            OFFLOAD_SORT_COLUMNS: None,
            OFFLOAD_PARTITION_FUNCTIONS: None,
            COMMAND_EXECUTION: ExecutionId(),
        }
        ora_obj = client._metadata_dict_to_ora_object(metadata_dict)
        self.assertEqual(
            ora_obj.OFFLOAD_HIGH_VALUE,
            ("LOB", oracledb.DB_TYPE_CLOB, "TO_DATE('2020-01-01')"),
        )
        self.assertEqual(ora_obj.OFFLOAD_PREDICATE_VALUE[0], "LOB")
        self.assertEqual(ora_obj.OFFLOAD_PREDICATE_VALUE[1], oracledb.DB_TYPE_CLOB)

        partitions = [
            OffloadSourcePartition(
                "P1", "2020-01-01", (), "2020-01-01", 10, 100, None, None
            ),
            OffloadSourcePartition(
                "P2", "2020-02-01", (), "2020-02-01", 20, 200, None, None
            ),
        ]
        ntt = client._offload_partitions_to_ora_object(partitions, 1, "SH", "SALES")
        self.assertEqual(ntt.append.call_count, 2)
        p1_obj = ntt.append.call_args_list[0].args[0]
        p2_obj = ntt.append.call_args_list[1].args[0]
        self.assertIsNot(p1_obj, p2_obj)
        self.assertEqual(p1_obj.PARTITION_NAME, "P1")
        self.assertEqual(p2_obj.PARTITION_NAME, "P2")
        self.assertEqual(
            p1_obj.PARTITION_BOUNDARY,
            ("LOB", oracledb.DB_TYPE_CLOB, "2020-01-01"),
        )
        self.assertEqual(
            p2_obj.PARTITION_BOUNDARY,
            ("LOB", oracledb.DB_TYPE_CLOB, "2020-02-01"),
        )

    def test_oracle_str_to_clob_empty_is_null(self):
        """Ensure None and empty string map to NULL rather than an empty CLOB."""
        client = OracleOrchestrationRepoClient.__new__(OracleOrchestrationRepoClient)
        mock_conn = mock.MagicMock()
        mock_frontend_api = mock.MagicMock()
        mock_frontend_api.get_oracle_connection_object.return_value = mock_conn
        client._frontend_client = mock_frontend_api

        self.assertIsNone(client._str_to_clob(None))
        self.assertIsNone(client._str_to_clob(""))
        mock_conn.createlob.assert_not_called()


if __name__ == "__main__":
    main()
