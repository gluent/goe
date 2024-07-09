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
from unittest import TestCase, main
import uuid

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


if __name__ == "__main__":
    main()
