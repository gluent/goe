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

from unittest import TestCase, main

import json

from goe.conductor.hybrid_view_service import (
    HybridViewService,
    JSON_KEY_BACKEND_NUM_ROWS,
    JSON_KEY_BACKEND_SIZE,
    JSON_KEY_BACKEND_PARTITIONS,
    JSON_KEY_VALIDATE_STATUS,
    JSON_KEY_VALIDATE_MESSAGE,
)
from goe.offload.offload_constants import DBTYPE_ORACLE, DBTYPE_TERADATA
from tests.offload.unittest_functions import (
    build_current_options,
    get_default_test_user,
)


class TestHybridViewService(TestCase):
    def setUp(self):
        self.hybrid_schema = get_default_test_user(hybrid=True)
        self.orchestration_options = build_current_options()
        self.hybrid_view = "SALES"
        self.partition_name = "SALES_201102"
        self.hybrid_view_dim = "CHANNELS"

    def _get_db_type_hvs(self):
        if self.orchestration_options.db_type == DBTYPE_ORACLE:
            hv1 = "TO_DATE(' 2011-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
            hv2 = "TO_DATE(' 2011-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
        elif self.orchestration_options.db_type == DBTYPE_TERADATA:
            hv1 = "DATE '2011-03-01'"
            hv2 = "DATE '2011-04-01'"
        else:
            raise NotImplementedError(
                "Frontend system not implemented for TestHybridViewService: {}".format(
                    self.orchestration_options.db_type
                )
            )
        return hv1, hv2

    def test_backend_attributes(self):
        conductor_api = HybridViewService(self.hybrid_schema, self.hybrid_view)

        json_payload = conductor_api.backend_attributes()
        self.assertIsInstance(json_payload, str)
        dict_payload = json.loads(json_payload)
        self.assertIsInstance(dict_payload[JSON_KEY_BACKEND_NUM_ROWS], (int, int))
        self.assertTrue(bool(dict_payload[JSON_KEY_BACKEND_NUM_ROWS] > 0))
        self.assertIsInstance(dict_payload[JSON_KEY_BACKEND_SIZE], (int, int, float))
        self.assertTrue(bool(dict_payload[JSON_KEY_BACKEND_SIZE] > 0))

        # SALES is partitioned so we expect details if the backend supports partitioning
        if conductor_api.backend_partition_by_column_supported:
            self.assertIsInstance(dict_payload[JSON_KEY_BACKEND_PARTITIONS], dict)
            self.assertTrue(bool(dict_payload[JSON_KEY_BACKEND_PARTITIONS]))
            part_id, part = dict_payload[JSON_KEY_BACKEND_PARTITIONS].popitem()
            self.assertIsInstance(part, dict)

    def test_validate_by_aggregation(self):
        hv1, hv2 = self._get_db_type_hvs()
        conductor_api = HybridViewService(self.hybrid_schema, self.hybrid_view)
        # No lower bound
        json_payload = conductor_api.validate_by_aggregation(upper_hv=hv2)
        dict_payload = json.loads(json_payload)
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_STATUS], bool)
        self.assertTrue(dict_payload[JSON_KEY_VALIDATE_STATUS])
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_MESSAGE], str)
        # Upper and lower bounds
        json_payload = conductor_api.validate_by_aggregation(upper_hv=hv2, lower_hv=hv1)
        dict_payload = json.loads(json_payload)
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_STATUS], bool)
        self.assertTrue(dict_payload[JSON_KEY_VALIDATE_STATUS])
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_MESSAGE], str)

        conductor_api = HybridViewService(self.hybrid_schema, self.hybrid_view_dim)
        # Unbound
        json_payload = conductor_api.validate_by_aggregation()
        dict_payload = json.loads(json_payload)
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_STATUS], bool)
        self.assertTrue(dict_payload[JSON_KEY_VALIDATE_STATUS])
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_MESSAGE], str)

    def test_validate_by_count(self):
        hv1, hv2 = self._get_db_type_hvs()
        conductor_api = HybridViewService(self.hybrid_schema, self.hybrid_view)
        # No lower bound
        json_payload = conductor_api.validate_by_count(upper_hv=hv2)
        dict_payload = json.loads(json_payload)
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_STATUS], bool)
        self.assertTrue(dict_payload[JSON_KEY_VALIDATE_STATUS])
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_MESSAGE], str)
        # Upper and lower bounds
        json_payload = conductor_api.validate_by_count(upper_hv=hv2, lower_hv=hv1)
        dict_payload = json.loads(json_payload)
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_STATUS], bool)
        self.assertTrue(dict_payload[JSON_KEY_VALIDATE_STATUS])
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_MESSAGE], (str, str))

        conductor_api = HybridViewService(self.hybrid_schema, self.hybrid_view_dim)
        # Unbound
        json_payload = conductor_api.validate_by_count()
        dict_payload = json.loads(json_payload)
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_STATUS], bool)
        self.assertTrue(dict_payload[JSON_KEY_VALIDATE_STATUS])
        self.assertIsInstance(dict_payload[JSON_KEY_VALIDATE_MESSAGE], (str, str))


if __name__ == "__main__":
    main()
