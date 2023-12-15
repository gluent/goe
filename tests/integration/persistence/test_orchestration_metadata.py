""" TestOrchestrationMetadata: Unit test library to test orchestration metadata API for configured frontend.
"""

from unittest import TestCase, main

from goe.orchestration.execution_id import ExecutionId
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from goe.persistence.orchestration_metadata import (
    OrchestrationMetadata,
    METADATA_ATTRIBUTES,
)
from goe.offload.offload_constants import DBTYPE_ORACLE
from goe.orchestration import orchestration_constants

from tests.integration.test_functions import (
    build_current_options,
    get_default_test_user,
)
from tests.testlib.test_framework.test_functions import (
    get_test_messages,
)


UNITTEST_METADATA_NAME = "UNITTEST_METADATA_SALES"

METADATA_DICT_TEST_DEFAULTS = {
    "OFFLOAD_TYPE": "FULL",
    "HADOOP_OWNER": "SH_LONG_1234567890_NAME",
    "HADOOP_TABLE": "TABLE_LONG_1234567890_NAME",
    "OFFLOADED_OWNER": "SH_LONG_1234567890_NAME",
    "OFFLOADED_TABLE": "TABLE_LONG_1234567890_NAME",
    "INCREMENTAL_KEY": "COLUMN_LONG_1234567890_NAME",
    "INCREMENTAL_HIGH_VALUE": "LONG_1234567890_1234567890_VALUE",
    "INCREMENTAL_PREDICATE_TYPE": "RANGE",
    "INCREMENTAL_PREDICATE_VALUE": ["column(OBJECT_ID) < numeric(100)"],
    "OFFLOAD_BUCKET_COLUMN": "SOME_COLUMN",
    "OFFLOAD_VERSION": "6.0.0",
    "OFFLOAD_SORT_COLUMNS": "SOME_COLUMN1,SOME_COLUMN2",
    "INCREMENTAL_RANGE": "PARTITION",
    "OFFLOAD_PARTITION_FUNCTIONS": "UDF1,UDF2",
}

EXAMPLE_CHANNELS_METADATA_DICT = {
    "OFFLOAD_TYPE": "FULL",
    "HADOOP_OWNER": "SH",
    "HADOOP_TABLE": "CHANNELS",
    "OFFLOADED_OWNER": "SH",
    "OFFLOADED_TABLE": "CHANNELS",
    "INCREMENTAL_KEY": None,
    "INCREMENTAL_HIGH_VALUE": None,
    "INCREMENTAL_PREDICATE_TYPE": None,
    "INCREMENTAL_PREDICATE_VALUE": None,
    "OFFLOAD_BUCKET_COLUMN": None,
    "OFFLOAD_VERSION": "4.3.0",
    "OFFLOAD_SORT_COLUMNS": None,
    "INCREMENTAL_RANGE": None,
    "OFFLOAD_PARTITION_FUNCTIONS": None,
}

EXAMPLE_SALES_METADATA_DICT = {
    "OFFLOAD_TYPE": "INCREMENTAL",
    "HADOOP_OWNER": "SH",
    "HADOOP_TABLE": "SALES",
    "OFFLOADED_OWNER": "SH",
    "OFFLOADED_TABLE": "SALES",
    "OFFLOAD_SNAPSHOT": 76850168,
    "INCREMENTAL_KEY": "TIME_ID",
    "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2012-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
    "INCREMENTAL_PREDICATE_TYPE": "RANGE",
    "INCREMENTAL_PREDICATE_VALUE": None,
    "OFFLOAD_BUCKET_COLUMN": None,
    "OFFLOAD_VERSION": "4.3.0",
    "OFFLOAD_SORT_COLUMNS": None,
    "INCREMENTAL_RANGE": "PARTITION",
    "OFFLOAD_PARTITION_FUNCTIONS": None,
}

EXAMPLE_GL_LIST_RANGE_DAY_DT_METADATA_DICT = {
    "OFFLOAD_TYPE": "INCREMENTAL",
    "HADOOP_OWNER": "SH_TEST",
    "HADOOP_TABLE": "GL_LIST_RANGE_DAY_DT",
    "OFFLOADED_OWNER": "SH_TEST",
    "OFFLOADED_TABLE": "GL_LIST_RANGE_DAY_DT",
    "OFFLOAD_SNAPSHOT": 69472864,
    "INCREMENTAL_KEY": "DT",
    "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2015-01-31 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
    "INCREMENTAL_PREDICATE_TYPE": "RANGE",
    "INCREMENTAL_PREDICATE_VALUE": None,
    "OFFLOAD_BUCKET_COLUMN": None,
    "OFFLOAD_VERSION": "4.3.0",
    "OFFLOAD_SORT_COLUMNS": None,
    "INCREMENTAL_RANGE": "SUBPARTITION",
    "OFFLOAD_PARTITION_FUNCTIONS": None,
}

EXAMPLE_STORY_PBO_DIM_METADATA_DICT = {
    "OFFLOAD_TYPE": "INCREMENTAL",
    "HADOOP_OWNER": "SH_TEST",
    "HADOOP_TABLE": "STORY_PBO_DIM",
    "OFFLOADED_OWNER": "SH_TEST",
    "OFFLOADED_TABLE": "STORY_PBO_DIM",
    "OFFLOAD_SNAPSHOT": 76853651,
    "INCREMENTAL_KEY": None,
    "INCREMENTAL_HIGH_VALUE": None,
    "INCREMENTAL_PREDICATE_TYPE": "PREDICATE",
    "INCREMENTAL_PREDICATE_VALUE": ['column(PROD_SUBCATEGORY) = string("Camcorders")'],
    "OFFLOAD_BUCKET_COLUMN": None,
    "OFFLOAD_VERSION": "4.3.0",
    "OFFLOAD_SORT_COLUMNS": None,
    "INCREMENTAL_RANGE": None,
    "OFFLOAD_PARTITION_FUNCTIONS": None,
}

EXAMPLE_STORY_PBO_R_INTRA_METADATA_DICT = {
    "OFFLOAD_TYPE": "INCREMENTAL",
    "HADOOP_OWNER": "SH_TEST",
    "HADOOP_TABLE": "STORY_PBO_R_INTRA",
    "OFFLOADED_OWNER": "SH_TEST",
    "OFFLOADED_TABLE": "STORY_PBO_R_INTRA",
    "OFFLOAD_SNAPSHOT": 76856298,
    "INCREMENTAL_KEY": "TIME_ID",
    "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2012-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
    "INCREMENTAL_PREDICATE_TYPE": "RANGE_AND_PREDICATE",
    "INCREMENTAL_PREDICATE_VALUE": [
        "((column(TIME_ID) >= datetime(2012-02-01) AND column(TIME_ID) < datetime(2012-03-01)) AND column(CHANNEL_ID) = numeric(2))"
    ],
    "OFFLOAD_BUCKET_COLUMN": None,
    "OFFLOAD_VERSION": "4.3.0",
    "OFFLOAD_SORT_COLUMNS": None,
    "INCREMENTAL_RANGE": "PARTITION",
    "OFFLOAD_PARTITION_FUNCTIONS": None,
}


class TestOrchestrationMetadataException(Exception):
    pass


class TestOrchestrationMetadata(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = build_current_options()
        self.messages = get_test_messages(self.config, "TestOrchestrationMetadata")
        self.db = get_default_test_user()
        # See all unittest output in diff mismatches
        self.maxDiff = None

    def _convert_to_test_metadata(
        self,
        source_metadata,
        new_owner,
        new_name,
    ):
        """Generate some test data, using the metadata for SALES as a base and a description of the
        OFFLOAD_METADATA table to fill in the blanks...
        """
        # Modify the test data...
        test_metadata = source_metadata
        test_metadata["OFFLOADED_OWNER"] = new_owner
        test_metadata["OFFLOADED_TABLE"] = new_name
        for k, v in test_metadata.items():
            if v is None:
                # Fill in the blanks with dummy data.
                test_metadata[k] = METADATA_DICT_TEST_DEFAULTS[k]
        return test_metadata

    def _gen_test_metadata(self):
        source_metadata = OrchestrationMetadata(EXAMPLE_SALES_METADATA_DICT)
        new_metadata_dict = self._convert_to_test_metadata(
            source_metadata.as_dict(),
            self.db,
            UNITTEST_METADATA_NAME,
        )
        return OrchestrationMetadata(
            new_metadata_dict, connection_options=self.config, messages=self.messages
        )

    def _test_metadata(
        self, test_metadata, client=None, connection_options=None, messages=None
    ):
        """Tests we can get/change and delete OrchestrationMetadata"""
        execution_id = ExecutionId()
        repo_client = test_metadata.client
        cid = repo_client.start_command(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            "test",
            '{"test": 123}',
        )

        # Save initial metadata
        test_metadata.save(execution_id)

        # Retrieve the metadata again and confirm all attributes are the same as those we saved
        saved_metadata = OrchestrationMetadata.from_name(
            self.db,
            UNITTEST_METADATA_NAME,
            client=client,
            connection_options=connection_options,
            messages=messages,
        )
        self.assertIsInstance(saved_metadata, OrchestrationMetadata)
        self.assertEqual(
            test_metadata.as_dict(),
            saved_metadata.as_dict(),
            "Saved metadata mismatch when re-fetched",
        )

        # Test that the save function updates changes
        # Change by attribute:
        test_metadata.offload_snapshot = -1
        test_metadata.save(execution_id)
        saved_metadata = OrchestrationMetadata.from_name(
            self.db,
            UNITTEST_METADATA_NAME,
            client=client,
            connection_options=connection_options,
            messages=messages,
        )
        self.assertIsInstance(saved_metadata, OrchestrationMetadata)
        self.assertEqual(
            test_metadata.offload_snapshot,
            saved_metadata.offload_snapshot,
            "Saved metadata.offload_snapshot mismatch when re-fetched",
        )
        # Change by dict:
        test_metadata_dict = test_metadata.as_dict()
        test_metadata_dict["OFFLOAD_SNAPSHOT"] = -2
        test_metadata = OrchestrationMetadata(
            test_metadata_dict, client=test_metadata.client
        )
        test_metadata.save(execution_id)
        saved_metadata = OrchestrationMetadata.from_name(
            self.db,
            UNITTEST_METADATA_NAME,
            client=client,
            connection_options=connection_options,
            messages=messages,
        )
        self.assertIsInstance(saved_metadata, OrchestrationMetadata)
        self.assertEqual(
            test_metadata.offload_snapshot,
            saved_metadata.offload_snapshot,
            "Saved metadata.offload_snapshot mismatch when re-fetched",
        )

        # Drop metadata
        test_metadata.drop()
        saved_metadata = OrchestrationMetadata.from_name(
            self.db,
            UNITTEST_METADATA_NAME,
            client=client,
            connection_options=connection_options,
            messages=messages,
        )
        self.assertIsNone(saved_metadata, "Metadata was not deleted")

        repo_client.end_command(cid, orchestration_constants.COMMAND_SUCCESS)

    def test_metadata_by_client(self):
        """Tests we can interact with OrchestrationMetadata by client.
        This is more efficient as it will re-use a connection.
        """
        client = orchestration_repo_client_factory(self.config, self.messages)
        self._test_metadata(self._gen_test_metadata(), client=client)

    def test_metadata_direct(self):
        """Tests we can interact with OrchestrationMetadata without creating a client.
        Ensures each call is able to create its own RDBMS connection.
        """
        self._test_metadata(
            self._gen_test_metadata(),
            connection_options=self.config,
            messages=self.messages,
        )

    def test_generated_metadata(self):
        """Test that the metadata prepared with gen_offload_metadata goes through the standard test flow."""
        source_metadata = self._gen_test_metadata()
        source_metadata_dict = source_metadata.as_dict()
        # Generate a new metadata object using constructor attributes
        attributes = {
            k: source_metadata_dict[v] for k, v in METADATA_ATTRIBUTES.items()
        }
        new_metadata = OrchestrationMetadata.from_attributes(
            connection_options=self.config, messages=self.messages, **attributes
        )
        self.assertIsInstance(new_metadata, OrchestrationMetadata)
        self.assertEqual(
            source_metadata_dict, new_metadata.as_dict(), "Generated metadata mismatch"
        )
        self._test_metadata(
            new_metadata, connection_options=self.config, messages=self.messages
        )

    def test_is_subpartition_offload(self):
        channels_metadata = OrchestrationMetadata(EXAMPLE_CHANNELS_METADATA_DICT)
        self.assertIsInstance(channels_metadata.is_subpartition_offload(), bool)
        self.assertEqual(channels_metadata.is_subpartition_offload(), False)

        sales_metadata = OrchestrationMetadata(EXAMPLE_SALES_METADATA_DICT)
        self.assertIsInstance(sales_metadata.is_subpartition_offload(), bool)
        self.assertEqual(sales_metadata.is_subpartition_offload(), False)

        gl_list_range_day_dt_metadata = OrchestrationMetadata(
            EXAMPLE_GL_LIST_RANGE_DAY_DT_METADATA_DICT
        )
        self.assertIsInstance(
            gl_list_range_day_dt_metadata.is_subpartition_offload(), bool
        )
        self.assertEqual(gl_list_range_day_dt_metadata.is_subpartition_offload(), True)

    def test_is_hwm_in_hybrid_view(self):
        channels_metadata = OrchestrationMetadata(EXAMPLE_CHANNELS_METADATA_DICT)
        self.assertIsInstance(channels_metadata.is_hwm_in_hybrid_view(), bool)
        self.assertEqual(channels_metadata.is_hwm_in_hybrid_view(), False)

        sales_metadata = OrchestrationMetadata(EXAMPLE_SALES_METADATA_DICT)
        self.assertIsInstance(sales_metadata.is_hwm_in_hybrid_view(), bool)
        self.assertEqual(sales_metadata.is_hwm_in_hybrid_view(), True)

        pbo_metadata = OrchestrationMetadata(EXAMPLE_STORY_PBO_DIM_METADATA_DICT)
        self.assertIsInstance(pbo_metadata.is_hwm_in_hybrid_view(), bool)
        self.assertEqual(pbo_metadata.is_hwm_in_hybrid_view(), True)

    def test_incremental_data_append_feature(self):
        # Offload dimension
        channels_metadata = OrchestrationMetadata(EXAMPLE_CHANNELS_METADATA_DICT)
        self.assertIsInstance(channels_metadata.incremental_data_append_feature(), str)
        self.assertNotIn(
            "partition", channels_metadata.incremental_data_append_feature().lower()
        )
        self.assertNotIn(
            "sub", channels_metadata.incremental_data_append_feature().lower()
        )
        self.assertNotIn(
            "predicate", channels_metadata.incremental_data_append_feature().lower()
        )

        # Offload by partition
        sales_metadata = OrchestrationMetadata(EXAMPLE_SALES_METADATA_DICT)
        self.assertIsInstance(sales_metadata.incremental_data_append_feature(), str)
        self.assertIn(
            "partition", sales_metadata.incremental_data_append_feature().lower()
        )
        self.assertNotIn(
            "sub", sales_metadata.incremental_data_append_feature().lower()
        )
        self.assertNotIn(
            "predicate", sales_metadata.incremental_data_append_feature().lower()
        )

        # Offload by subpartition
        gl_list_range_day_dt_metadata = OrchestrationMetadata(
            EXAMPLE_GL_LIST_RANGE_DAY_DT_METADATA_DICT
        )
        self.assertIsInstance(
            gl_list_range_day_dt_metadata.incremental_data_append_feature(), str
        )
        self.assertIn(
            "partition",
            gl_list_range_day_dt_metadata.incremental_data_append_feature().lower(),
        )
        self.assertIn(
            "sub",
            gl_list_range_day_dt_metadata.incremental_data_append_feature().lower(),
        )
        self.assertNotIn(
            "predicate",
            gl_list_range_day_dt_metadata.incremental_data_append_feature().lower(),
        )

        # Offload by predicate
        pbo_metadata = OrchestrationMetadata(EXAMPLE_STORY_PBO_DIM_METADATA_DICT)
        self.assertIsInstance(pbo_metadata.incremental_data_append_feature(), str)
        self.assertNotIn(
            "partition", pbo_metadata.incremental_data_append_feature().lower()
        )
        self.assertNotIn("sub", pbo_metadata.incremental_data_append_feature().lower())
        self.assertIn(
            "predicate", pbo_metadata.incremental_data_append_feature().lower()
        )

        # Offload by predicate and partition
        intra_pbo_metadata = OrchestrationMetadata(
            EXAMPLE_STORY_PBO_R_INTRA_METADATA_DICT
        )
        self.assertIsInstance(intra_pbo_metadata.incremental_data_append_feature(), str)
        self.assertIn(
            "partition", intra_pbo_metadata.incremental_data_append_feature().lower()
        )
        self.assertNotIn(
            "sub", intra_pbo_metadata.incremental_data_append_feature().lower()
        )
        self.assertIn(
            "predicate", intra_pbo_metadata.incremental_data_append_feature().lower()
        )


if __name__ == "__main__":
    main()
