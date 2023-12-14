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
from goe.offload.factory.frontend_api_factory import frontend_api_factory
from goe.offload.offload_constants import DBTYPE_ORACLE
from goe.offload.offload_messages import OffloadMessages
from goe.orchestration import orchestration_constants

from tests.integration.test_functions import (
    build_current_options,
    get_default_test_user,
)


UNITTEST_METADATA_NAME = "UNITTEST_METADATA_SALES"

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
    "OFFLOAD_SCN": 76850168,
    "INCREMENTAL_KEY": "TIME_ID",
    "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2012-01-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
    "INCREMENTAL_PREDICATE_TYPE": "RANGE",
    "INCREMENTAL_PREDICATE_VALUE": None,
    "OFFLOAD_BUCKET_COLUMN": None,
    "OFFLOAD_VERSION": "4.3.0",
    "OFFLOAD_SORT_COLUMNS": None,
    "INCREMENTAL_RANGE": "PARTITION",
    "OFFLOAD_PARTITION_FUNCTIONS": "",
}

EXAMPLE_GL_LIST_RANGE_DAY_DT_METADATA_DICT = {
    "OFFLOAD_TYPE": "INCREMENTAL",
    "HADOOP_OWNER": "SH_TEST",
    "HADOOP_TABLE": "GL_LIST_RANGE_DAY_DT",
    "OFFLOADED_OWNER": "SH_TEST",
    "OFFLOADED_TABLE": "GL_LIST_RANGE_DAY_DT",
    "OFFLOAD_SCN": 69472864,
    "INCREMENTAL_KEY": "DT",
    "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2015-01-31 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
    "INCREMENTAL_PREDICATE_TYPE": "RANGE",
    "INCREMENTAL_PREDICATE_VALUE": None,
    "OFFLOAD_BUCKET_COLUMN": None,
    "OFFLOAD_VERSION": "4.3.0",
    "OFFLOAD_SORT_COLUMNS": None,
    "INCREMENTAL_RANGE": "SUBPARTITION",
    "OFFLOAD_PARTITION_FUNCTIONS": "",
}

EXAMPLE_STORY_PBO_DIM_METADATA_DICT = {
    "OBJECT_TYPE": "GLUENT_OFFLOAD_HYBRID_VIEW",
    "EXTERNAL_TABLE": "STORY_PBO_DIM_EXT",
    "HYBRID_OWNER": "SH_TEST_H",
    "HYBRID_VIEW": "STORY_PBO_DIM",
    "OFFLOAD_TYPE": "INCREMENTAL",
    "HADOOP_OWNER": "SH_TEST",
    "HADOOP_TABLE": "STORY_PBO_DIM",
    "OFFLOADED_OWNER": "SH_TEST",
    "OFFLOADED_TABLE": "STORY_PBO_DIM",
    "OFFLOAD_SCN": 76853651,
    "INCREMENTAL_KEY": None,
    "INCREMENTAL_HIGH_VALUE": None,
    "INCREMENTAL_PREDICATE_TYPE": "PREDICATE",
    "INCREMENTAL_PREDICATE_VALUE": ['column(PROD_SUBCATEGORY) = string("Camcorders")'],
    "OFFLOAD_BUCKET_COLUMN": None,
    "OFFLOAD_BUCKET_METHOD": None,
    "OFFLOAD_BUCKET_COUNT": None,
    "OBJECT_HASH": None,
    "OFFLOAD_VERSION": "4.3.0",
    "OFFLOAD_SORT_COLUMNS": None,
    "INCREMENTAL_RANGE": None,
    "TRANSFORMATIONS": None,
    "OFFLOAD_PARTITION_FUNCTIONS": None,
    "IU_KEY_COLUMNS": None,
    "IU_EXTRACTION_METHOD": None,
    "IU_EXTRACTION_SCN": None,
    "IU_EXTRACTION_TIME": None,
    "CHANGELOG_TABLE": None,
    "CHANGELOG_TRIGGER": None,
    "CHANGELOG_SEQUENCE": None,
    "UPDATABLE_VIEW": None,
    "UPDATABLE_TRIGGER": None,
}

EXAMPLE_STORY_PBO_R_INTRA_METADATA_DICT = {
    "OBJECT_TYPE": "GLUENT_OFFLOAD_HYBRID_VIEW",
    "EXTERNAL_TABLE": "STORY_PBO_R_INTRA_EXT",
    "HYBRID_OWNER": "SH_TEST_H",
    "HYBRID_VIEW": "STORY_PBO_R_INTRA",
    "OFFLOAD_TYPE": "INCREMENTAL",
    "HADOOP_OWNER": "SH_TEST",
    "HADOOP_TABLE": "STORY_PBO_R_INTRA",
    "OFFLOADED_OWNER": "SH_TEST",
    "OFFLOADED_TABLE": "STORY_PBO_R_INTRA",
    "OFFLOAD_SCN": 76856298,
    "INCREMENTAL_KEY": "TIME_ID",
    "INCREMENTAL_HIGH_VALUE": "TO_DATE(' 2012-02-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')",
    "INCREMENTAL_PREDICATE_TYPE": "RANGE_AND_PREDICATE",
    "INCREMENTAL_PREDICATE_VALUE": [
        "((column(TIME_ID) >= datetime(2012-02-01) AND column(TIME_ID) < datetime(2012-03-01)) AND column(CHANNEL_ID) = numeric(2))"
    ],
    "OFFLOAD_BUCKET_COLUMN": None,
    "OFFLOAD_BUCKET_METHOD": None,
    "OFFLOAD_BUCKET_COUNT": None,
    "OBJECT_HASH": None,
    "OFFLOAD_VERSION": "4.3.0",
    "OFFLOAD_SORT_COLUMNS": None,
    "INCREMENTAL_RANGE": "PARTITION",
    "TRANSFORMATIONS": None,
    "OFFLOAD_PARTITION_FUNCTIONS": None,
    "IU_KEY_COLUMNS": None,
    "IU_EXTRACTION_METHOD": None,
    "IU_EXTRACTION_SCN": None,
    "IU_EXTRACTION_TIME": None,
    "CHANGELOG_TABLE": None,
    "CHANGELOG_TRIGGER": None,
    "CHANGELOG_SEQUENCE": None,
    "UPDATABLE_VIEW": None,
    "UPDATABLE_TRIGGER": None,
}


class TestOrchestrationMetadataException(Exception):
    pass


class TestOrchestrationMetadata(TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.options = build_current_options()
        self.messages = OffloadMessages()
        self.db = get_default_test_user(hybrid=True)
        # See all unittest output in diff mismatches
        self.maxDiff = None

    def _convert_to_test_metadata(
        self, source_metadata, new_owner, new_name, metadata_columns
    ):
        """Generate some test data, using the metadata for SALES as a base and a description of the
        OFFLOAD_METADATA table to fill in the blanks...
        """
        # Modify the test data...
        test_metadata = source_metadata
        test_metadata["HYBRID_OWNER"] = new_owner
        test_metadata["HYBRID_VIEW"] = new_name
        test_metadata["EXTERNAL_TABLE"] = "%s_EXT" % new_name
        for k, v in test_metadata.items():
            if v is None:
                # Fill in the blanks with dummy data (column names for strings and name length for numbers)
                metadata_value = None
                metadata_column = metadata_columns[k]
                # The problem with dynamic data creation... we have to hack the ones that require special knowledge
                if k == "TRANSFORMATIONS":
                    metadata_value = None
                elif k == "INCREMENTAL_PREDICATE_VALUE":
                    metadata_value = ["column(OBJECT_ID) < numeric(100)"]
                elif k in ("IU_EXTRACTION_SCN", "IU_EXTRACTION_TIME"):
                    metadata_value = None
                elif metadata_column.is_string_based() and metadata_column.data_length:
                    metadata_value = k[: metadata_column.data_length]
                elif metadata_column.is_number_based():
                    metadata_value = len(k)
                else:
                    raise TestOrchestrationMetadataException(
                        f"Metadata unit tests do not cater for key: {k}"
                    )
                test_metadata[k] = metadata_value
        return test_metadata

    def _gen_test_metadata(self, metadata_client=None, source_table="SALES"):
        source_metadata = OrchestrationMetadata.from_name(
            self.db,
            source_table,
            client=metadata_client,
            connection_options=self.options,
            messages=self.messages,
        )
        self.assertIsInstance(source_metadata, OrchestrationMetadata)
        metadata_columns = self._get_offload_metadata_description()
        new_metadata_dict = self._convert_to_test_metadata(
            source_metadata.as_dict(), self.db, UNITTEST_METADATA_NAME, metadata_columns
        )
        return OrchestrationMetadata(
            new_metadata_dict, connection_options=self.options, messages=self.messages
        )

    def _get_offload_metadata_description(self):
        frontend_api = frontend_api_factory(
            self.options.db_type, self.options, self.messages, dry_run=True
        )
        if self.options.db_type == DBTYPE_ORACLE:
            return self._get_offload_metadata_description_from_oracle(frontend_api)
        else:
            raise Exception(
                "TestOrchestrationMetadata not implemented for system: {}".format(
                    self.options.db_type
                )
            )

    def _get_offload_metadata_description_from_oracle(self, frontend_api):
        """Fetch the description of the OFFLOAD_METADATA table to generate some
        test data...
        """
        repo_user = self.options.ora_repo_user.upper()
        metadata_columns = {}
        for column in frontend_api.get_columns(repo_user, "OFFLOAD_METADATA"):
            metadata_columns[column.name] = column
        return metadata_columns

    def _test_metadata(
        self, test_metadata, client=None, connection_options=None, messages=None
    ):
        """Tests we can get/change and delete OrchestrationMetadata"""
        execution_id = ExecutionId()
        repo_client = test_metadata.client
        # TODO log name is no longer passed in
        cid = repo_client.start_command(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            "unit_tests",
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
        test_metadata.offload_scn = -1
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
            test_metadata.offload_scn,
            saved_metadata.offload_scn,
            "Saved metadata.offload_scn mismatch when re-fetched",
        )
        # Change by dict:
        test_metadata_dict = test_metadata.as_dict()
        test_metadata_dict["OFFLOAD_SCN"] = -2
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
            test_metadata.offload_scn,
            saved_metadata.offload_scn,
            "Saved metadata.offload_scn mismatch when re-fetched",
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
        client = orchestration_repo_client_factory(self.options, self.messages)
        self._test_metadata(
            self._gen_test_metadata(metadata_client=client), client=client
        )

    def test_metadata_direct(self):
        """Tests we can interact with OrchestrationMetadata without creating a client.
        Ensures each call is able to create its own RDBMS connection.
        """
        self._test_metadata(
            self._gen_test_metadata(),
            connection_options=self.options,
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
            connection_options=self.options, messages=self.messages, **attributes
        )
        self.assertIsInstance(new_metadata, OrchestrationMetadata)
        self.assertEqual(
            source_metadata_dict, new_metadata.as_dict(), "Generated metadata mismatch"
        )
        self._test_metadata(
            new_metadata, connection_options=self.options, messages=self.messages
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
