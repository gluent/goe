""" Unit tests for offload metadata functions.
"""

import os
from unittest import TestCase, main
from tests.unit.offload.unittest_functions import get_default_test_user, build_offload_operation
from tests.integration.test_functions import build_current_options
from goe.offload.offload_messages import OffloadMessages
from goe.offload.offload_metadata_functions import gen_offload_metadata_from_base
from goe.offload.predicate_offload import GenericPredicate
from goe.offload.oracle.oracle_column import OracleColumn
from goe.persistence.orchestration_metadata import OrchestrationMetadata
from goe.util.ora_query import get_oracle_connection


class TestOffloadMetadataFunctions(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestOffloadMetadataFunctions, self).__init__(*args, **kwargs)
        self.ut_app_owner = 'UNIT_TEST'
        self.ut_hybrid_owner = '%s_H' % self.ut_app_owner
        self.ut_hybrid_view = 'OFFLOAD_METADATA_FUNCTIONS'
        self.options = build_current_options()
        self.messages = OffloadMessages()
        self.maxDiff = None

    def _get_oracle_connection(self):
        """ Setup an Oracle connection
        """
        conn = get_oracle_connection(self.options.ora_adm_user, self.options.ora_adm_pass,
                                     self.options.rdbms_dsn, self.options.use_oracle_wallet)
        return conn

    def _get_offload_metadata_description(self):
        """ Fetch the description of the OFFLOAD_METADATA table to generate some
            test data...
        """
        metadata_columns = {}
        repo_user = os.environ.get('ORA_REPO_USER').upper()

        db_conn = self._get_oracle_connection()
        db_curs = db_conn.cursor()

        try:
            sql = """SELECT column_name, data_type, data_length
                     FROM   dba_tab_columns
                     WHERE  owner = :owner
                     AND    table_name = 'OFFLOAD_METADATA'
                     ORDER  BY column_id"""

            rows = db_curs.execute(sql, {'owner': repo_user}).fetchall()

            for name, data_type, length in rows:
                metadata_columns[name] = (data_type, length)
            return metadata_columns

        except Exception as exc:
            print('Unable to fetch OFFLOAD_METADATA description')
            raise

        finally:
            db_curs.close()
            db_conn.close()

    def _get_test_metadata(self):
        """ Generate some test data, using the metadata for SALES as a base and a description of the
            OFFLOAD_METADATA table to fill in the blanks...
        """

        # Seed the OFFLOAD_METADATA description used to generate test data...
        metadata_columns = self._get_offload_metadata_description()
        self.assertIsInstance(metadata_columns, dict)
        self.assertGreaterEqual(len(metadata_columns), 1)

        # Get source metadata from a table guaranteed to be available...
        source_hybrid_owner = get_default_test_user(hybrid=True)
        source_hybrid_view = 'SALES'
        source_metadata_obj = OrchestrationMetadata.from_name(source_hybrid_owner, source_hybrid_view,
                                                              connection_options=self.options, messages=self.messages)
        source_metadata = source_metadata_obj.as_dict()
        self.assertIsInstance(source_metadata, dict)
        self.assertEqual(source_metadata['HYBRID_OWNER'], source_hybrid_owner,
                         'Source metadata mismatch (%s != %s)' % (source_metadata['HYBRID_OWNER'], source_hybrid_owner))
        self.assertEqual(source_metadata['HYBRID_VIEW'], source_hybrid_view,
                         'Source metadata mismatch (%s != %s)' % (source_metadata['HYBRID_VIEW'], source_hybrid_view))

        # Modify the test data...
        test_metadata = source_metadata
        test_metadata['HYBRID_OWNER'] = self.ut_hybrid_owner
        test_metadata['HYBRID_VIEW'] = self.ut_hybrid_view
        test_metadata['EXTERNAL_TABLE'] = '%s_EXT' % self.ut_hybrid_view
        for k, v in test_metadata.items():
            if v is None:
                # Fill in the blanks with dummy data (column names for strings and name length for numbers)...
                metadata_value = None
                col_type, col_length = metadata_columns[k]
                # Here's the problem with dynamic data creation. We have to hack the ones that require special knowledge...
                if k == 'TRANSFORMATIONS':
                    metadata_value = None
                elif k == 'INCREMENTAL_PREDICATE_VALUE':
                    metadata_value = ['column(OBJECT_ID) < numeric(100)']
                elif k in ('IU_EXTRACTION_SCN', 'IU_EXTRACTION_TIME'):
                    metadata_value = None
                elif col_type in ['CHAR', 'VARCHAR2', 'CLOB']:
                    metadata_value = k[:col_length]
                elif col_type == 'NUMBER':
                    metadata_value = len(k)
                else:
                    print('Unit tests do not cater for data type %s. Update test_offload_metadata_functions.py' % k)
                    raise
                test_metadata[k] = metadata_value

        return test_metadata

    def test_gen_offload_metadata_from_base(self):
        """ Test that the generation of offload metadata from base metadata goes through the workflow...
        """
        base_metadata = self._get_test_metadata()
        self.assertIsNotNone(base_metadata, 'Cannot fetch base metadata')

        # Some setup needed for the base metadata API...
        hybrid_operation = build_offload_operation(options=self.options, messages=self.messages)
        hybrid_operation.hybrid_owner = base_metadata['HYBRID_OWNER']
        hybrid_operation.hybrid_name = base_metadata['HYBRID_VIEW']
        hybrid_operation.ext_table_name = base_metadata['EXTERNAL_TABLE']
        hybrid_operation.offload_type = base_metadata['OFFLOAD_TYPE']
        hybrid_operation.num_buckets = base_metadata['OFFLOAD_BUCKET_COUNT']
        hybrid_operation.ipa_predicate_type = base_metadata['INCREMENTAL_PREDICATE_TYPE']
        hybrid_operation.offload_by_subpartition = False
        hybrid_operation.bucket_hash_col = base_metadata['OFFLOAD_BUCKET_COLUMN']
        hybrid_operation.bucket_hash_method = base_metadata['OFFLOAD_BUCKET_METHOD']
        hybrid_operation.column_transformations = []
        hybrid_operation.hwm_in_hybrid_view = base_metadata['INCREMENTAL_HIGH_VALUE'] is not None
        hybrid_operation.sort_columns = [base_metadata['OFFLOAD_SORT_COLUMNS']]
        hybrid_operation.iu_key_columns = [base_metadata['IU_KEY_COLUMNS']]
        hybrid_operation.iu_extraction_method = base_metadata['IU_EXTRACTION_METHOD']
        hybrid_operation.iu_extraction_scn = base_metadata['IU_EXTRACTION_SCN']
        hybrid_operation.iu_extraction_time = base_metadata['IU_EXTRACTION_TIME']
        hybrid_operation.changelog_table = base_metadata['CHANGELOG_TABLE']
        hybrid_operation.changelog_trigger = base_metadata['CHANGELOG_TRIGGER']
        hybrid_operation.changelog_sequence = base_metadata['CHANGELOG_SEQUENCE']
        hybrid_operation.updatable_view = base_metadata['UPDATABLE_VIEW']
        hybrid_operation.updatable_trigger = base_metadata['UPDATABLE_TRIGGER']

        generated_metadata = gen_offload_metadata_from_base(
            repo_client=hybrid_operation.repo_client,
            hybrid_operation=hybrid_operation,
            threshold_cols=[OracleColumn(base_metadata['INCREMENTAL_KEY'], 'DATE')],
            offload_high_values=[base_metadata['INCREMENTAL_HIGH_VALUE']],
            incremental_predicate_values=[GenericPredicate(base_metadata['INCREMENTAL_PREDICATE_VALUE'][0])],
            object_type=base_metadata['OBJECT_TYPE'],
            backend_owner=base_metadata['HADOOP_OWNER'],
            backend_table_name=base_metadata['HADOOP_TABLE'],
            base_metadata=base_metadata,
            object_hash=base_metadata['OBJECT_HASH']
        )
        self.assertEqual(base_metadata, generated_metadata.as_dict(), 'Mismatch in metadata generated from base')


if __name__ == '__main__':
    main()
