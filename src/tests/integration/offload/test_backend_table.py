""" TestBackendTable: Unit test library to test table level API for the configured backend.
    Because there are so few table level methods that do not need a database we do not
    skim all backends like we do for BackendApi testing.
    A good number of methods are not unit tested because they need detailed inputs, such
    as RDBMS columns, cast information, staging file details. For these we continue to
    rely on integration tests.
"""
from datetime import datetime
import decimal
import logging
from unittest import TestCase, main

from gluent import OffloadOperation
from goe.offload.factory.backend_table_factory import backend_table_factory
from goe.offload.column_metadata import CanonicalColumn, ColumnMetadataInterface, \
    get_partition_columns,\
    GLUENT_TYPE_DECIMAL, GLUENT_TYPE_INTEGER_4, GLUENT_TYPE_INTEGER_8
from goe.offload.offload_constants import DBTYPE_IMPALA
from goe.offload.offload_messages import OffloadMessages
from goe.offload.synthetic_partition_literal import SyntheticPartitionLiteral
from goe.orchestration import orchestration_constants
from goe.orchestration.execution_id import ExecutionId
from goe.persistence.orchestration_metadata import OrchestrationMetadata
from tests.testlib.test_framework.factory.backend_testing_api_factory import backend_testing_api_factory
from tests.unit.offload.unittest_functions import build_current_options, get_default_test_user


# logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


SALES = 'SALES'


def partition_key_test_numbers(low_digits, high_digits, wiggle_room=3, negative=False):
    nums = []
    for digits in range(low_digits, high_digits + 1):
        boundary = 10 ** digits
        for num in range(boundary - wiggle_room, min(boundary + wiggle_room + 1, 10 ** high_digits)):
            nums.append(-num if negative else num)
    return nums


class TestBackendTable(TestCase):

    def __init__(self, *args, **kwargs):
        super(TestBackendTable, self).__init__(*args, **kwargs)
        self.api = None
        self.test_api = None
        self.options = None
        self.db = None
        self.table = None

    def setUp(self):
        execution_id = ExecutionId()
        messages = OffloadMessages(execution_id=execution_id, command_type=orchestration_constants.COMMAND_OFFLOAD)
        self.test_api = backend_testing_api_factory(self.options.target, self.options, messages, dry_run=True)
        self.db, rdbms_owner, self.table = self._get_real_db_table()
        operation = OffloadOperation.from_dict({'owner_table': '%s.%s' % (rdbms_owner or self.db, self.table)},
                                               self.options, messages, execution_id=execution_id)
        self.api = backend_table_factory(self.db, self.table, self.options.target, self.options, messages,
                                         orchestration_operation=operation, dry_run=True)
        if rdbms_owner:
            operation.set_bucket_info_from_metadata(operation.get_hybrid_metadata(self.options), messages)
            self.api.refresh_operational_settings(operation)

    def _compare_sql_and_python_synthetic_part_number_outcomes(self, num, num_column, granularity, padding_digits):
        logger.info(f'Testing synthetic expressions. Input/Granularity/Digits: {num}/{granularity}/{padding_digits}')
        num_staging_cast = 'CAST({} AS {})'.format(num, num_column.format_data_type())
        cast = self.api._gen_synthetic_part_number_sql_expr(num_staging_cast, num_column, granularity, padding_digits)
        logger.info(f'Synthetic CAST expression: {cast}')
        row = None
        try:
            row = self._run_call_sql_expression_from_sql(cast,
                                                         query_options=self.api._cast_verification_query_options())
        except Exception as exc:
            self.assertTrue(bool(row),
                            'Exception casting input number: ({}) {}\nCast: {}\nException: {}'.format(type(num), num,
                                                                                                      cast, str(exc)))
        sql_value = row[0]
        logger.info(f'SQL CAST output: {sql_value}')
        if isinstance(sql_value, str):
            python_value = SyntheticPartitionLiteral._gen_number_string_literal(num, granularity, padding_digits)
            logger.info(f'Orchestration output: {python_value}')
            self.assertEqual(str(sql_value), str(python_value),
                             'String cast for input number: ({}) {}\nCast: {}'.format(type(num), num, cast))
        else:
            python_value = SyntheticPartitionLiteral._gen_number_integral_literal(num, granularity)
            logger.info(f'Orchestration output: {python_value}')
            self.assertEqual(sql_value, int(python_value),
                             'Numeric cast for input number: ({}) {}\nCast: {}'.format(type(num), num, cast))

    def _get_real_db_table(self):
        # Try to find an SH_TEST.SALES table
        messages = OffloadMessages()
        hybrid_schema = get_default_test_user(hybrid=True)
        for hybrid_schema, hybrid_view in [(hybrid_schema, SALES), ('SH_TEST_H', SALES), ('SH_H', SALES)]:
            metadata = OrchestrationMetadata.from_name(hybrid_schema, hybrid_view, connection_options=self.options,
                                                       messages=messages)
            if metadata:
                return (metadata.backend_owner, metadata.offloaded_owner, metadata.backend_table)
        # We shouldn't get to here in a correctly configured environment
        raise Exception('SALES test table is missing, please configure your environment')

    def _run_call_sql_expression_from_sql(self, sql_expression, db=None, table=None, base_column=None,
                                          query_options=None):
        """ Test that a SQL expression is valid SQL for the backend by running a quick SQL query.
            Also selects the raw column value that went into the SQL expression for verification.
        """
        if db and table:
            where_clause = ' WHERE {} IS NOT NULL'.format(base_column) if base_column else ''
            sql = 'SELECT %s, %s FROM %s%s LIMIT 1' \
                % (sql_expression, self.test_api.enclose_identifier(base_column),
                   self.test_api.enclose_object_reference(self.db, self.table), where_clause)
        else:
            sql = 'SELECT %s' % sql_expression
        return self.test_api.execute_query_fetch_one(sql, query_options=query_options)

    def _test_alter_table_sort_columns(self):
        if self.api.sorted_table_modify_supported():
            self.assertIsInstance(self.api.alter_table_sort_columns(), list)

    def _test_cleanup_staging_area(self):
        self.api.cleanup_staging_area()

    def _test_compute_final_table_stats(self):
        if self.api.table_stats_compute_supported():
            self.api.compute_final_table_stats(True)

    def _test_create_backend_table(self):
        self.api.set_columns(self.api.get_columns())
        self.assertIsInstance(self.api.create_backend_table(), list)

    def _test_create_db(self):
        try:
            self.assertIsInstance(self.api.create_db(), list)
        except NotImplementedError:
            pass

    def _test_derive_unicode_string_columns(self):
        self.assertIsInstance(self.api.derive_unicode_string_columns(as_csv=False), list)
        self.assertIsInstance(self.api.derive_unicode_string_columns(as_csv=True), str)

    def _test__derive_partition_info(self):
        part_cols = get_partition_columns(self.api.get_partition_columns(), exclude_bucket_column=True)
        if part_cols:
            self.api._derive_partition_info(part_cols[0], partition_columns=part_cols)
            self.api._derive_partition_info(part_cols[0].name, partition_columns=part_cols)

    def _test__gen_synthetic_literal_function(self):
        """ Double underscore because we are unit testing a private method """
        if self.api.synthetic_partitioning_supported():
            part_cols = get_partition_columns(self.api.get_partition_columns(), exclude_bucket_column=True)
            if part_cols:
                part_col = part_cols[0]
                literal_fn = self.api._gen_synthetic_literal_function(part_col)
                self.assertIsNotNone(literal_fn)
                source_column = self.api.get_column(part_col.partition_info.source_column_name)
                source_value = datetime.now() if source_column.is_date_based() else 123
                self.assertIsNotNone(literal_fn(source_value))

    def _test__gen_synthetic_part_date_as_string_sql_expr(self):
        """ Double underscore because we are unit testing a private method """
        if self.api.synthetic_partitioning_supported():
            date_columns = [_ for _ in self.api.get_columns() if _.is_date_based()]
            if date_columns:
                for granularity, expected_length in [('Y', 4), ('M', 7), ('D', 10)]:
                    # Cast as STRING synthetic expression
                    cast = self.api._gen_synthetic_part_date_as_string_sql_expr(date_columns[0].name, granularity,
                                                                                source_column_cast=date_columns[0].name)
                    row = self._run_call_sql_expression_from_sql(cast, self.db, self.table, date_columns[0].name)
                    self.assertIsNotNone(row)
                    self.assertEqual(len(row[0]), expected_length)
                    # Check that the value from SQL matches output of our Python logic
                    verification_value = SyntheticPartitionLiteral._gen_date_as_string_literal(row[1], granularity)
                    self.assertEqual(row[0], verification_value)

    def _test__gen_synthetic_part_number_granularity_sql_expr(self):
        """ Ensure that decimal places are floored and not rounded """
        def check_num_values_truncated(col_input, granularity, digits, expected_str_outcome, expected_int_outcome):
            num_column = self.api.gen_default_numeric_column('A_COLUMN')
            cast = self.api._gen_synthetic_part_number_sql_expr(col_input, num_column, granularity, digits)
            row = self._run_call_sql_expression_from_sql(cast, query_options=self.api._cast_verification_query_options())
            test_value = row[0]
            if isinstance(test_value, str):
                self.assertEqual(str(test_value), expected_str_outcome)
            else:
                self.assertEqual(test_value, expected_int_outcome)
        if self.api.synthetic_partitioning_supported():
            check_num_values_truncated('125.678', 1, 5, '00125', 125)
            check_num_values_truncated('-125.678', 1, 5, '0-126', -126)
            check_num_values_truncated('129.678', 10, 5, '00120', 120)
            check_num_values_truncated('-129.678', 10, 5, '0-130', -130)

    def _test__gen_synthetic_part_number_sql_expr(self):
        """ Ensure correct number of trailing zeros after granularity expression.
            Double underscore because we are unit testing a private method.
        """
        if not self.api.synthetic_partitioning_supported():
            return

        num_columns = [_ for _ in self.api.get_columns() if _.is_number_based()]
        if not num_columns:
            return
        num_column = num_columns[0]
        for granularity, digits, expected_trailing_zeros in [
            (10, 10, 1),
            (1000, 10, 3),
        ]:
            if not self.api.synthetic_partition_numbers_are_string():
                digits = None
            cast = self.api._gen_synthetic_part_number_sql_expr(num_column.name, num_column, granularity, digits)
            row = self._run_call_sql_expression_from_sql(cast, self.db, self.table, num_column.name)
            self.assertIsNotNone(row)
            synthetic_value = row[0]
            raw_column_value = row[1]
            if self.api.synthetic_partition_numbers_are_string():
                self.assertEqual(len(synthetic_value), digits)
                self.assertEqual(synthetic_value[-expected_trailing_zeros:], '0' * expected_trailing_zeros)
                # Check that the value from SQL matches output of our Python logic
                python_value = SyntheticPartitionLiteral._gen_number_string_literal(raw_column_value,
                                                                                    granularity, digits)
            else:
                synthetic_value = str(synthetic_value)
                # Use min() below because number < granularity won't have enough trailing zeros
                self.assertEqual(synthetic_value[-expected_trailing_zeros:],
                                 '0' * min(expected_trailing_zeros, len(synthetic_value)))
                # Check that the value from SQL matches output of our Python logic
                python_value = SyntheticPartitionLiteral._gen_number_integral_literal(raw_column_value, granularity)
            self.assertEqual(row[0], python_value)

    def _test__gen_synthetic_part_string_sql_expr(self):
        """ Double underscore because we are unit testing a private method """
        if self.api.synthetic_partitioning_supported():
            str_columns = [_ for _ in self.api.get_columns() if _.is_string_based()]
            if str_columns:
                str_column = str_columns[0]
                for granularity in [1, 3]:
                    cast = self.api._gen_synthetic_part_string_sql_expr(str_column.name, granularity)
                    row = self._run_call_sql_expression_from_sql(cast, self.db, self.table, str_column.name)
                    self.assertIsNotNone(row)
                    self.assertEqual(len(row[0]), granularity)
                    # Check that the value from SQL matches output of our Python logic
                    verification_value = SyntheticPartitionLiteral._gen_string_literal(row[1], granularity)
                    self.assertEqual(row[0], verification_value)

    def _test__staging_to_backend_cast(self):
        for col in self.api.get_columns():
            # This is a bit of a cheat because we pass in the backend column for both front and backend.
            # It's close enough to give the code a shake down though.
            cast_tuple = self.api._staging_to_backend_cast(col, col)
            self.assertIsInstance(cast_tuple, tuple)
            self.assertIsNotNone(cast_tuple[0])
            self.assertEqual(len(cast_tuple), 3)

    def _test_get_columns(self):
        cols = self.api.get_columns()
        self.assertTrue(cols)
        self.assertIsInstance(cols, list)
        self.assertIsInstance(cols[0], ColumnMetadataInterface)

    def _test_get_default_location(self):
        try:
            self.api.get_default_location()
        except NotImplementedError:
            pass

    def _test_get_partition_columns(self):
        """ Unit tests are based on SALES therefore we should have partition columns """
        cols = self.api.get_partition_columns()
        if self.api.partition_by_column_supported():
            self.assertTrue(cols)
            self.assertIsInstance(cols, list)
            self.assertIsInstance(cols[0], ColumnMetadataInterface)
        else:
            self.assertFalse(cols)

    def _test_get_staging_table_location(self):
        try:
            self.api.get_staging_table_location()
        except NotImplementedError:
            pass

    def _test_is_incremental_update_enabled(self):
        self.assertIn(self.api.is_incremental_update_enabled(), (True, False))

    def _test_result_cache_area_exists(self):
        try:
            self.assertIn(self.api.result_cache_area_exists(), (True, False))
        except NotImplementedError:
            pass

    def _test_setup_result_cache_area(self):
        self.api.setup_result_cache_area()

    def _test_setup_staging_area(self):
        self.api.setup_staging_area(None)

    def _test_staging_area_exists(self):
        self.assertIn(self.api.staging_area_exists(), (True, False))

    def _test_synthetic_bucket_data_type(self):
        if self.api.synthetic_bucketing_supported():
            self.api.synthetic_bucket_data_type()

    def _test_synthetic_bucket_filter_capable_column(self):
        if self.api.synthetic_bucketing_supported():
            column = self.api.get_columns()[0]
            result = self.api.synthetic_bucket_filter_capable_column(column)
            self.assertIsNotNone(result)
            self.assertIsInstance(result, tuple)

    def _test_synthetic_part_number_int_expressions(self):
        """ Ensure that difficult synthetic column inputs are converted equally by SQL and Orchestration code. """
        if not self.api.synthetic_partitioning_supported():
            return
        max_precision = 9
        canonical_column = CanonicalColumn('A_COLUMN', GLUENT_TYPE_INTEGER_4, from_override=True)
        num_column = self.api.from_canonical_column(canonical_column)
        for granularity in [1000, 10, 512, 1234]:
            for num in (partition_key_test_numbers(8, max_precision) +
                        partition_key_test_numbers(8, max_precision, negative=True)):
                self._compare_sql_and_python_synthetic_part_number_outcomes(num, num_column, granularity,
                                                                            max_precision + 2)

    def _test_synthetic_part_number_bigint_expressions(self):
        """ Ensure that difficult synthetic column inputs are converted equally by SQL and Orchestration code. """
        if not self.api.synthetic_partitioning_supported():
            return
        max_precision = 18
        canonical_column = CanonicalColumn('A_COLUMN', GLUENT_TYPE_INTEGER_8)
        num_column = self.api.from_canonical_column(canonical_column)
        for granularity in [1000, 10000, 8192, 1234]:
            for num in (partition_key_test_numbers(15, max_precision, wiggle_room=10) +
                        partition_key_test_numbers(15, max_precision, wiggle_room=10, negative=True)):
                self._compare_sql_and_python_synthetic_part_number_outcomes(num, num_column, granularity,
                                                                            max_precision + 2)

    def _test_synthetic_part_number_decimal_18_expressions(self):
        """ Ensure that difficult synthetic column inputs are converted equally by SQL and Orchestration code. """
        if not self.api.synthetic_partitioning_supported():
            return
        max_precision = min(self.api.max_decimal_precision(), 18)
        canonical_column = CanonicalColumn('A_COLUMN', GLUENT_TYPE_DECIMAL, data_precision=max_precision, data_scale=0)
        num_column = self.api.from_canonical_column(canonical_column)
        for granularity in [1000, 100000, 8192, 1234]:
            nums = partition_key_test_numbers(16, 18, wiggle_room=10)
            if self.api.backend_type() != DBTYPE_IMPALA:
                # Issues with large negative numbers on CDH. Saving for GOE-1938
                nums += partition_key_test_numbers(16, 18, negative=True, wiggle_room=10)
            for num in nums:
                self._compare_sql_and_python_synthetic_part_number_outcomes(num, num_column, granularity,
                                                                            max_precision + 2)

    def _test_synthetic_part_number_decimal_38_expressions(self):
        """ Ensure that difficult synthetic column inputs are converted equally by SQL and Orchestration code. """
        if not self.api.synthetic_partitioning_supported() or self.api.backend_type() != DBTYPE_IMPALA:
            # Only Hadoop currently copes with partition keys over BIGINT
            return
        max_precision = min(self.api.max_decimal_precision(), 38)
        canonical_column = CanonicalColumn('A_COLUMN', GLUENT_TYPE_DECIMAL, data_precision=max_precision, data_scale=0)
        num_column = self.api.from_canonical_column(canonical_column)
        for granularity in [1000, 100000, 16384, 1234]:
            for num in (partition_key_test_numbers(16, 19, wiggle_room=10) +
                        partition_key_test_numbers(min(max_precision, 36), max_precision, wiggle_room=10) +
                        partition_key_test_numbers(16, 19, negative=True, wiggle_room=10) +
                        partition_key_test_numbers(min(max_precision, 36), max_precision,
                                                   wiggle_room=10, negative=True)
            ):
                if num >= 0:
                    # All negatives fail with DECIMAL(38) and FLOOR SQL syntax
                    self._compare_sql_and_python_synthetic_part_number_outcomes(num, num_column, granularity,
                                                                                max_precision + 2)

    def _test_synthetic_part_number_decimal_18_9_expressions(self):
        """ Ensure that difficult synthetic column inputs are converted equally by SQL and Orchestration code. """
        if not self.api.synthetic_partitioning_supported():
            return
        # 12399.6 truncates decimal places on CDH 5.15 and rounds on CDH 6.2 therefore a good test value.
        canonical_column = CanonicalColumn('A_COLUMN', GLUENT_TYPE_DECIMAL, data_precision=18, data_scale=9)
        num_column = self.api.from_canonical_column(canonical_column)
        for granularity in [1000, 100, 8192, 1234]:
            for num in [12391.6,
                        -12391.6,
                        12399.6,
                        -12399.6,
                        # Full 9 decimal places
                        decimal.Decimal('12345678.123456789'),
                        decimal.Decimal('12345678.987654321'),
                        decimal.Decimal('87654321.123456789'),
                        decimal.Decimal('87654321.987654321'),
                        decimal.Decimal('10000000.000000001'),
                        decimal.Decimal('-10000000.000000001'),
                        decimal.Decimal('-12345678.123456789'),
                        decimal.Decimal('-12345678.987654321'),
                        decimal.Decimal('-87654321.123456789'),
                        decimal.Decimal('-87654321.987654321'),
                        ]:
                self._compare_sql_and_python_synthetic_part_number_outcomes(num, num_column, granularity, 38)

    def _test_synthetic_part_number_decimal_38_9_expressions(self):
        """ Ensure that difficult synthetic column inputs are converted equally by SQL and Orchestration code. """
        if not self.api.synthetic_partitioning_supported() or self.api.backend_type() != DBTYPE_IMPALA:
            # Only Hadoop currently copes with partition keys over BIGINT
            return
        # 12399.6 truncates decimal places on CDH 5.15 and rounds on CDH 6.2 therefore a good test value.
        canonical_column = CanonicalColumn('A_COLUMN', GLUENT_TYPE_DECIMAL, data_precision=38, data_scale=9)
        num_column = self.api.from_canonical_column(canonical_column)
        for granularity in [1000, 100, 8192, 1234]:
            for num in [12391.6,
                        -12391.6,
                        12399.6,
                        -12399.6,
                        decimal.Decimal('12345678901234567.12345'),
                        decimal.Decimal('-12345678901234567.12345'),
                        decimal.Decimal('1998849133104929799110640.003010780'),
                        decimal.Decimal('-1998849133104929799110640.003010780'),
                        ]:
                self._compare_sql_and_python_synthetic_part_number_outcomes(num, num_column, granularity, 38)

    def _run_all_tests(self):
        # Private methods, feels wrong but there are just a few we want testing
        self._test__derive_partition_info()
        self._test__gen_synthetic_literal_function()
        self._test__gen_synthetic_part_date_as_string_sql_expr()
        self._test__gen_synthetic_part_number_granularity_sql_expr()
        self._test__gen_synthetic_part_number_sql_expr()
        self._test__gen_synthetic_part_string_sql_expr()
        self._test__staging_to_backend_cast()
        # Public methods
        self._test_alter_table_sort_columns()
        self._test_cleanup_staging_area()
        self._test_compute_final_table_stats()
        self._test_create_backend_table()
        self._test_create_db()
        self._test_derive_unicode_string_columns()
        self._test_get_columns()
        self._test_get_default_location()
        self._test_get_partition_columns()
        self._test_get_staging_table_location()
        self._test_is_incremental_update_enabled()
        self._test_result_cache_area_exists()
        self._test_setup_result_cache_area()
        self._test_setup_staging_area()
        self._test_staging_area_exists()
        self._test_synthetic_bucket_data_type()
        self._test_synthetic_bucket_filter_capable_column()
        self._test_synthetic_part_number_int_expressions()
        self._test_synthetic_part_number_bigint_expressions()
        self._test_synthetic_part_number_decimal_18_expressions()
        self._test_synthetic_part_number_decimal_38_expressions()
        self._test_synthetic_part_number_decimal_18_9_expressions()
        self._test_synthetic_part_number_decimal_38_9_expressions()


class TestCurrentBackendTable(TestBackendTable):

    def setUp(self):
        self.options = self._build_current_options()
        super(TestCurrentBackendTable, self).setUp()

    @staticmethod
    def _build_current_options():
        return build_current_options()

    def test_full_api_on_current_backend(self):
        self._run_all_tests()


if __name__ == '__main__':
    main()
