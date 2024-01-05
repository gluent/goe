from unittest.mock import Mock

from goe.offload.offload_metadata_functions import (
    column_name_list_to_csv,
    incremental_hv_list_from_csv,
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
    gen_offload_metadata,
)
from goe.offload.oracle.oracle_column import OracleColumn
from goe.orchestration.execution_id import ExecutionId


def test_incremental_hv_list_from_csv():
    test_metadata = "TO_DATE(' 2011-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
    decoded_metadata = incremental_hv_list_from_csv(
        test_metadata, INCREMENTAL_PREDICATE_TYPE_RANGE
    )
    assert decoded_metadata == test_metadata

    test_metadata = "('A', 'B'), ('C', 'D')"
    decoded_metadata = incremental_hv_list_from_csv(
        test_metadata, INCREMENTAL_PREDICATE_TYPE_LIST
    )
    assert decoded_metadata == ["'A', 'B'", "'C', 'D'"]

    test_metadata = "2015, 03"
    decoded_metadata = incremental_hv_list_from_csv(
        test_metadata, INCREMENTAL_PREDICATE_TYPE_RANGE
    )
    assert decoded_metadata == test_metadata


def test_column_name_list_to_csv():
    test_metadata = ["COLUMN_1", "COLUMN_2"]
    csv_formatted_metadata = column_name_list_to_csv(test_metadata)
    assert csv_formatted_metadata == "COLUMN_1,COLUMN_2"
    tsv_formatted_metadata = column_name_list_to_csv(test_metadata, "~")
    assert tsv_formatted_metadata == "COLUMN_1~COLUMN_2"


def test_gen_offload_metadata_rpa():
    """Test metadata generation for Offload of first and second partitions."""
    execution_id = ExecutionId()
    fake_repo_client = None
    partition_key = "TIME_ID"
    inc_cols = [
        OracleColumn(partition_key, "DATE"),
    ]

    # First offload
    fake_hybrid_operation = Mock()
    fake_hybrid_operation.owner = "goe_test"
    fake_hybrid_operation.table_name = "sales"
    fake_hybrid_operation.bucket_hash_col = "CUST_ID"
    fake_hybrid_operation.offload_partition_functions = ["UDF1"]
    fake_hybrid_operation.sort_columns = ["CUST_ID"]
    fake_hybrid_operation.ipa_predicate_type = "RANGE"
    fake_hybrid_operation.execution_id = execution_id
    fake_hybrid_operation.offload_type = "INCREMENTAL"
    fake_hybrid_operation.hwm_in_hybrid_view = True
    fake_hybrid_operation.offload_by_subpartition = False
    pre_offload_metadata = None
    pre_offload_scn = 123
    inc_hvs = "TO_DATE(' 2012-03-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
    generated_metadata = gen_offload_metadata(
        fake_repo_client,
        fake_hybrid_operation,
        fake_hybrid_operation.owner.upper(),
        fake_hybrid_operation.table_name.upper(),
        fake_hybrid_operation.owner.lower(),
        fake_hybrid_operation.table_name.lower(),
        inc_cols,
        [
            inc_hvs,
        ],
        None,
        pre_offload_scn,
        pre_offload_metadata,
    )
    expected_metadata = {
        "OFFLOAD_TYPE": "INCREMENTAL",
        "HADOOP_OWNER": fake_hybrid_operation.owner.lower(),
        "HADOOP_TABLE": fake_hybrid_operation.table_name.lower(),
        "OFFLOADED_OWNER": fake_hybrid_operation.owner.upper(),
        "OFFLOADED_TABLE": fake_hybrid_operation.table_name.upper(),
        "OFFLOAD_SNAPSHOT": pre_offload_scn,
        "INCREMENTAL_KEY": partition_key,
        "INCREMENTAL_HIGH_VALUE": inc_hvs,
        "INCREMENTAL_PREDICATE_TYPE": fake_hybrid_operation.ipa_predicate_type,
        "INCREMENTAL_PREDICATE_VALUE": None,
        "OFFLOAD_BUCKET_COLUMN": fake_hybrid_operation.bucket_hash_col,
        "OFFLOAD_SORT_COLUMNS": fake_hybrid_operation.sort_columns[0],
        "INCREMENTAL_RANGE": "PARTITION",
        "OFFLOAD_PARTITION_FUNCTIONS": fake_hybrid_operation.offload_partition_functions[
            0
        ],
        "COMMAND_EXECUTION": execution_id,
    }
    assert expected_metadata == generated_metadata.as_dict()

    # Test metadata generation for Offload of second partition.
    # Capture metadata from previous test to use as input for this one.
    pre_offload_metadata = generated_metadata
    prior_offload_scn = pre_offload_scn

    # Set new operational values.
    # New execution id, GOE version, SCN and partition high value.
    execution_id = ExecutionId()
    fake_hybrid_operation.owner = "GOE_TEST"
    fake_hybrid_operation.table_name = "SALES"
    fake_hybrid_operation.bucket_hash_col = "CUST_ID"
    fake_hybrid_operation.offload_partition_functions = ["UDF1"]
    fake_hybrid_operation.sort_columns = ["CUST_ID", "PROD_ID"]
    fake_hybrid_operation.ipa_predicate_type = "RANGE"
    fake_hybrid_operation.execution_id = execution_id
    fake_hybrid_operation.offload_type = "INCREMENTAL"
    fake_hybrid_operation.hwm_in_hybrid_view = True
    fake_hybrid_operation.offload_by_subpartition = False
    pre_offload_scn = 321
    inc_hvs = "TO_DATE(' 2012-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
    generated_metadata = gen_offload_metadata(
        fake_repo_client,
        fake_hybrid_operation,
        fake_hybrid_operation.owner.upper(),
        fake_hybrid_operation.table_name.upper(),
        fake_hybrid_operation.owner.lower(),
        fake_hybrid_operation.table_name.lower(),
        inc_cols,
        [
            inc_hvs,
        ],
        None,
        pre_offload_scn,
        pre_offload_metadata,
    )
    expected_metadata = {
        "OFFLOAD_TYPE": "INCREMENTAL",
        "HADOOP_OWNER": fake_hybrid_operation.owner.lower(),
        "HADOOP_TABLE": fake_hybrid_operation.table_name.lower(),
        "OFFLOADED_OWNER": fake_hybrid_operation.owner.upper(),
        "OFFLOADED_TABLE": fake_hybrid_operation.table_name.upper(),
        "OFFLOAD_SNAPSHOT": prior_offload_scn,
        "INCREMENTAL_KEY": partition_key,
        "INCREMENTAL_HIGH_VALUE": inc_hvs,
        "INCREMENTAL_PREDICATE_TYPE": fake_hybrid_operation.ipa_predicate_type,
        "INCREMENTAL_PREDICATE_VALUE": None,
        "OFFLOAD_BUCKET_COLUMN": fake_hybrid_operation.bucket_hash_col,
        "OFFLOAD_SORT_COLUMNS": ",".join(fake_hybrid_operation.sort_columns),
        "INCREMENTAL_RANGE": "PARTITION",
        "OFFLOAD_PARTITION_FUNCTIONS": fake_hybrid_operation.offload_partition_functions[
            0
        ],
        "COMMAND_EXECUTION": execution_id,
    }
    assert expected_metadata == generated_metadata.as_dict()


# TODO We should add PBO tests for gen_offload_metadata
