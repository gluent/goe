from goe.offload.offload_metadata_functions import column_name_list_to_csv, incremental_hv_list_from_csv, INCREMENTAL_PREDICATE_TYPE_LIST, INCREMENTAL_PREDICATE_TYPE_RANGE


def test_x():
    test_metadata = "TO_DATE(' 2011-04-01 00:00:00', 'SYYYY-MM-DD HH24:MI:SS', 'NLS_CALENDAR=GREGORIAN')"
    decoded_metadata = incremental_hv_list_from_csv(test_metadata, INCREMENTAL_PREDICATE_TYPE_RANGE)
    assert decoded_metadata == test_metadata

    test_metadata = "('A', 'B'), ('C', 'D')"
    decoded_metadata = incremental_hv_list_from_csv(test_metadata, INCREMENTAL_PREDICATE_TYPE_LIST)
    assert decoded_metadata == ["'A', 'B'", "'C', 'D'"]

    test_metadata = "2015, 03"
    decoded_metadata = incremental_hv_list_from_csv(test_metadata, INCREMENTAL_PREDICATE_TYPE_RANGE)
    assert decoded_metadata == test_metadata

    test_metadata = ['COLUMN_1', 'COLUMN_2']
    csv_formatted_metadata = column_name_list_to_csv(test_metadata)
    assert csv_formatted_metadata == 'COLUMN_1,COLUMN_2'
    tsv_formatted_metadata = column_name_list_to_csv(test_metadata, '~')
    assert tsv_formatted_metadata == 'COLUMN_1~COLUMN_2'