from test_sets.stories.story_globals import STORY_TYPE_OFFLOAD, STORY_SETUP_TYPE_ORACLE, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import drop_backend_test_table, gen_drop_sales_based_fact_partition_ddls,\
    gen_hybrid_drop_ddl, gen_sales_based_list_create_ddl, sales_based_fact_partition_exists,\
    gen_sales_based_subpartitioned_fact_ddl, gen_list_multi_part_value_create_ddl, \
    SALES_BASED_FACT_HV_1, SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3, SALES_BASED_FACT_HV_4, SALES_BASED_FACT_HV_5, \
    SALES_BASED_LIST_HV_1, SALES_BASED_LIST_HV_2, SALES_BASED_LIST_HV_3, SALES_BASED_LIST_HV_4, SALES_BASED_LIST_HV_5, \
    SALES_BASED_LIST_PNAME_4, SALES_BASED_LIST_PNAME_5,\
    STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_PYTHON, LOWER_YRMON_NUM, UPPER_YRMON_NUM
from test_sets.stories.story_assertion_functions import offload_fact_assertions, synthetic_part_col_name

from gluent import IPA_PREDICATE_TYPE_EXCEPTION_TEXT, IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT
from goe.offload.offload_constants import OFFLOAD_STATS_METHOD_COPY, OFFLOAD_STATS_METHOD_NATIVE
from goe.offload.offload_functions import convert_backend_identifier_case
from goe.offload.offload_source_data import INVALID_HV_FOR_LIST_AS_RANGE_EXCEPTION_TEXT, \
    TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT
from goe.offload.oracle.oracle_column import ORACLE_TYPE_DATE, ORACLE_TYPE_NUMBER,\
    ORACLE_TYPE_TIMESTAMP, ORACLE_TYPE_VARCHAR2
from goe.persistence.orchestration_metadata import INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE, \
    INCREMENTAL_PREDICATE_TYPE_RANGE
from testlib.test_framework.test_constants import PARTITION_FUNCTION_TEST_FROM_INT8
from testlib.test_framework.test_functions import log


LPA_FACT_TABLE_NUM = 'story_list_rpa_num'
LPA_FACT_TABLE_DATE = 'story_list_rpa_date'
LPA_FACT_TABLE_TS = 'story_list_rpa_ts'
LPA_FACT_TABLE_STR = 'story_list_rpa_str'
LPA_FACT_TABLE_NUM_UDF = 'story_list_rpa_unum'
LPA_FACT_TABLE_STR_UDF = 'story_list_rpa_ustr'
LIST_RANGE_TABLE = 'story_list_rpa_range'
STD_LPA_TABLE = 'story_list_rpa_bad'


def offload_list_as_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                                   repo_client, part_key_type, partition_function=None):
    option_target = options.target if options else None
    if partition_function and backend_api and not backend_api.gluent_partition_functions_supported():
        log('Skipping offload_rpa partition function tests due to gluent_partition_functions_supported() == False')
        return []

    hv_1 = SALES_BASED_LIST_HV_1
    hv_2 = SALES_BASED_LIST_HV_2
    hv_3 = SALES_BASED_LIST_HV_3
    hv_4 = SALES_BASED_LIST_HV_4
    hv_5 = SALES_BASED_LIST_HV_5
    less_than_option = 'less_than_value'
    udf = None
    gl_part_column_check_name = None
    if part_key_type == ORACLE_TYPE_NUMBER:
        if partition_function:
            story_id = 'unum'
            table_name = LPA_FACT_TABLE_NUM_UDF
            udf = data_db + '.' + partition_function
            gl_part_column_check_name = synthetic_part_col_name('U0', 'yrmon')
            gl_part_column_check_name = convert_backend_identifier_case(options, gl_part_column_check_name)
        else:
            story_id = 'num'
            table_name = LPA_FACT_TABLE_NUM
    elif part_key_type == ORACLE_TYPE_VARCHAR2:
        if partition_function:
            story_id = 'ustr'
            table_name = LPA_FACT_TABLE_STR_UDF
            udf = data_db + '.' + partition_function
            gl_part_column_check_name = synthetic_part_col_name('U0', 'yrmon')
            gl_part_column_check_name = convert_backend_identifier_case(options, gl_part_column_check_name)
        else:
            story_id = 'str'
            table_name = LPA_FACT_TABLE_STR
    else:
        if part_key_type == ORACLE_TYPE_TIMESTAMP:
            story_id = 'ts'
            table_name = LPA_FACT_TABLE_TS
        else:
            story_id = 'date'
            table_name = LPA_FACT_TABLE_DATE
        hv_1 = SALES_BASED_FACT_HV_1
        hv_2 = SALES_BASED_FACT_HV_2
        hv_3 = SALES_BASED_FACT_HV_3
        hv_4 = SALES_BASED_FACT_HV_4
        hv_5 = SALES_BASED_FACT_HV_5
        less_than_option = 'older_than_date'
    backend_name = convert_backend_identifier_case(options, table_name)
    return [
        {'id': 'offload_list_rpa_%s_setup1' % story_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % table_name,
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_sales_based_list_create_ddl(frontend_api, schema, table_name,
                                                                            part_key_type=part_key_type,
                                                                            default_partition=True,
                                                                            out_of_sequence=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, table_name),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, table_name)]}},
        {'id': 'offload_list_rpa_%s_offload_verification1' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Fact IPA Offload - Expect Exception',
         'narrative': 'Try to force LIST table to offload as RANGE',
         'options': {'owner_table': schema + '.' + table_name,
                     'ipa_predicate_type': INCREMENTAL_PREDICATE_TYPE_RANGE,
                     less_than_option: hv_1,
                     'offload_partition_functions': udf,
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'expected_exception_string': IPA_PREDICATE_TYPE_EXCEPTION_TEXT},
        {'id': 'offload_list_rpa_%s_offload_verification2' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Fact IPA Offload - Non-Execute',
         'narrative': 'Non-Execute LIST_AS_RANGE offload of 1st partition',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_1,
                     'offload_partition_functions': udf,
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         'expected_status': False},
        {'id': 'offload_list_rpa_%s_offload_verification3' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Fact IPA Offload - Non-Execute',
         'narrative': 'Non-Execute LIST_AS_RANGE offload of 1st partition',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_2,
                     'offload_partition_functions': udf,
                     'offload_partition_lower_value': LOWER_YRMON_NUM,
                     'offload_partition_upper_value': UPPER_YRMON_NUM,
                     'reset_backend_table': True},
         'config_override': {'execute': False}},
        {'id': 'offload_list_rpa_%s_offload1' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'LIST_AS_RANGE Offload 1st Partition',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_2,
                     'offload_partition_functions': udf,
                     'offload_partition_lower_value': LOWER_YRMON_NUM,
                     'offload_partition_upper_value': UPPER_YRMON_NUM,
                     'reset_backend_table': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, table_name, hv_1, incremental_key='YRMON',
                                                    ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                                    incremental_key_type=part_key_type, hadoop_table=backend_name,
                                                    partition_functions=udf,
                                                    synthetic_partition_column_name=gl_part_column_check_name)},
        {'id': 'offload_list_rpa_%s_offload2' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'LIST_AS_RANGE Offload 2nd Partition',
         'narrative': '2nd partition happens to be empty but we should still move HWMs etc',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_3},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, table_name, hv_2, incremental_key='YRMON',
                                                    ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                                    incremental_key_type=part_key_type,
                                                    partition_functions=udf,
                                                    synthetic_partition_column_name=gl_part_column_check_name)},
        {'id': 'offload_list_rpa_%s_offload_verification4' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'LIST_AS_RANGE Offload 3rd Partition - Verification',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_4},
         'config_override': {'execute': False},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, table_name, hv_2, incremental_key='YRMON',
                                                    ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                                    incremental_key_type=part_key_type,
                                                    partition_functions=udf)},
        {'id': 'offload_list_rpa_%s_offload_as_lpa' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Attempt to top up LIST_AS_RANGE by LPA - Expect Exception',
         'options': {'owner_table': schema + '.' + table_name,
                     'equal_to_values': hv_4},
         'expected_exception_string': IPA_PREDICATE_TYPE_FILTER_EXCEPTION_TEXT},
        {'id': 'offload_list_rpa_%s_offload3' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'LIST_AS_RANGE Offload With Multiple Partition Names - Expect Exception',
         'options': {'owner_table': schema + '.' + table_name,
                     'partition_names_csv': SALES_BASED_LIST_PNAME_4 + ',' + SALES_BASED_LIST_PNAME_5},
         'expected_exception_string': TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT},
        {'id': 'offload_list_rpa_%s_offload4' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'LIST_AS_RANGE Offload 3rd Partition',
         'narrative': 'Tests copy stats and also identifies partition by name',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_stats_method': OFFLOAD_STATS_METHOD_COPY if option_target == 'impala' else OFFLOAD_STATS_METHOD_NATIVE,
                     'partition_names_csv': SALES_BASED_LIST_PNAME_4},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, table_name, hv_3, incremental_key='YRMON',
                                                    ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                                    incremental_key_type=part_key_type,
                                                    partition_functions=udf)},
        {'id': 'offload_list_rpa_%s_offload5' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'LIST_AS_RANGE No-op of 3rd Partition',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_4},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, table_name, hv_3, incremental_key='YRMON',
                                                    ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                                    incremental_key_type=part_key_type,
                                                    partition_functions=udf),
         'expected_status': False},
        {'id': 'offload_list_rpa_%s_drop_partition' % story_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Drop Oldest Partition From Fact',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_drop_sales_based_fact_partition_ddls(schema, table_name, [hv_1],
                                                                                             frontend_api)},
         'assertion_pairs': [(lambda test: sales_based_fact_partition_exists(schema, table_name, [hv_1], frontend_api),
                              lambda test: False)]},
        {'id': 'offload_list_rpa_%s_offload6' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'LIST_AS_RANGE Offload After Partition Drop',
         'narrative': 'Offloads next partition from fact table after the oldest partition was dropped. The verification step should still succeed.',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_5},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, table_name, hv_4, incremental_key='YRMON',
                                                    ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                                    check_rowcount=False, incremental_key_type=part_key_type,
                                                    partition_functions=udf)},
        {'id': 'offload_list_rpa_%s_drop_all_offloaded' % story_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Drop All Offloaded Partition From Fact',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_drop_sales_based_fact_partition_ddls(schema, table_name,
                                                                                             [hv_1, hv_2, hv_3, hv_4],
                                                                                             frontend_api)},
         'assertion_pairs': [(lambda test: sales_based_fact_partition_exists(schema, table_name,
                                                                             [hv_1, hv_2, hv_3, hv_4], frontend_api),
                              lambda test: False)]},
        {'id': 'offload_list_rpa_%s_offload7' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'LIST_AS_RANGE No-op Offload After Partition Drop',
         'narrative': 'Offloads no partitions from fact table after all offloaded partitions have been dropped (GOE-1035)',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_5},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, table_name, hv_4, incremental_key='YRMON',
                                                    ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                                    check_rowcount=False, incremental_key_type=part_key_type,
                                                    partition_functions=udf),
         'expected_status': False},
    ]


def offload_list_as_range_ipa_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                          repo_client):
    return \
        offload_list_as_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api,
                                                       frontend_api, repo_client, ORACLE_TYPE_NUMBER) + \
        offload_list_as_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api,
                                                       frontend_api, repo_client, ORACLE_TYPE_DATE) + \
        offload_list_as_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api,
                                                       frontend_api, repo_client, ORACLE_TYPE_TIMESTAMP) + \
        offload_list_as_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api,
                                                       frontend_api, repo_client, ORACLE_TYPE_VARCHAR2) + \
        offload_list_as_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api,
                                                       frontend_api, repo_client, ORACLE_TYPE_NUMBER,
                                                       partition_function=PARTITION_FUNCTION_TEST_FROM_INT8) +\
    [
        {'id': 'offload_list_rpa_subpart_setup1',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % LIST_RANGE_TABLE,
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_sales_based_subpartitioned_fact_ddl(schema, LIST_RANGE_TABLE),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, LIST_RANGE_TABLE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, LIST_RANGE_TABLE)]}},
        {'id': 'offload_list_rpa_subpart_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Partition From LIST/RANGE Fact',
         'narrative': 'Offloads from a LIST/RANGE subpartitioned fact table with LIST_AS_RANGE proving we can use the IPA options on the top level',
         'options': {'owner_table': schema + '.' + LIST_RANGE_TABLE,
                     'reset_backend_table': True,
                     'less_than_value': '4',
                     'offload_partition_lower_value': 0,
                     'offload_partition_upper_value': 10},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                                    hybrid_schema, data_db, LIST_RANGE_TABLE, '3', incremental_key='CHANNEL_ID',
                                                    ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
                                                    incremental_key_type=ORACLE_TYPE_NUMBER, incremental_range='PARTITION')},
        {'id': 'offload_list_rpa_bad_fact_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % STD_LPA_TABLE,
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_list_multi_part_value_create_ddl(schema, STD_LPA_TABLE, ORACLE_TYPE_NUMBER, [1, 2, 3]),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, STD_LPA_TABLE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, STD_LPA_TABLE)]}},
        {'id': 'offload_list_rpa_bad_fact_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Attempt Offload Of Std LIST Fact',
         'options': {'owner_table': schema + '.' + STD_LPA_TABLE,
                     'reset_backend_table': True,
                     'less_than_value': '3'},
         'expected_exception_string': INVALID_HV_FOR_LIST_AS_RANGE_EXCEPTION_TEXT},
    ]
