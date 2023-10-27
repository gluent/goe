from gluentlib.offload.offload_constants import DBTYPE_HIVE, DBTYPE_IMPALA, DBTYPE_ORACLE, DBTYPE_TERADATA
from gluentlib.offload.offload_functions import convert_backend_identifier_case
from gluentlib.offload.offload_source_data import TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT
from gluentlib.offload.oracle.oracle_column import ORACLE_TYPE_NVARCHAR2
from testlib.test_framework.test_constants import PARTITION_FUNCTION_TEST_FROM_INT8,\
    PARTITION_FUNCTION_TEST_FROM_STRING
from testlib.test_framework.test_functions import log

from test_sets.stories.story_globals import STORY_SETUP_TYPE_FRONTEND, STORY_SETUP_TYPE_HYBRID, \
    STORY_SETUP_TYPE_ORACLE, STORY_SETUP_TYPE_PYTHON, \
    STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import drop_backend_test_table, gen_drop_sales_based_fact_partition_ddls,\
    gen_hybrid_drop_ddl, gen_sales_based_fact_create_ddl, sales_based_fact_partition_exists,\
    SALES_BASED_FACT_PRE_HV, SALES_BASED_FACT_HV_1, SALES_BASED_FACT_HV_2,\
    SALES_BASED_FACT_HV_3, SALES_BASED_FACT_HV_4, \
    SALES_BASED_FACT_PRE_HV_NUM, SALES_BASED_FACT_HV_1_NUM, SALES_BASED_FACT_HV_2_NUM,\
    SALES_BASED_FACT_HV_3_NUM, SALES_BASED_FACT_HV_4_NUM, SALES_BASED_FACT_HV_7_NUM
from test_sets.stories.story_assertion_functions import offload_fact_assertions, synthetic_part_col_name


RPA_FACT_TABLE_NUM = 'STORY_RPA_NUM'
RPA_FACT_TABLE_NUM_UDF = 'STORY_RPA_UNUM'
RPA_FACT_TABLE_DATE = 'STORY_RPA_DATE'
RPA_FACT_TABLE_TS = 'STORY_RPA_TS'
RPA_FACT_TABLE_STR = 'STORY_RPA_STR'
RPA_FACT_TABLE_STR_UDF = 'STORY_RPA_USTR'
RPA_FACT_TABLE_NSTR = 'STORY_RPA_NSTR'
RPA_ALPHA_FACT_TABLE = 'STORY_RPA_ALPHA'


def offload_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                           repo_client, part_key_type, partition_function=None) -> list:
    if options:
        options_target = options.target
        canonical_int_8 = frontend_api.test_type_canonical_int_8()
        canonical_string = frontend_api.test_type_canonical_string()
    else:
        options_target = DBTYPE_IMPALA
        canonical_int_8 = None
        canonical_string = None
    hv_0 = SALES_BASED_FACT_PRE_HV_NUM
    hv_1 = SALES_BASED_FACT_HV_1_NUM
    hv_2 = SALES_BASED_FACT_HV_2_NUM
    hv_3 = SALES_BASED_FACT_HV_3_NUM
    hv_4 = SALES_BASED_FACT_HV_4_NUM
    less_than_option = 'less_than_value'
    lower_value = None
    upper_value = None
    udf = different_udf = None
    synthetic_partition_column_expected = False
    if options_target in [DBTYPE_IMPALA, DBTYPE_HIVE]:
        synthetic_partition_column_expected = True
    synthetic_partition_digits = different_partition_digits = None
    if partition_function and backend_api and not backend_api.gluent_partition_functions_supported():
        log('Skipping offload_rpa partition function tests due to gluent_partition_functions_supported() == False')
        return []
    if part_key_type == canonical_int_8:
        if options and options.db_type == DBTYPE_TERADATA:
            # TODO We need our numeric sales table to be partitioned on YYYYMM for assertions to make sense.
            #      Didn't have time to rectify this for Teradata MVP.
            log(f'Skipping offload_range_ipa_standard_story_tests for system/type: {options.db_type}/{part_key_type}')
            return []
        if partition_function:
            story_id = 'unum'
            table_name = RPA_FACT_TABLE_NUM_UDF
            udf = data_db + '.' + partition_function
            different_udf = udf + '_unknown'
            synthetic_partition_column_expected = True
        else:
            story_id = 'num'
            table_name = RPA_FACT_TABLE_NUM
        granularity = '100'
        different_granularity = '1000'
        lower_value = SALES_BASED_FACT_PRE_HV_NUM
        upper_value = SALES_BASED_FACT_HV_7_NUM
        if options_target in [DBTYPE_IMPALA, DBTYPE_HIVE]:
            synthetic_partition_digits = 15
            different_partition_digits = 16
    elif part_key_type == canonical_string:
        if options and options.db_type == DBTYPE_TERADATA:
            # TODO In Teradata MVP we don't support string based partitioning.
            log(f'Skipping offload_range_ipa_standard_story_tests for system/type: {options.db_type}/{part_key_type}')
            return []
        if partition_function:
            story_id = 'ustr'
            table_name = RPA_FACT_TABLE_STR_UDF
            udf = data_db + '.' + partition_function
            different_udf = udf + '_unknown'
            synthetic_partition_column_expected = True
            granularity = '100'
            different_granularity = '1000'
            lower_value = SALES_BASED_FACT_PRE_HV_NUM
            upper_value = SALES_BASED_FACT_HV_7_NUM
        else:
            story_id = 'str'
            table_name = RPA_FACT_TABLE_STR
            granularity = '4'
            different_granularity = '6'
    elif part_key_type == ORACLE_TYPE_NVARCHAR2:
        if options and options.db_type != DBTYPE_ORACLE:
            log(f'Skipping offload_range_ipa_standard_story_tests for system/type: {options.db_type}/{part_key_type}')
            return []
        story_id = 'nstr'
        table_name = RPA_FACT_TABLE_NSTR
        granularity = '4'
        different_granularity = '6'
    else:
        if part_key_type == frontend_api.test_type_canonical_timestamp():
            story_id = 'ts'
            table_name = RPA_FACT_TABLE_TS
        else:
            story_id = 'date'
            table_name = RPA_FACT_TABLE_DATE
        hv_0 = SALES_BASED_FACT_PRE_HV
        hv_1 = SALES_BASED_FACT_HV_1
        hv_2 = SALES_BASED_FACT_HV_2
        hv_3 = SALES_BASED_FACT_HV_3
        hv_4 = SALES_BASED_FACT_HV_4
        less_than_option = 'older_than_date'
        granularity = 'M'
        different_granularity = 'Y'

    offload3_opt_name = 'partition_names_csv'
    offload3_opt_value = 'P3'
    if options and options.db_type == DBTYPE_TERADATA:
        offload3_opt_name = less_than_option
        offload3_opt_value = hv_3

    expected_gl_part_name = None
    if synthetic_partition_column_expected:
        expected_gl_part_name = synthetic_part_col_name(granularity, 'time_id', partition_function=udf,
                                                        synthetic_partition_digits=synthetic_partition_digits)
        expected_gl_part_name = convert_backend_identifier_case(options, expected_gl_part_name)

    backend_name = convert_backend_identifier_case(options, table_name)

    return [
        {'id': 'offload_rpa_%s_setup' % story_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % table_name,
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, table_name,
                                                               extra_pred='AND EXTRACT(DAY FROM time_id) <= 10',
                                                               part_key_type=part_key_type,
                                                               simple_partition_names=True),
                   STORY_SETUP_TYPE_HYBRID:
                       gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, table_name),
                   STORY_SETUP_TYPE_PYTHON:
                       [lambda: drop_backend_test_table(options, backend_api, data_db, table_name)]}},
        {'id': 'offload_rpa_%s_offload_verification1' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Fact IPA Offload - Non-Execute',
         'narrative': 'Non-Execute RANGE offload of empty partition',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_0,
                     'offload_partition_functions': udf,
                     'offload_partition_granularity': granularity,
                     'offload_partition_lower_value': lower_value,
                     'offload_partition_upper_value': upper_value,
                     'synthetic_partition_digits': synthetic_partition_digits,
                     'reset_backend_table': True},
         'config_override': {'execute': False},
         # Empty partitions do not exist on Teradata
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_rpa_%s_offload1' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'RANGE Offload 1st Partition',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_1,
                     'offload_partition_functions': udf,
                     'offload_partition_granularity': granularity,
                     'offload_partition_lower_value': lower_value,
                     'offload_partition_upper_value': upper_value,
                     'reset_backend_table': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, hv_1,
                                                    incremental_key_type=part_key_type,
                                                    hadoop_table=backend_name, partition_functions=udf,
                                                    synthetic_partition_column_name=expected_gl_part_name)},
        {'id': 'offload_rpa_%s_offload2' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'RANGE Offload 2nd Partition',
         'narrative': 'Attempt to change some partition settings which is ignored because settings come from metadata',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_2,
                     'offload_partition_functions': different_udf,
                     'offload_partition_granularity': different_granularity,
                     'synthetic_partition_digits': different_partition_digits},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, hv_2,
                                                    incremental_key_type=part_key_type, partition_functions=udf,
                                                    synthetic_partition_column_name=expected_gl_part_name)},
        {'id': 'offload_rpa_%s_offload_verification3' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'RANGE Offload 3rd Partition - Verification',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_3},
         'config_override': {'execute': False},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, hv_2,
                                                    incremental_key_type=part_key_type, partition_functions=udf)},
        {'id': 'offload_rpa_%s_offload_fail1' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'RANGE Offload With Multiple Partition Names - Expect Exception',
         'options': {'owner_table': schema + '.' + table_name,
                     'partition_names_csv': 'P4,P5'},
         'expected_exception_string': TOO_MANY_PARTITION_NAMES_EXCEPTION_TEXT,
         #TODO  Partition names are unpredicatble on Teradata, for MVP we'll not test this.
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_rpa_%s_offload3' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'RANGE Offload 3rd Partition',
         'narrative': 'Test with partition identification by name (if not Teradata) and use agg validate',
         'options': {'owner_table': schema + '.' + table_name,
                     offload3_opt_name: offload3_opt_value,
                     'verify_row_count': 'aggregate'},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, hv_3,
                                                    incremental_key_type=part_key_type, partition_functions=udf),
         'prereq': lambda: options.db_type != DBTYPE_TERADATA},
        {'id': 'offload_rpa_%s_offload4' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'RANGE No-op of 3rd Partition',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_3},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, hv_3,
                                                    incremental_key_type=part_key_type, partition_functions=udf),
         # On Teradata we can't test by partition name in previous test so this test will not be a no-op.
         'expected_status': bool(options and options.db_type == DBTYPE_TERADATA)},
        {'id': 'offload_rpa_%s_drop_partition' % story_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Drop Oldest Partition From Fact',
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       lambda: gen_drop_sales_based_fact_partition_ddls(schema, table_name, [hv_0, hv_1],
                                                                        frontend_api, dropping_oldest=True)},
         'assertion_pairs':
             [(lambda test: sales_based_fact_partition_exists(schema, table_name, [hv_1], frontend_api),
               lambda test: False)]},
        {'id': 'offload_rpa_%s_offload5' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'RANGE Offload After Partition Drop',
         'narrative': """Offloads next partition from fact table after the oldest partition was dropped.
                         The verification step should still succeed.""",
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_4},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, hv_4,
                                                    check_rowcount=False, incremental_key_type=part_key_type,
                                                    partition_functions=udf)},
        {'id': 'offload_rpa_%s_drop_all_offloaded' % story_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Drop All Offloaded Partition From Fact',
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                       lambda: gen_drop_sales_based_fact_partition_ddls(schema, table_name, [hv_1, hv_2, hv_3, hv_4],
                                                                        frontend_api, dropping_oldest=True)},
         'assertion_pairs': [(lambda test: sales_based_fact_partition_exists(schema, table_name,
                                                                             [hv_1, hv_2, hv_3, hv_4], frontend_api),
                              lambda test: False)]},
        {'id': 'offload_rpa_%s_offload6' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'RANGE No-op Offload After Partition Drop',
         'narrative': 'Offloads no partitions from fact after all offloaded partitions have been dropped (GOE-1035)',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_4},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, hv_4,
                                                    check_rowcount=False, incremental_key_type=part_key_type,
                                                    partition_functions=udf),
         'expected_status': False},
    ]


def offload_range_ipa_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api, repo_client):
    canonical_int_8 = None
    canonical_date = None
    canonical_timestamp = None
    canonical_string = None
    if frontend_api:
        canonical_int_8 = frontend_api.test_type_canonical_int_8()
        canonical_date = frontend_api.test_type_canonical_date()
        canonical_timestamp = frontend_api.test_type_canonical_timestamp()
        canonical_string = frontend_api.test_type_canonical_string()
    return offload_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                               repo_client, canonical_int_8) + \
        offload_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                               repo_client, canonical_date) + \
        offload_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                               repo_client, canonical_timestamp) + \
        offload_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                               repo_client, canonical_string) +\
        offload_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                               repo_client, ORACLE_TYPE_NVARCHAR2) +\
        offload_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                               repo_client, canonical_int_8,
                                               partition_function=PARTITION_FUNCTION_TEST_FROM_INT8) +\
        offload_range_ipa_standard_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                               repo_client, canonical_string,
                                               partition_function=PARTITION_FUNCTION_TEST_FROM_STRING) +\
    [
        {'id': 'offload_rpa_alpha_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % RPA_ALPHA_FACT_TABLE,
         'narrative': 'Prepare for tests ensuring lower and upper case alpha characters are differentiated correctly',
         'setup': {STORY_SETUP_TYPE_ORACLE:
                       [f"DROP TABLE {schema}.{RPA_ALPHA_FACT_TABLE}",
                        f"""CREATE TABLE {schema}.{RPA_ALPHA_FACT_TABLE}
                            PARTITION BY RANGE(str)
                            ( PARTITION upper_a_j VALUES LESS THAN ('K')
                            , PARTITION upper_k_t VALUES LESS THAN ('U')
                            , PARTITION upper_u_z VALUES LESS THAN ('a')
                            , PARTITION lower_a_j VALUES LESS THAN ('k')
                            , PARTITION lower_k_t VALUES LESS THAN ('u')
                            , PARTITION lower_u_z VALUES LESS THAN ('zzzzzz'))
                            AS
                            SELECT owner
                            ,      object_name
                            ,      dbms_random.string('a',5) AS str
                            FROM   dba_objects
                            WHERE  object_id <= 1000"""],
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, RPA_ALPHA_FACT_TABLE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, RPA_ALPHA_FACT_TABLE)]},
         'prereq': lambda: bool(options.db_type == DBTYPE_ORACLE)},
        {'id': 'offload_rpa_alpha_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': '1st Offload By Uppoer Case Character',
         'options': {'owner_table': schema + '.' + RPA_ALPHA_FACT_TABLE,
                     'less_than_value': 'U',
                     'offload_partition_granularity': '1',
                     'reset_backend_table': True},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, RPA_ALPHA_FACT_TABLE, 'U', incremental_key='STR',
                                     incremental_key_type=canonical_string),
         'prereq': lambda: bool(options.db_type == DBTYPE_ORACLE)},
        {'id': 'offload_rpa_alpha_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': '2nd Offload By Lower Case Character',
         'options': {'owner_table': schema + '.' + RPA_ALPHA_FACT_TABLE,
                     'less_than_value': 'u'},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, RPA_ALPHA_FACT_TABLE, 'u', incremental_key='STR',
                                     incremental_key_type=canonical_string),
         'prereq': lambda: bool(options.db_type == DBTYPE_ORACLE)},
    ]
