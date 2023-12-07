from test_sets.stories.story_globals import OFFLOAD_PATTERN_100_0, OFFLOAD_PATTERN_100_10, OFFLOAD_PATTERN_90_10, \
    STORY_TYPE_LINK_ID, STORY_TYPE_OFFLOAD, STORY_TYPE_SETUP, \
    STORY_SETUP_TYPE_FRONTEND, STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_PYTHON
from test_sets.stories.story_setup_functions import drop_backend_test_table, gen_add_sales_based_fact_partition_ddl,\
    gen_add_sales_based_list_partition_ddl, gen_hybrid_drop_ddl, gen_sales_based_fact_create_ddl,\
    gen_sales_based_list_create_ddl, \
    SALES_BASED_FACT_PRE_HV, SALES_BASED_FACT_HV_1, SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3, \
    SALES_BASED_FACT_HV_4, SALES_BASED_FACT_HV_5, SALES_BASED_FACT_HV_6, SALES_BASED_FACT_HV_7, \
    SALES_BASED_FACT_HV_8, SALES_BASED_FACT_HV_9
from test_sets.stories.story_assertion_functions import offload_fact_assertions, text_in_messages

from goe.gluent import verbose
from goe.offload import offload_constants
from goe.offload.offload_metadata_functions import OFFLOAD_TYPE_FULL, OFFLOAD_TYPE_INCREMENTAL
from goe.offload.offload_source_data import NO_MAXVALUE_PARTITION_NOTICE_TEXT
from goe.persistence.orchestration_metadata import INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE, \
    INCREMENTAL_PREDICATE_TYPE_RANGE
from testlib.test_framework.test_functions import log


OFFLOAD_FULL_FACT = 'STORY_FULL_FACT'
OFFLOAD_FULL_LR_FACT = 'STORY_FULL_LR_FACT'
OFFLOAD_FULL_MAXPART_TABLE = 'STORY_MAXVAL_FACT'
LR_STORY_ID = '_lr'


def offload_full_std_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api, repo_client,
                                 table_name, ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_RANGE):
    story_id = ''
    part_key_type = None
    inc_key = 'TIME_ID'
    add_part_fn = lambda: gen_add_sales_based_fact_partition_ddl(schema, table_name, options, frontend_api)
    check_hvs = [SALES_BASED_FACT_PRE_HV, SALES_BASED_FACT_HV_1, SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3,
                 SALES_BASED_FACT_HV_4, SALES_BASED_FACT_HV_5, SALES_BASED_FACT_HV_6, SALES_BASED_FACT_HV_7,
                 SALES_BASED_FACT_HV_8, SALES_BASED_FACT_HV_9]
    if ipa_predicate_type == INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE:
        if frontend_api:
            if not frontend_api.gluent_lpa_supported():
                log('Skipping INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE because gluent_lpa_supported == False',
                    detail=verbose)
                return []
            part_key_type = frontend_api.test_type_canonical_date()
        story_id = LR_STORY_ID
        inc_key = 'YRMON'
        add_part_fn = lambda: gen_add_sales_based_list_partition_ddl(schema, table_name, options, frontend_api)
        # For list we need to check for the prior HV so bump them all along 1
        check_hvs.insert(0, None)
    return [
        {'id': 'offload_full_fact%s_incr_90_10_offload1' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': '90/10 Offload All Partitions of Fact',
         'narrative': 'Offloads a partitioned table as INCREMENTAL with no partition filter. All partitions offloaded, HV matches that of final partition.',
         'options': {'owner_table': schema + '.' + table_name,
                     'reset_backend_table': True,
                     'ipa_predicate_type': ipa_predicate_type,
                     'offload_type': OFFLOAD_TYPE_INCREMENTAL},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, SALES_BASED_FACT_HV_7,
                                                    offload_pattern=OFFLOAD_PATTERN_90_10, incremental_key=inc_key,
                                                    ipa_predicate_type=ipa_predicate_type,
                                                    incremental_key_type=part_key_type)},
        {'id': 'offload_full_fact%s_incr_90_10_offload2' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': '90/10 Offload 1st Partition of Fact',
         'narrative': 'Offloads 1st partition as prep for next test to offload remainder',
         'options': {'owner_table': schema + '.' + table_name,
                     'reset_backend_table': True,
                     'older_than_date': SALES_BASED_FACT_HV_1,
                     'ipa_predicate_type': ipa_predicate_type,
                     'offload_type': OFFLOAD_TYPE_INCREMENTAL},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, check_hvs[1],
                                                    offload_pattern=OFFLOAD_PATTERN_90_10,
                                                    incremental_key=inc_key, ipa_predicate_type=ipa_predicate_type,
                                                    incremental_key_type=part_key_type)},
        {'id': 'offload_full_fact%s_incr_90_10_offload3' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': '90/10 Offload All Remaining Partitions of Fact',
         'narrative': 'Offloads remaining partitions from INCREMENTAL table with no partition filter. HV should match that of final partition.',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_type': OFFLOAD_TYPE_INCREMENTAL},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, SALES_BASED_FACT_HV_7,
                                                    offload_pattern=OFFLOAD_PATTERN_90_10, incremental_key=inc_key,
                                                    ipa_predicate_type=ipa_predicate_type,
                                                    incremental_key_type=part_key_type)},
        {'id': 'offload_full_fact%s_setup2' % story_id,
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'offload_full_fact%s_setup1' % story_id},
        {'id': 'offload_full_fact%s_90_10' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': '90/10 Offload of Fact Ready to Convert',
         'narrative': 'Offloads first partitions from a fact table ready for subsequent tests',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'ipa_predicate_type': ipa_predicate_type,
                     'reset_backend_table': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, check_hvs[2],
                                                    offload_pattern=OFFLOAD_PATTERN_90_10, incremental_key=inc_key,
                                                    ipa_predicate_type=ipa_predicate_type,
                                                    incremental_key_type=part_key_type)},
        {'id': 'offload_full_fact%s_to_100_0' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Switch Fact Table to 100/0',
         'narrative': 'Use offload to convert to offload type full. Will offload remaining partitions, update hybrid view to not have UNION ALL and mark metadata as FULL',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_type': OFFLOAD_TYPE_FULL},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name,
                                                    None, offload_pattern=OFFLOAD_PATTERN_100_0)},
        {'id': 'offload_full_fact%s_add_part1' % story_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Add a partition to story_full_fact',
         'narrative': 'Add a partition (with data) to story_full_fact',
         'setup': {STORY_SETUP_TYPE_FRONTEND: add_part_fn}},
        {'id': 'offload_full_fact%s_100_0_incr' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload new partition in 100/0 fact',
         'narrative': 'Offload new partition in 100/0 fact',
         'options': {'owner_table': schema + '.' + table_name},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name,
                                                    None, offload_pattern=OFFLOAD_PATTERN_100_0)},
        {'id': 'offload_full_fact%s_to_100_10' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Switch Fact Table to 100/10',
         'narrative': 'Use offload to convert 100/0 to 100/10. Will update hybrid view to have UNION ALL and mark metadata as FULL',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'offload_type': OFFLOAD_TYPE_FULL},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, check_hvs[2],
                                                    offload_pattern=OFFLOAD_PATTERN_100_10, incremental_key=inc_key,
                                                    ipa_predicate_type=ipa_predicate_type,
                                                    incremental_key_type=part_key_type)},
        {'id': 'offload_full_fact%s_100_10_hwm' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Move HWM in 100/10 Offload',
         'narrative': 'Move HWM in 100/10 Offload',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': SALES_BASED_FACT_HV_3,
                     'offload_type': OFFLOAD_TYPE_FULL},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, check_hvs[3],
                                                    offload_pattern=OFFLOAD_PATTERN_100_10, incremental_key=inc_key,
                                                    ipa_predicate_type=ipa_predicate_type,
                                                    incremental_key_type=part_key_type)},
        {'id': 'offload_full_fact%s_add_part2' % story_id,
         'type': STORY_TYPE_LINK_ID,
         'linked_id': 'offload_full_fact%s_add_part1' % story_id},
        {'id': 'offload_full_fact%s_100_10_new_part' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload new partition to 100/10',
         'narrative': 'Offload new partition to 100/10 without specifying a HWM',
         'options': {'owner_table': schema + '.' + table_name},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, SALES_BASED_FACT_HV_9,
                                                    offload_pattern=OFFLOAD_PATTERN_100_10, incremental_key=inc_key,
                                                    ipa_predicate_type=ipa_predicate_type,
                                                    incremental_key_type=part_key_type)},
        {'id': 'offload_full_fact%s_to_90_10' % story_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Convert Fact Table to 90/10',
         'narrative': 'Use offload to convert 100/10 to 90/10. Will update hybrid view to have UNION ALL and mark metadata as INCREMENTAL but will ignore specified HWM and do the best it can',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'offload_type': OFFLOAD_TYPE_INCREMENTAL},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, check_hvs[2],
                                                    offload_pattern=OFFLOAD_PATTERN_90_10, incremental_key=inc_key,
                                                    ipa_predicate_type=ipa_predicate_type,
                                                    incremental_key_type=part_key_type)},
    ]


def offload_full_maxval_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                    repo_client, table_name):
    return [
        {'id': 'offload_full_maxval_fact_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % table_name,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, table_name,
                                                                                      maxval_partition=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, table_name),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, table_name)]}},
        {'id': 'offload_full_maxval_fact_90_10',
         'type': STORY_TYPE_OFFLOAD,
         'title': '90/10 Offload of Fact Ready to Convert',
         'narrative': 'Offloads first partitions from a fact table ready for subsequent tests',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'reset_backend_table': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name,
                                                    SALES_BASED_FACT_HV_2, offload_pattern=OFFLOAD_PATTERN_90_10)},
        {'id': 'offload_full_maxval_fact_no_maxval_in_90_10',
         'type': STORY_TYPE_OFFLOAD,
         'title': '90/10 Offload of Fact with MAXVALUE Partition',
         'narrative': 'Offloads all partitions from a MAXVALUE fact table but in 90/10, the MAXVALUE partition should be skipped',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_type': OFFLOAD_TYPE_INCREMENTAL},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name,
                                                    SALES_BASED_FACT_HV_6, offload_pattern=OFFLOAD_PATTERN_90_10) + \
                            [(lambda test: text_in_messages(test.offload_messages, NO_MAXVALUE_PARTITION_NOTICE_TEXT),
                              lambda test: True)]},
        {'id': 'offload_full_maxval_fact_to_100_0',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 90/10 fact to 100/0',
         'narrative': 'Offloads all partitions from a fact table including MAXVALUE partition',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_type': OFFLOAD_TYPE_FULL},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name,
                                                    None, offload_pattern=OFFLOAD_PATTERN_100_0)},
        {'id': 'offload_full_maxval_fact_to_100_10',
         'type': STORY_TYPE_OFFLOAD,
         'title': '100/10 Offload of Fact',
         'narrative': 'Convert offloaded table to 100/10 with low HWM',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': SALES_BASED_FACT_HV_2,
                     'force': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name,
                                                    SALES_BASED_FACT_HV_2, offload_pattern=OFFLOAD_PATTERN_100_10)},
        {'id': 'offload_full_maxval_fact_100_10_hwm',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Move HWM in 100/10 Offload',
         'narrative': 'Move HWM in 100/10 Offload',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': SALES_BASED_FACT_HV_5,
                     'force': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name,
                                                    SALES_BASED_FACT_HV_5, offload_pattern=OFFLOAD_PATTERN_100_10)},
        {'id': 'offload_full_maxval_fact_10_to_0',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Convert 100/10 Offload to 100/0',
         'narrative': 'Convert 100/10 Offload to 100/0',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_type': OFFLOAD_TYPE_FULL,
                     'force': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name,
                                                    None, offload_pattern=OFFLOAD_PATTERN_100_0)},
        {'id': 'offload_full_maxval_fact_to_90_10',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Convert 100/0 to 90/10',
         'narrative': 'Failed attempt 100/0 to 90/10, not a valid action for tables with MAXVALUE partition',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': SALES_BASED_FACT_HV_7,
                     'offload_type': OFFLOAD_TYPE_INCREMENTAL,
                     'force': True},
         'expected_exception_string': offload_constants.PART_OUT_OF_RANGE},
    ]


def offload_full_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api, repo_client):
    return [
        {'id': 'offload_full_fact_setup1',
         'type': STORY_TYPE_SETUP,
         'title': 'Create RANGE %s' % OFFLOAD_FULL_FACT,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_fact_create_ddl(frontend_api, schema,
                                                                                      OFFLOAD_FULL_FACT),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, OFFLOAD_FULL_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_FULL_FACT)]}},
        {'id': 'offload_full_fact%s_setup1' % LR_STORY_ID,
         'type': STORY_TYPE_SETUP,
         'title': 'Create LIST_AS_RANGE %s' % OFFLOAD_FULL_LR_FACT,
         'setup': {STORY_SETUP_TYPE_FRONTEND:
                    lambda: gen_sales_based_list_create_ddl(frontend_api, schema, OFFLOAD_FULL_LR_FACT,
                                                            part_key_type=frontend_api.test_type_canonical_date(),
                                                            include_older_partition=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema,
                                                                OFFLOAD_FULL_LR_FACT),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, OFFLOAD_FULL_LR_FACT)]},
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
    ] + \
    offload_full_std_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                 repo_client, OFFLOAD_FULL_FACT) + \
    offload_full_std_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                 repo_client, OFFLOAD_FULL_LR_FACT,
                                 ipa_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE) + \
    offload_full_maxval_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                    repo_client, OFFLOAD_FULL_MAXPART_TABLE)
