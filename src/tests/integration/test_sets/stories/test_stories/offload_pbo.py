from textwrap import dedent

from gluent import (
    CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_CHANGE_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT,
    IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT,
    log,
)
from goe.offload.offload_constants import (
    DBTYPE_BIGQUERY,
    DBTYPE_ORACLE,
    DBTYPE_TERADATA,
)
from goe.offload.offload_metadata_functions import (
    OFFLOAD_TYPE_FULL,
    OFFLOAD_TYPE_INCREMENTAL,
)
from goe.offload.offload_source_data import (
    OFFLOAD_TYPE_CHANGE_FOR_PBO_EXCEPTION_TEXT,
    PREDICATE_APPEND_HWM_MESSAGE_TEXT,
    PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT,
    PREDICATE_TYPE_NO_MODIFY_HV_EXCEPTION_TEXT,
    PREDICATE_TYPE_NO_MODIFY_RESET_EXCEPTION_TEXT,
    PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT,
    PREDICATE_TYPE_REQUIRED_EXCEPTION_TEXT,
    RANGE_AND_PREDICATE_WITHOUT_PART_KEY_EXCEPTION_TEXT,
)
from goe.offload.predicate_offload import GenericPredicate
from goe.orchestration import command_steps
from goe.orchestration.command_steps import step_title
from goe.persistence.orchestration_metadata import (
    INCREMENTAL_PREDICATE_TYPE_LIST,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE,
    INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_PREDICATE,
    INCREMENTAL_PREDICATE_TYPE_RANGE,
    INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE,
)
from goe.util.misc_functions import add_suffix_in_same_case

from test_sets.stories.story_assertion_functions import (
    check_metadata,
    get_offload_row_count_from_log,
    frontend_column_exists,
    messages_step_executions,
    offload_dim_assertion,
    offload_fact_assertions,
    offload_lpa_fact_assertion,
    text_in_messages,
)
from test_sets.stories.story_globals import (
    OFFLOAD_PATTERN_100_0,
    OFFLOAD_PATTERN_100_10,
    STORY_SETUP_TYPE_FRONTEND,
    STORY_SETUP_TYPE_HYBRID,
    STORY_SETUP_TYPE_ORACLE,
    STORY_SETUP_TYPE_PYTHON,
    STORY_TYPE_LINK_ID,
    STORY_TYPE_OFFLOAD,
    STORY_TYPE_SETUP,
)
from test_sets.stories.story_setup_functions import (
    SALES_BASED_FACT_HV_1,
    SALES_BASED_FACT_HV_2,
    SALES_BASED_FACT_HV_3,
    SALES_BASED_FACT_HV_4,
    drop_backend_test_table,
    gen_hybrid_drop_ddl,
    gen_insert_late_arriving_sales_based_data,
    gen_insert_late_arriving_sales_based_list_data,
    gen_insert_late_arriving_sales_based_multi_pcol_data,
    gen_rdbms_dim_create_ddl,
    gen_sales_based_fact_create_ddl,
    gen_sales_based_list_create_ddl,
    gen_sales_based_multi_pcol_fact_create_ddl,
    gen_sales_based_subpartitioned_fact_ddl,
    get_backend_drop_incr_fns,
)

DIM_TABLE = 'STORY_PBO_DIM'
DIM_TABLE_LATE = 'STORY_PBO_D_LATE'
RANGE_TABLE = 'STORY_PBO_RANGE'
RANGE_TABLE_LATE = 'STORY_PBO_R_LATE'
RANGE_TABLE_INTRA = 'STORY_PBO_R_INTRA'
LAR_TABLE_LATE = 'STORY_PBO_LAR_LATE'
LAR_TABLE_INTRA = 'STORY_PBO_LAR_INTRA'
LIST_TABLE = 'STORY_PBO_LIST'
LIST_TABLE_LATE = 'STORY_PBO_L90_10_LATE'
LIST_100_0_LATE = 'STORY_PBO_L100_0_LATE'
LIST_TABLE_INTRA = 'STORY_PBO_L_INTRA'
MCOL_TABLE_LATE = 'STORY_PBO_MC_LATE'

JOIN_NAME = 'STORY_PBO_PJ'
MJOIN_NAME = 'STORY_PBO_MJ'
UNICODE_TABLE = 'STORY_PBO_UNI'
UCODE_VALUE1 = '\u03a3'
UCODE_VALUE2 = '\u30ad'
CHAR_TABLE = 'STORY_PBO_CHAR'
TS_TABLE = 'STORY_PBO_TS'
RANGE_SP_LATE = 'STORY_PBO_RR_LATE'

OLD_HV_Y = '1970'
OLD_HV_M = '01'
OLD_HV_1 = '1970-01-01'


def gen_simple_unicode_dimension_ddl(options, frontend_api, schema, table_name, unicode_ch1, unicode_ch2) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        subquery = """SELECT 1 AS id, '%s' AS data FROM dual UNION ALL SELECT 2 AS id, '%s' AS data FROM dual""" \
            % (unicode_ch1, unicode_ch2)
    elif options.db_type == DBTYPE_TERADATA:
        subquery = dedent(f"""\
        SELECT id, CAST('{unicode_ch1}' AS VARCHAR(3) CHARACTER SET UNICODE) AS data
        FROM {schema}.generated_ids WHERE id = 1
        UNION ALL
        SELECT id, CAST('{unicode_ch2}' AS VARCHAR(3) CHARACTER SET UNICODE) AS data
        FROM {schema}.generated_ids WHERE id = 2""")
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery)


def gen_char_frontend_ddl(options, frontend_api, schema, table_name) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        subquery = dedent("""\
        SELECT 1 AS id, CAST('a' AS CHAR(3)) AS data FROM dual
        UNION ALL SELECT 2 AS id, CAST('b' AS CHAR(3)) AS data FROM dual""")
    elif options.db_type == DBTYPE_TERADATA:
        subquery = f"""SELECT id, CAST('a' AS CHAR(3)) AS data FROM {schema}.generated_ids WHERE id = 1
        UNION ALL SELECT id, CAST('b' AS CHAR(3)) AS data FROM {schema}.generated_ids WHERE id = 2"""
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery)


def gen_ts_frontend_ddl(frontend_api, options, schema, table_name) -> list:
    if not options:
        return []
    if options.db_type == DBTYPE_ORACLE:
        subquery = dedent("""\
        SELECT 1 AS id, CAST(TIMESTAMP '2020-01-01 12:12:12.0' AS TIMESTAMP(9)) AS ts FROM dual
        UNION ALL
        SELECT 2 AS id, CAST(TIMESTAMP '2020-02-01 21:21:21.0' AS TIMESTAMP(9)) AS ts FROM dual""")
    elif options.db_type == DBTYPE_TERADATA:
        subquery = dedent(f"""\
        SELECT id, TIMESTAMP '2020-01-01 12:12:12.0' AS ts FROM {schema}.generated_ids WHERE id = 1
        UNION ALL
        SELECT id, TIMESTAMP '2020-02-01 21:21:21.0' AS ts FROM {schema}.generated_ids WHERE id = 2""")
    else:
        raise NotImplementedError(f'Unsupported db_type: {options.db_type}')
    return frontend_api.gen_ctas_from_subquery(schema, table_name, subquery)


def check_pbo_metadata(repo_client, hybrid_schema, hybrid_view, number_of_predicates=None, expected_offload_type=None,
                       expected_incremental_key=None, expected_incremental_range=None, expected_predicate_type=None,
                       expected_incremental_hv=None,
                       values_in_predicate_value_metadata=None, values_not_in_predicate_value_metadata=None):
    """ Fetches hybrid metadata and runs checks pertinent to PBO offloads.
    """
    metadata = repo_client.get_offload_metadata(hybrid_schema, hybrid_view)

    if not check_metadata(hybrid_schema, hybrid_view, repo_client, metadata_override=metadata,
                          offload_type=expected_offload_type, incremental_key=expected_incremental_key,
                          incremental_high_value=expected_incremental_hv, incremental_range=expected_incremental_range,
                          incremental_predicate_type=expected_predicate_type):
        log('Failed metadata check')
        return False
    if number_of_predicates is not None:
        if len(metadata.decode_incremental_predicate_values() or []) != number_of_predicates:
            log('Length of decode_incremental_predicate_values(%s) != %s' % (hybrid_view, number_of_predicates))
            return False
    for check_val in (values_in_predicate_value_metadata or []):
        if not any(check_val in _ for _ in metadata.incremental_predicate_value):
            log('Value not found in INCREMENTAL_PREDICATE_VALUE: %s' % check_val)
            return False
    for check_val in (values_not_in_predicate_value_metadata or []):
        if any(check_val in _ for _ in (metadata.incremental_predicate_value or [])):
            log('Value should NOT be in INCREMENTAL_PREDICATE_VALUE: %s' % check_val)
            return False
    return True


def pbo_assertion(options, repo_client, frontend_api, hybrid_schema, table_name, number_of_predicates=None,
                  expected_offload_type=None, expected_incremental_key=None, expected_incremental_range=None,
                  expected_predicate_type=None,
                  values_in_predicate_value_metadata=None, values_not_in_predicate_value_metadata=None,
                  columns_in_cnt_agg_rule=None, columns_not_in_cnt_agg_rule=None):
    assert hybrid_schema and table_name
    if values_in_predicate_value_metadata:
        assert isinstance(values_in_predicate_value_metadata, list)
    if values_not_in_predicate_value_metadata:
        assert isinstance(values_not_in_predicate_value_metadata, list)
    if not options:
        return True

    # Assuming table_name short enough for these not the get hashed
    gen_agg_name = add_suffix_in_same_case(table_name, '_agg')
    cnt_agg_name = add_suffix_in_same_case(table_name, '_cnt_agg')

    metadata_views = [table_name]
    if frontend_api.hybrid_schema_supported():
        metadata_views = [table_name, gen_agg_name, cnt_agg_name]

    for hybrid_view in metadata_views:
        if not check_pbo_metadata(repo_client, hybrid_schema, hybrid_view, number_of_predicates=number_of_predicates,
                                  expected_offload_type=expected_offload_type,
                                  expected_incremental_key=expected_incremental_key,
                                  expected_incremental_range=expected_incremental_range,
                                  expected_predicate_type=expected_predicate_type,
                                  values_in_predicate_value_metadata=values_in_predicate_value_metadata,
                                  values_not_in_predicate_value_metadata=values_not_in_predicate_value_metadata):
            log('check_pbo_metadata(%s) return False' % hybrid_view)
            return False

    if frontend_api.hybrid_schema_supported():
        for check_col in (columns_in_cnt_agg_rule or []):
            if not frontend_column_exists(frontend_api, hybrid_schema, cnt_agg_name, check_col):
                log(f'Column missing from count star rule: {check_col}')
                return False

        for check_col in (columns_not_in_cnt_agg_rule or []):
            if frontend_column_exists(frontend_api, hybrid_schema, cnt_agg_name, check_col):
                log(f'Column incorrectly in count star rule: {check_col}')
                return False

    return True


def check_predicate_counts_match(frontend_api, schema, hybrid_schema, table_name, test_id, where_clause, must_be_gt_zero=True):
    if not frontend_api.hybrid_schema_supported():
        return check_predicate_count_matches_log(frontend_api, schema, table_name, test_id, where_clause)
    app_count = frontend_api.get_table_row_count(schema, table_name, filter_clause=where_clause)
    hyb_count = frontend_api.get_table_row_count(hybrid_schema, table_name, filter_clause=where_clause)
    if app_count != hyb_count:
        log('%s != %s' % (app_count, hyb_count))
        return False
    if must_be_gt_zero and hyb_count == 0:
        log('hybrid count == 0')
        return False
    return True


def check_predicate_count_matches_log(frontend_api, schema, table_name, test_id, where_clause):
    """ Sometimes we can't use the hybrid view for count checks (e.g. when --no-modify-hybrid-view)
        we compare RDBMS count to logged count.
    """
    log('check_predicate_count_matches_log(%s)' % test_id)
    app_count = frontend_api.get_table_row_count(schema, table_name, filter_clause=where_clause)
    offload_count = get_offload_row_count_from_log(test_id)
    if app_count != offload_count:
        log('%s != %s' % (app_count, offload_count))
        return False
    return True


def const_to_date_expr(options, constant):
    if options and options.db_type == DBTYPE_TERADATA:
        return f"DATE '{constant}'"
    else:
        return f"DATE' {constant}'"


def late_dim_filter_clause(options):
    return "time_id BETWEEN {} AND  {}".format(
        const_to_date_expr(options, SALES_BASED_FACT_HV_3),
        const_to_date_expr(options, SALES_BASED_FACT_HV_4)
    )


def offload_pbo_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                            hybrid_cursor, repo_client):
    """ Standard tests for Predicate Based Offload, case 1 from the design:
        https://gluent.atlassian.net/wiki/spaces/DEV/pages/779812884/Predicate-Based+Offload+Design+Notes
    """
    part_key_type = frontend_api.test_type_canonical_date() if frontend_api else None
    return [
        #
        # NON-PARTITIONED TABLE
        #
        {'id': 'offload_pbo_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % DIM_TABLE,
         'narrative': 'Create dimension for PBO testing, gen_rdbms_dim_create_ddl table is based on PRODUCTS',
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, DIM_TABLE,
                                                                               pk_col='prod_id'),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DIM_TABLE),
                   STORY_SETUP_TYPE_PYTHON: get_backend_drop_incr_fns(options, backend_api, data_db, DIM_TABLE)}},
        {'id': 'offload_pbo_dim_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_TABLE,
         'narrative': 'Initial offload with --no-modify-hybrid-view should fail',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'reset_backend_table': True,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Camcorders")'),
                     'offload_predicate_modify_hybrid_view': False},
         'config_override': {'execute': False},
         'expected_exception_string': PREDICATE_TYPE_NO_MODIFY_HV_EXCEPTION_TEXT},
        {'id': 'offload_pbo_dim_fail2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to offload with partition and predicate identification',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'reset_backend_table': True,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Camcorders")'),
                     'older_than_date': '2012-01-01'},
         'config_override': {'execute': False},
         'expected_exception_string': CONFLICTING_DATA_ID_OPTIONS_EXCEPTION_TEXT},
        {'id': 'offload_pbo_dim_fail3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to offload with offload_type FULL and predicate identification',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'reset_backend_table': True,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Camcorders")'),
                     'offload_type': OFFLOAD_TYPE_FULL},
         'config_override': {'execute': False},
         'expected_exception_string': PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT},
        # Badly formatted predicates should be covered by unit tests so we're testing other types of bad predicate
        {'id': 'offload_pbo_dim_fail4',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Unknown column name',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'reset_backend_table': True,
                     'offload_predicate': GenericPredicate('column(prod_not_a_column) = string("Camcorders")')},
         'config_override': {'execute': False},
         'expected_exception_string': 'Unable to resolve column'},
        {'id': 'offload_pbo_dim_fail5',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload No Matching Rows',
         'narrative': 'No matching data',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'reset_backend_table': True,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Not-a-category")')},
         'config_override': {'execute': False},
         'expected_status': False},
        {'id': 'offload_pbo_dim_offload1_verification',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_TABLE,
         'narrative': 'Verification offload of dimension by predicate',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Camcorders")'),
                     'reset_backend_table': True},
         'config_override': {'execute': False}},
        {'id': 'offload_pbo_dim_offload1_run',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_TABLE,
         'narrative': 'Offload 1st string predicate of dimension',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Camcorders")'),
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, DIM_TABLE),
              lambda test: True),
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, DIM_TABLE,
                                         number_of_predicates=1,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=['Camcorders'],
                                         columns_in_cnt_agg_rule=['prod_subcategory']),
              lambda test: True),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, DIM_TABLE,
                                                        'offload_pbo_dim_offload1_run',
                                                        "prod_subcategory = 'Camcorders'"),
              lambda test: True),
             ]},
        {'id': 'offload_pbo_dim_fail6',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_TABLE,
         'narrative': 'Attempt to use --no-modify-hybrid-view in PREDICATE mode is invalid',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'reset_backend_table': True,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Camcorders")'),
                     'offload_predicate_modify_hybrid_view': False},
         'config_override': {'execute': False},
         'expected_exception_string': PREDICATE_TYPE_NO_MODIFY_HV_EXCEPTION_TEXT},
        {'id': 'offload_pbo_dim_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_TABLE,
         'narrative': """Offload 2nd string predicate of dimension.
                         Predicate in hybrid view should be modified by dependent hybrid view logic.""",
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Accessories")')},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, DIM_TABLE,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         number_of_predicates=2,
                                         values_in_predicate_value_metadata=['Camcorders', 'Accessories'],
                                         columns_in_cnt_agg_rule=['prod_subcategory']),
              lambda test: True),
         ]},
        {'id': 'offload_pbo_dim_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Re-offload Predicate',
         'narrative': 'Attempt to re-offload same predicate, will return False',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Accessories")')},
         'expected_status': False},
        {'id': 'offload_pbo_dim_offload4',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Re-offload Predicate in Force Mode',
         'narrative': 'Re-offload same predicate in force mode, should process successfully but move no data',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Accessories")'),
                     'force': True},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, DIM_TABLE,
                                         number_of_predicates=2,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=['Camcorders', 'Accessories'],
                                         columns_in_cnt_agg_rule=['prod_subcategory']),
              lambda test: True),
             (lambda test: messages_step_executions(test.offload_messages, step_title(command_steps.STEP_FINAL_LOAD)),
              lambda test: 0),
         ]},
        {'id': 'offload_pbo_dim_error_rec_drop_hybrid',
         'type': STORY_TYPE_SETUP,
         'title': 'Drop Hybrid Objects',
         'setup': {'hybrid': gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DIM_TABLE)},
         'prereq': lambda: frontend_api.hybrid_schema_supported()},
        {'id': 'offload_pbo_dim_error_rec_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Re-offload Predicate to Fix Hybrid Schema',
         'narrative': 'Re-offload same predicate, should recreate hybrid objects successfully, even in force=False mode, but move no data',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Accessories")'),
                     'force': True},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, DIM_TABLE,
                                         number_of_predicates=2,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=['Camcorders', 'Accessories'],
                                         columns_in_cnt_agg_rule=['prod_subcategory']),
              lambda test: True),
             (lambda test: messages_step_executions(test.offload_messages, step_title(command_steps.STEP_FINAL_LOAD)),
              lambda test: 0),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, DIM_TABLE,
                                                        'offload_pbo_dim_error_rec_offload',
                                                        "prod_subcategory = 'Accessories'"),
              lambda test: True),
         ],
         'prereq': lambda: frontend_api.hybrid_schema_supported()},
        {'id': 'offload_pbo_dim_offload5',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_TABLE,
         'narrative': 'Offload 3rd predicate of dimension but reset hybrid view',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'offload_predicate': GenericPredicate('column(prod_subcategory) = string("Cameras")'),
                     'reset_hybrid_view': True},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, DIM_TABLE,
                                         number_of_predicates=1,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=['Cameras'],
                                         values_not_in_predicate_value_metadata=['Camcorders', 'Accessories'],
                                         columns_in_cnt_agg_rule=['prod_subcategory']),
              lambda test: True),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, DIM_TABLE,
                                                        'offload_pbo_dim_offload5', "prod_subcategory = 'Cameras'"),
              lambda test: True),
         ]},
        {'id': 'offload_pbo_dim_offload6',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % DIM_TABLE,
         'narrative': """Offload 4th predicate using a new column which should be referenced in CNT_AGG rule.
                         Also confirm data with aggregate method""",
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'offload_predicate': GenericPredicate(
                         '(column(prod_subcategory) = string("Monitors")) and (column(PROD_SUBCATEGORY_DESC) = string("Monitors"))'),
                     'verify_row_count': 'aggregate'},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, DIM_TABLE,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=['Cameras', 'Monitors'],
                                         values_not_in_predicate_value_metadata=['Camcorders', 'Accessories'],
                                         columns_in_cnt_agg_rule=['prod_subcategory', 'prod_subcategory_desc']),
              lambda test: True)]},
        {'id': 'offload_pbo_dim_full_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Full %s' % DIM_TABLE,
         'narrative': 'Offload a dimension 100/0',
         'options': {'owner_table': schema + '.' + DIM_TABLE,
                     'offload_type': OFFLOAD_TYPE_FULL,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, DIM_TABLE),
              lambda test: True)]},
        {'id': 'offload_pbo_unicode_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % UNICODE_TABLE,
         'narrative': 'Create dimension for PBO testing with unicode data',
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_simple_unicode_dimension_ddl(options, frontend_api,
                                                                                       schema, UNICODE_TABLE,
                                                                                       UCODE_VALUE1, UCODE_VALUE2),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, UNICODE_TABLE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, UNICODE_TABLE)]}},
        {'id': 'offload_pbo_unicode_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % UNICODE_TABLE,
         'narrative': 'Offload 1st unicode predicate of dimension',
         'options': {'owner_table': schema + '.' + UNICODE_TABLE,
                     'unicode_string_columns_csv': 'data',
                     'offload_predicate': GenericPredicate(
                         '((column(id) = numeric(1)) and (column(data) = string("%s")))' % UCODE_VALUE1),
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, UNICODE_TABLE),
              lambda test: True),
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, UNICODE_TABLE,
                                         number_of_predicates=1,
                                         values_in_predicate_value_metadata=[UCODE_VALUE1]),
              lambda test: True),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, UNICODE_TABLE,
                                                        'offload_pbo_unicode_offload1', "data = '%s'" % UCODE_VALUE1),
              lambda test: True)]},
        {'id': 'offload_pbo_unicode_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % UNICODE_TABLE,
         'narrative': 'Offload 2nd unicode predicate of dimension',
         'options': {'owner_table': schema + '.' + UNICODE_TABLE,
                     'offload_predicate': GenericPredicate('((column(data) = string("%s")))' % UCODE_VALUE2)},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, UNICODE_TABLE),
              lambda test: True),
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, UNICODE_TABLE,
                                         number_of_predicates=2,
                                         values_in_predicate_value_metadata=[UCODE_VALUE2]),
              lambda test: True),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, UNICODE_TABLE,
                                                        'offload_pbo_unicode_offload2', "data = '%s'" % UCODE_VALUE2),
              lambda test: True)]},
        {'id': 'offload_pbo_char_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % CHAR_TABLE,
         'narrative': 'Create dimension for PBO testing with a CHAR padded column.',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_char_frontend_ddl(options, frontend_api, schema, CHAR_TABLE),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, CHAR_TABLE),
                   STORY_SETUP_TYPE_PYTHON: get_backend_drop_incr_fns(options, backend_api, data_db, CHAR_TABLE)}},
        {'id': 'offload_pbo_char_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % CHAR_TABLE,
         'narrative': 'Offload 1st CHAR padded predicate of dimension',
         'options': {'owner_table': schema + '.' + CHAR_TABLE,
                     'offload_predicate': GenericPredicate('(column(data) = string("a  "))'),
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, CHAR_TABLE),
              lambda test: True),
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, CHAR_TABLE,
                                         number_of_predicates=1,
                                         values_in_predicate_value_metadata=['("a  ")']),
              lambda test: True),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, CHAR_TABLE,
                                                        'offload_pbo_char_offload1', "data = 'a  '"),
              lambda test: True)]},
        {'id': 'offload_pbo_char_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'No-op Offload %s' % CHAR_TABLE,
         'narrative': 'Offload 1st CHAR padded predicate again, nothing to offload',
         'options': {'owner_table': schema + '.' + CHAR_TABLE,
                     'offload_predicate': GenericPredicate('(column(data) = string("a  "))')},
         'expected_status': False},
        {'id': 'offload_pbo_ts_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create %s' % TS_TABLE,
         'narrative': 'Create table for PBO testing with a TIMESTAMP column because our SALES based testing is on DATE',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_ts_frontend_ddl(frontend_api, options, schema, TS_TABLE),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, TS_TABLE),
                   STORY_SETUP_TYPE_PYTHON: get_backend_drop_incr_fns(options, backend_api, data_db, TS_TABLE)}},
        {'id': 'offload_pbo_ts_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload %s' % TS_TABLE,
         'narrative': 'Offload 1st TIMESTAMP predicate',
         'options': {'owner_table': schema + '.' + TS_TABLE,
                     'offload_predicate': GenericPredicate('(column(ts) < datetime(2020-02-01))'),
                     'allow_nanosecond_timestamp_columns': True,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, TS_TABLE),
              lambda test: True),
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, TS_TABLE,
                                         number_of_predicates=1,
                                         values_in_predicate_value_metadata=['(2020-02-01)']),
              lambda test: True),
             (lambda test: check_predicate_count_matches_log(frontend_api, schema, TS_TABLE,
                                                             'offload_pbo_ts_offload1', "id = 1"),
              lambda test: True),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, TS_TABLE,
                                                        'offload_pbo_ts_offload1', "id = 1"),
              lambda test: True)]},
        #
        # RANGE PARTITIONED TABLE
        #
        {'id': 'offload_pbo_range_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create RANGE %s' % RANGE_TABLE,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, RANGE_TABLE),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, RANGE_TABLE),
                   STORY_SETUP_TYPE_PYTHON: get_backend_drop_incr_fns(options, backend_api, data_db, RANGE_TABLE)}},
        {'id': 'offload_pbo_range_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st Predicate',
         'narrative': 'Offload 1st datetime predicate of RANGE partitioned table',
         'options': {'owner_table': schema + '.' + RANGE_TABLE,
                     'offload_predicate': GenericPredicate(
                         '(column(time_id) >= datetime(%s)) and (column(time_id) < datetime(%s))'
                         % (SALES_BASED_FACT_HV_2, SALES_BASED_FACT_HV_3)),
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, RANGE_TABLE,
                                         number_of_predicates=1,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=[SALES_BASED_FACT_HV_2,
                                                                             SALES_BASED_FACT_HV_3],
                                         columns_in_cnt_agg_rule=['time_id']),
              lambda test: True),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, RANGE_TABLE,
                                                        'offload_pbo_range_offload1',
                                                        "time_id >= %s and time_id < %s"
                                                        % (const_to_date_expr(options, SALES_BASED_FACT_HV_2),
                                                           const_to_date_expr(options, SALES_BASED_FACT_HV_3))),
              lambda test: True),
         ]},
        {'id': 'offload_pbo_range_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 2nd Predicate',
         'narrative': 'Offload 2nd datetime predicate of RANGE partitioned table',
         'options': {'owner_table': schema + '.' + RANGE_TABLE,
                     'offload_predicate': GenericPredicate('column(time_id) = datetime(%s)' % SALES_BASED_FACT_HV_1)},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, RANGE_TABLE,
                                         number_of_predicates=2,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=[SALES_BASED_FACT_HV_2,
                                                                             SALES_BASED_FACT_HV_3,
                                                                             SALES_BASED_FACT_HV_1]),
              lambda test: True),
         ]},
        {'id': 'offload_pbo_range_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to offload by partition while in PREDICATE mode is not valid',
         'options': {'owner_table': schema + '.' + RANGE_TABLE,
                     'older_than_date': SALES_BASED_FACT_HV_1},
         'expected_exception_string': IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT,
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, RANGE_TABLE,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE),
              lambda test: True),
         ]},
        {'id': 'offload_pbo_range_fail2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to use offload type FULL while in PREDICATE mode is not valid',
         'options': {'owner_table': schema + '.' + RANGE_TABLE,
                     'offload_predicate': GenericPredicate(
                         '(column(time_id) = datetime(%s)) and (column(channel_id) = numeric(3))'
                         % SALES_BASED_FACT_HV_4),
                     'offload_type': OFFLOAD_TYPE_FULL},
         'expected_exception_string': PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT},
        {'id': 'offload_pbo_range_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 3rd Predicate With Extra Column Involved',
         'narrative': 'Offload predicate of RANGE table with extra column involved which we should see in cnt agg',
         'options': {'owner_table': schema + '.' + RANGE_TABLE,
                     'offload_predicate': GenericPredicate(
                         '(column(time_id) = datetime(%s)) and (column(channel_id) = numeric(3))'
                         % SALES_BASED_FACT_HV_4)},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, RANGE_TABLE,
                                         number_of_predicates=3,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=[SALES_BASED_FACT_HV_2,
                                                                             SALES_BASED_FACT_HV_3,
                                                                             SALES_BASED_FACT_HV_1],
                                         columns_in_cnt_agg_rule=['time_id', 'channel_id']),
             lambda test: True),
         ]},
        #
        # LIST PARTITIONED TABLE
        #
        {'id': 'offload_pbo_list_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create RANGE %s' % LIST_TABLE,
         'setup': {STORY_SETUP_TYPE_FRONTEND: lambda: gen_sales_based_list_create_ddl(frontend_api, schema, LIST_TABLE,
                                                                                      part_key_type=part_key_type,
                                                                                      default_partition=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, LIST_TABLE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, LIST_TABLE)]},
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_list_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st LIST Partition',
         'narrative': 'Offload 1st partition putting table in LIST mode',
         'options': {'owner_table': '%s.%s' % (schema, LIST_TABLE),
                     'equal_to_values': [SALES_BASED_FACT_HV_1],
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(
                 test, schema, hybrid_schema, data_db, LIST_TABLE, options, backend_api, frontend_api,
                 repo_client, [SALES_BASED_FACT_HV_1], incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST,
                 incremental_predicate_value='NULL'),
              lambda test: True)],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_list_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Invalid PBO Over LIST IPA',
         'narrative': 'Attempt to Offload 1st predicate over LIST IPA table, this is not valid.',
         'options': {'owner_table': schema + '.' + LIST_TABLE,
                     'offload_predicate': GenericPredicate('(column(yrmon) = datetime(%s))' % (SALES_BASED_FACT_HV_1))},
         'expected_exception_string': PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT,
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_list_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Predicate Over LIST',
         'narrative': 'Offload 1st predicate over LIST table.',
         'options': {'owner_table': schema + '.' + LIST_TABLE,
                     'offload_predicate': GenericPredicate(
                         '((column(yrmon) = datetime(%s)) and (column(channel_id) = numeric(3)))'
                         % (SALES_BASED_FACT_HV_1)),
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, LIST_TABLE,
                                         number_of_predicates=1,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=[SALES_BASED_FACT_HV_1, '(3)'],
                                         columns_in_cnt_agg_rule=['yrmon', 'channel_id']),
              lambda test: True),
             (lambda test: check_predicate_count_matches_log(frontend_api, schema, LIST_TABLE,
                                                             'offload_pbo_list_offload2',
                                                             "yrmon = %s AND channel_id = 3"
                                                             % const_to_date_expr(options, SALES_BASED_FACT_HV_1)),
              lambda test: True),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, LIST_TABLE,
                                                        'offload_pbo_list_offload2',
                                                        "yrmon = %s AND channel_id = 3"
                                                        % const_to_date_expr(options, SALES_BASED_FACT_HV_1)),
              lambda test: True)],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_list_fail2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Attempt LIST IPA Over PREDICATE',
         'narrative': 'Attempt to offload partition from LIST table while already in PREDICATE mode',
         'options': {'owner_table': '%s.%s' % (schema, LIST_TABLE),
                     'equal_to_values': [SALES_BASED_FACT_HV_1]},
         'expected_exception_string': IPA_PREDICATE_TYPE_REQUIRES_PREDICATE_EXCEPTION_TEXT,
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_list_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Predicate Over LIST',
         'narrative': 'Offload 2nd predicate over LIST table.',
         'options': {'owner_table': schema + '.' + LIST_TABLE,
                     'offload_predicate': GenericPredicate(
                         '((column(yrmon) = datetime(%s)) and (column(channel_id) = numeric(4)))'
                         % (SALES_BASED_FACT_HV_1))},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, LIST_TABLE,
                                         number_of_predicates=2,
                                         expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type=INCREMENTAL_PREDICATE_TYPE_PREDICATE,
                                         values_in_predicate_value_metadata=[SALES_BASED_FACT_HV_1, '(4)'],
                                         columns_in_cnt_agg_rule=['yrmon', 'channel_id']),
              lambda test: True),
             (lambda test: check_predicate_count_matches_log(frontend_api, schema, LIST_TABLE,
                                                             'offload_pbo_list_offload3',
                                                             "yrmon = %s AND channel_id = 4"
                                                             % const_to_date_expr(options, SALES_BASED_FACT_HV_1)),
              lambda test: True),
             (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, LIST_TABLE,
                                                        'offload_pbo_list_offload3',
                                                        "yrmon = %s AND channel_id = 4"
                                                        % const_to_date_expr(options, SALES_BASED_FACT_HV_1)),
              lambda test: True)],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        ]


def offload_pbo_late_100_x_tests(options, backend_api, frontend_api, repo_client, schema, hybrid_schema,
                                 data_db, table_name, offload_pattern):
    """ Tests for testing 100/0 and 100/10 which were similar enough to share the config
    """
    assert offload_pattern in (OFFLOAD_PATTERN_100_0, OFFLOAD_PATTERN_100_10)
    assert table_name in (RANGE_TABLE_LATE, LAR_TABLE_LATE)

    part_key_type = frontend_api.test_type_canonical_date() if frontend_api else None
    if table_name == RANGE_TABLE_LATE:
        inc_key = 'TIME_ID'
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        add_rows_fn = lambda: gen_insert_late_arriving_sales_based_data(frontend_api, schema, table_name, OLD_HV_1)
        setup_link = 'offload_pbo_late_range_setup'
        if offload_pattern == OFFLOAD_PATTERN_100_0:
            test_id = 'range_100_0'
            hv_1 = chk_hv_1 = None
        else:
            test_id = 'range_100_10'
            hv_1 = chk_hv_1 = SALES_BASED_FACT_HV_2
    elif table_name == LAR_TABLE_LATE:
        if options and options.db_type == DBTYPE_TERADATA:
            log('Skipping LAR tests on Teradata because CASE_N is not yet supported')
            return []
        inc_key = 'YRMON'
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE
        add_rows_fn = lambda: gen_insert_late_arriving_sales_based_list_data(frontend_api, schema, LAR_TABLE_LATE, OLD_HV_1,
                                                                             SALES_BASED_FACT_HV_1)
        setup_link = 'offload_pbo_late_lar_setup'
        if offload_pattern == OFFLOAD_PATTERN_100_0:
            test_id = 'lar_100_0'
            hv_1 = chk_hv_1 = None
        else:
            test_id = 'lar_100_10'
            hv_1 = SALES_BASED_FACT_HV_2
            chk_hv_1 = SALES_BASED_FACT_HV_1
    else:
        raise NotImplementedError(f'Test table not implemented: {table_name}')

    return [{'id': 'offload_pbo_late_%s_setup' % test_id,
             'type': STORY_TYPE_LINK_ID,
             'linked_id': setup_link},
            {'id': 'offload_pbo_late_%s_offload1' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'Offload Full %s' % test_id,
             'narrative': 'Offload partitioned table as 100/x',
             'options': {'owner_table': schema + '.' + table_name,
                         'older_than_date': hv_1,
                         'offload_type': OFFLOAD_TYPE_FULL,
                         'ipa_predicate_type': ipa_predicate_type,
                         'reset_backend_table': True},
             'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                        schema, hybrid_schema, data_db, table_name, chk_hv_1,
                                                        offload_pattern=offload_pattern,
                                                        incremental_key=inc_key, incremental_key_type=part_key_type,
                                                        ipa_predicate_type=ipa_predicate_type)},
            {'id': 'offload_pbo_late_%s_add_rows' % test_id,
             'type': STORY_TYPE_SETUP,
             'title': 'Add Late Arriving Data',
             'narrative': 'Add late arriving data below the HWM',
             'setup': {STORY_SETUP_TYPE_ORACLE: add_rows_fn()},
             'assertion_pairs': [
                 (lambda test: bool(frontend_api.get_table_row_count(schema, table_name,
                                                                     filter_clause="time_id = %s"
                                                                     % const_to_date_expr(options, OLD_HV_1)) > 0),
                  lambda test: True)]},
            {'id': 'offload_pbo_late_%s_fail1' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'Attempt to Switch to INCREMENTAL During LAPBO Offload',
             'options': {'owner_table': schema + '.' + table_name,
                         'offload_predicate': GenericPredicate('column(time_id) = datetime(%s)' % OLD_HV_1),
                         'ipa_predicate_type': ipa_predicate_type,
                         'offload_type': OFFLOAD_TYPE_INCREMENTAL},
             'expected_exception_string': OFFLOAD_TYPE_CHANGE_FOR_PBO_EXCEPTION_TEXT},
            {'id': 'offload_pbo_late_%s_offload2' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'Offload Late Arriving Data',
             'narrative': 'Late arriving data should be invisible as far as metadata and hybrid view is concerned',
             'options': {'owner_table': schema + '.' + table_name,
                         'offload_predicate': GenericPredicate('column(time_id) = datetime(%s)' % OLD_HV_1),
                         'ipa_predicate_type': ipa_predicate_type},
             'assertion_pairs':
                 offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                         hybrid_schema, data_db, table_name, chk_hv_1, offload_pattern=offload_pattern,
                                         incremental_key=inc_key, incremental_key_type=part_key_type,
                                         ipa_predicate_type=ipa_predicate_type, incremental_predicate_value='NULL') + [
                     (lambda test: check_predicate_count_matches_log(frontend_api, schema, table_name,
                                                                     'offload_pbo_late_%s_offload2' % test_id,
                                                                     "time_id = %s"
                                                                     % const_to_date_expr(options, OLD_HV_1)),
                      lambda test: True),
                     (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, table_name,
                                                                'offload_pbo_late_%s_offload2' % test_id,
                                                                "time_id = %s"
                                                                % const_to_date_expr(options, OLD_HV_1)),
                      lambda test: True),
                 ]},
            {'id': 'offload_pbo_late_%s_offload3' % test_id,
             'type': STORY_TYPE_OFFLOAD,
             'title': 'Re-Offload Same Predicate',
             'narrative': 'Attempt to re-offload same predicate',
             'options': {'owner_table': schema + '.' + table_name,
                         'offload_predicate': GenericPredicate('column(time_id) = datetime(%s)' % OLD_HV_1),
                         'ipa_predicate_type': ipa_predicate_type},
             'expected_status': False}]


def offload_pbo_late_arriving_std_range_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                              repo_client, table_name):
    assert table_name in (RANGE_TABLE_LATE, LAR_TABLE_LATE, MCOL_TABLE_LATE, RANGE_SP_LATE)

    offload_partition_granularity = None
    less_than_option = 'older_than_date'
    fail1_hv_pred = '(column(time_id) = datetime(%s))' % (SALES_BASED_FACT_HV_1)
    chk_cnt_filter = "time_id = %s" % const_to_date_expr(options, OLD_HV_1)
    check_hwm_in_hybrid_view = True
    expected_incremental_range = None
    offload_by_subpartition = False
    offload_partition_columns = None
    part_key_type = frontend_api.test_type_canonical_date() if frontend_api else None
    if table_name == RANGE_TABLE_LATE:
        setup_fn = lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, table_name,
                                                           range_start_literal_override=OLD_HV_1)
        test_id = 'range'
        inc_key = 'TIME_ID'
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        hv_1 = chk_hv_1 = SALES_BASED_FACT_HV_1
        hv_pred = '(column(time_id) = datetime(%s))' % OLD_HV_1
        add_row_fn = lambda: gen_insert_late_arriving_sales_based_data(frontend_api, schema, table_name, OLD_HV_1)
    elif table_name == LAR_TABLE_LATE:
        if options and options.db_type == DBTYPE_TERADATA:
            log('Skipping LAR tests on Teradata because CASE_N is not yet supported')
            return []
        setup_fn = lambda: gen_sales_based_list_create_ddl(frontend_api, schema, table_name,
                                                           part_key_type=part_key_type)
        test_id = 'lar'
        inc_key = 'YRMON'
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE
        chk_hv_1 = SALES_BASED_FACT_HV_1
        hv_1 = SALES_BASED_FACT_HV_2
        hv_pred = '(column(yrmon) = datetime(%s)) and (column(time_id) = datetime(%s))' % (SALES_BASED_FACT_HV_1, OLD_HV_1)
        add_row_fn = lambda: gen_insert_late_arriving_sales_based_list_data(frontend_api, schema, table_name, OLD_HV_1,
                                                                            SALES_BASED_FACT_HV_1)
    elif table_name == MCOL_TABLE_LATE:
        if options and options.db_type == DBTYPE_TERADATA:
            log('Skipping multi-column tests on Teradata because we currently only support RANGE with a single column')
            return []
        setup_fn = lambda: gen_sales_based_multi_pcol_fact_create_ddl(schema, table_name)
        test_id = 'mcol'
        inc_key = 'TIME_YEAR, TIME_MONTH, TIME_ID'
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        hv_1 = chk_hv_1 = '2012,12,2012-12-01'
        offload_partition_granularity = '1,1,Y'
        less_than_option = 'less_than_value'
        check_hwm_in_hybrid_view = False
        hv_pred = '(column(time_id) = datetime(%s))' % OLD_HV_1
        add_row_fn = lambda: gen_insert_late_arriving_sales_based_multi_pcol_data(schema, table_name, OLD_HV_1)
        if options and options.target == DBTYPE_BIGQUERY:
            offload_partition_columns = 'TIME_ID'
            offload_partition_granularity = None
    elif table_name == RANGE_SP_LATE:
        if options and options.db_type == DBTYPE_TERADATA:
            log('Skipping subpartition tests on Teradata because we currently only support RANGE at top level')
            return []
        setup_fn = lambda: gen_sales_based_subpartitioned_fact_ddl(schema, table_name)
        test_id = 'subpart'
        inc_key = 'TIME_ID'
        offload_by_subpartition = True
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        hv_1 = chk_hv_1 = SALES_BASED_FACT_HV_1
        hv_pred = '(column(time_id) = datetime(%s))' % OLD_HV_1
        add_row_fn = lambda: gen_insert_late_arriving_sales_based_data(frontend_api, schema, table_name, OLD_HV_1)
        expected_incremental_range = 'SUBPARTITION'
    else:
        raise NotImplementedError(f'Test table not implemented: {table_name}')

    return [
        {'id': 'offload_pbo_late_%s_setup' % test_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Create RANGE %s' % table_name,
         'setup': {STORY_SETUP_TYPE_ORACLE: setup_fn(),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, table_name),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, table_name)]}},
        {'id': 'offload_pbo_late_%s_fail1' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to offload for first time with predicate and "range" predicate type',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(fail1_hv_pred),
                     'ipa_predicate_type': ipa_predicate_type,
                     'offload_by_subpartition': offload_by_subpartition,
                     'reset_backend_table': True},
         'expected_exception_string': IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT},
        {'id': 'offload_pbo_late_%s_offload1' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st Partition',
         'narrative': 'Offload 1st partition putting table in "range" mode',
         'options': {'owner_table': schema + '.' + table_name,
                     less_than_option: hv_1,
                     'offload_by_subpartition': offload_by_subpartition,
                     'offload_partition_columns': offload_partition_columns,
                     'offload_partition_granularity': offload_partition_granularity,
                     'reset_backend_table': True},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, table_name, chk_hv_1, incremental_key=inc_key,
                                     incremental_key_type=part_key_type,
                                     ipa_predicate_type=ipa_predicate_type,
                                     incremental_predicate_value='NULL',
                                     incremental_range=expected_incremental_range,
                                     check_hwm_in_hybrid_view=check_hwm_in_hybrid_view) + [
                 (lambda test: (frontend_api.get_table_row_count(hybrid_schema, table_name,
                                                                filter_clause=chk_cnt_filter)
                                if frontend_api.hybrid_schema_supported() else 0),
                  lambda test: 0)
             ]},
        {'id': 'offload_pbo_late_%s_offload2' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'No-op Offload Late Arriving Data',
         'narrative': 'Offload by predicate but when there isn\'t anything to offload yet',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(hv_pred),
                     'ipa_predicate_type': ipa_predicate_type},
         'expected_status': False},
        {'id': 'offload_pbo_late_%s_add_rows' % test_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Add Late Arriving Data',
         'narrative': 'Add late arriving data below the HWM',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: add_row_fn()},
         'assertion_pairs': [
             (lambda test: bool(frontend_api.get_table_row_count(schema, table_name,
                                                                 filter_clause=chk_cnt_filter) > 0),
              lambda test: True)]},
        {'id': 'offload_pbo_late_%s_fail2' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt offload late arriving predicate with --reset-hybrid-view, fails',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(hv_pred),
                     'reset_hybrid_view': True,
                     'ipa_predicate_type': ipa_predicate_type},
         'expected_exception_string': PREDICATE_TYPE_NO_MODIFY_RESET_EXCEPTION_TEXT},
        {'id': 'offload_pbo_late_%s_fail3' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to subvert RANGE with late arriving predicate and --offload-predicate-type=LIST_AS_RANGE',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(hv_pred),
                     'ipa_predicate_type': INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE},
         'prereq': bool(table_name in (RANGE_TABLE_LATE, RANGE_SP_LATE)),
         'expected_exception_string': IPA_PREDICATE_TYPE_EXCEPTION_TEXT},
        {'id': 'offload_pbo_late_%s_fail4' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to subvert LIST_AS_RANGE with late arriving predicate and --offload-predicate-type=LIST',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(hv_pred),
                     'ipa_predicate_type': INCREMENTAL_PREDICATE_TYPE_LIST},
         'prereq': bool(table_name == LAR_TABLE_LATE),
         'expected_exception_string': IPA_PREDICATE_TYPE_CHANGE_EXCEPTION_TEXT},
        {'id': 'offload_pbo_late_%s_fail5' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to subvert "range" config with --offload-predicate-type=PREDICATE',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(hv_pred),
                     'ipa_predicate_type': INCREMENTAL_PREDICATE_TYPE_PREDICATE},
         'expected_exception_string': 'is not valid for existing'},
        {'id': 'offload_pbo_late_%s_offload3' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Late Arriving Data',
         'narrative': 'Late arriving data should be invisible as far as metadata and hybrid view is concerned',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(hv_pred),
                     'ipa_predicate_type': ipa_predicate_type},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, table_name, chk_hv_1, incremental_key=inc_key,
                                     incremental_key_type=part_key_type,
                                     ipa_predicate_type=ipa_predicate_type,
                                     incremental_predicate_value='NULL',
                                     check_hwm_in_hybrid_view=check_hwm_in_hybrid_view) + [
                 (lambda test: check_predicate_count_matches_log(frontend_api, schema, table_name,
                                                                 'offload_pbo_late_%s_offload3' % test_id,
                                                                 chk_cnt_filter),
                  lambda test: True),
                 (lambda test: text_in_messages(test.offload_messages, PREDICATE_APPEND_HWM_MESSAGE_TEXT),
                  lambda test: True),
             ]},
        {'id': 'offload_pbo_late_%s_offload4' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Re-Offload Same Predicate',
         'narrative': 'Attempt to re-offload same predicate',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(hv_pred),
                     'ipa_predicate_type': ipa_predicate_type},
         'expected_status': False},
    ]


def offload_pbo_late_arriving_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                          repo_client):
    """ Tests for Late Arriving Predicate Based Offload, case 2 from the design:
        https://gluent.atlassian.net/wiki/spaces/DEV/pages/779812884/Predicate-Based+Offload+Design+Notes
    """
    part_key_type = frontend_api.test_type_canonical_date() if frontend_api else None
    return offload_pbo_late_arriving_std_range_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                                     repo_client, RANGE_TABLE_LATE) + \
           offload_pbo_late_100_x_tests(options, backend_api, frontend_api, repo_client, schema,
                                        hybrid_schema, data_db, RANGE_TABLE_LATE, OFFLOAD_PATTERN_100_0) + \
           offload_pbo_late_100_x_tests(options, backend_api, frontend_api, repo_client, schema,
                                        hybrid_schema, data_db, RANGE_TABLE_LATE, OFFLOAD_PATTERN_100_10) + \
           offload_pbo_late_arriving_std_range_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                                     repo_client, LAR_TABLE_LATE) + \
           offload_pbo_late_100_x_tests(options, backend_api, frontend_api, repo_client, schema,
                                        hybrid_schema, data_db, LAR_TABLE_LATE, OFFLOAD_PATTERN_100_0) + \
           offload_pbo_late_100_x_tests(options, backend_api, frontend_api, repo_client, schema,
                                        hybrid_schema, data_db, LAR_TABLE_LATE, OFFLOAD_PATTERN_100_10) + \
           offload_pbo_late_arriving_std_range_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                                     repo_client, MCOL_TABLE_LATE) + \
           offload_pbo_late_arriving_std_range_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                                     repo_client, RANGE_SP_LATE) + \
           [
        #
        # LIST-PARTITIONED 100/0 TABLE
        #
        {'id': 'offload_pbo_late_list_100_0_setup',
         'type': STORY_TYPE_SETUP,
         'title': f'Create LIST {LIST_100_0_LATE}',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_sales_based_list_create_ddl(frontend_api, schema,
                                                                                    LIST_100_0_LATE,
                                                                                    part_key_type=part_key_type,
                                                                                    default_partition=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, LIST_100_0_LATE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, LIST_100_0_LATE)]},
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_late_list_100_0_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': f'Offload {LIST_100_0_LATE}',
         'narrative': 'Offload list table 100/0',
         'options': {'owner_table': schema + '.' + LIST_100_0_LATE,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(test, schema, hybrid_schema, data_db, LIST_100_0_LATE,
                                                      options, backend_api, frontend_api, repo_client,
                                                      None, check_rowcount=True,
                                                      offload_pattern=OFFLOAD_PATTERN_100_0,
                                                      incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST),
              lambda test: True),
         ],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_late_list_100_0_add_rows',
         'type': STORY_TYPE_SETUP,
         'title': 'Add Late Arriving Data to FULL LIST Table',
         'setup': {STORY_SETUP_TYPE_ORACLE:
             lambda: gen_insert_late_arriving_sales_based_list_data(frontend_api, schema, LIST_100_0_LATE, OLD_HV_1,
                                                                    SALES_BASED_FACT_HV_1)},
         'assertion_pairs': [
             (lambda test: bool(frontend_api.get_table_row_count(schema, LIST_100_0_LATE,
                                                                 filter_clause="time_id = %s"
                                                                 % const_to_date_expr(options, OLD_HV_1)) > 0),
              lambda test: True)],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_late_list_100_0_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': f'Offload {DIM_TABLE_LATE}',
         'narrative': 'Offload predicate is invalid with LIST',
         'options': {'owner_table': schema + '.' + LIST_100_0_LATE,
                     'offload_predicate': GenericPredicate(f'column(time_id) = datetime({OLD_HV_1})'),
                     'offload_predicate_modify_hybrid_view': False,
                     'ipa_predicate_type': INCREMENTAL_PREDICATE_TYPE_LIST},
         'assertion_pairs': [
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, LIST_100_0_LATE,
                                         number_of_predicates=0,
                                         expected_offload_type=OFFLOAD_TYPE_FULL,
                                         expected_incremental_key='NULL',
                                         values_not_in_predicate_value_metadata=[OLD_HV_1],
                                         columns_not_in_cnt_agg_rule=['time_id']),
               lambda test: True),
              (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, LIST_100_0_LATE,
                                                         'offload_pbo_late_list_100_0_offload2',
                                                         "time_id = %s"
                                                         % const_to_date_expr(options, OLD_HV_1)),
               lambda test: True)
         ],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        #
        # LIST-PARTITIONED 90/10 TABLE
        #
        {'id': 'offload_pbo_late_list_90_10_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create LIST %s' % LIST_TABLE_LATE,
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_sales_based_list_create_ddl(frontend_api, schema,
                                                                                    LIST_TABLE_LATE,
                                                                                    part_key_type=part_key_type,
                                                                                    default_partition=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, LIST_TABLE_LATE),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, LIST_TABLE_LATE)]},
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_late_list_90_10_offload',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st LIST Partition',
         'narrative': 'Offload 1st partition putting table in LIST mode',
         'options': {'owner_table': '%s.%s' % (schema, LIST_TABLE_LATE),
                     'equal_to_values': [SALES_BASED_FACT_HV_1],
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(test, schema, hybrid_schema, data_db, LIST_TABLE_LATE,
                                                      options, backend_api, frontend_api, repo_client,
                                                      [SALES_BASED_FACT_HV_1],
                                                      incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST),
              lambda test: True)],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_late_list_90_10_add_rows',
         'type': STORY_TYPE_SETUP,
         'title': 'Add Late Arriving Data to LIST Table',
         'setup': {STORY_SETUP_TYPE_ORACLE:
             lambda: gen_insert_late_arriving_sales_based_list_data(frontend_api, schema, LIST_TABLE_LATE, OLD_HV_1,
                                                                    SALES_BASED_FACT_HV_1)},
         'assertion_pairs': [
             (lambda test: bool(frontend_api.get_table_row_count(schema, LIST_TABLE_LATE,
                                                                 filter_clause=f"time_id = %s"
                                                                 % const_to_date_expr(options, OLD_HV_1)) > 0),
              lambda test: True)],
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        {'id': 'offload_pbo_late_list_90_10_fail',
         'type': STORY_TYPE_OFFLOAD,
         'title': f'Offload {DIM_TABLE_LATE}',
         'narrative': 'Offload predicate is invalid with LIST',
         'options': {'owner_table': schema + '.' + LIST_TABLE_LATE,
                     'offload_predicate': GenericPredicate(f'column(time_id) = datetime({OLD_HV_1})'),
                     'offload_predicate_modify_hybrid_view': False},
         'expected_exception_string': PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT,
         'prereq': lambda: frontend_api.gluent_lpa_supported()},
        #
        # NON-PARTITIONED TABLE
        #
        {'id': 'offload_pbo_late_dim_setup',
         'type': STORY_TYPE_SETUP,
         'title': f'Create {DIM_TABLE_LATE}',
         'narrative': 'Create dimension for 100/0 Late PBO testing',
         'setup': {STORY_SETUP_TYPE_ORACLE: lambda: gen_rdbms_dim_create_ddl(frontend_api, schema, DIM_TABLE_LATE,
                                                                             source='SALES',
                                                                             filter_clause=late_dim_filter_clause(options)),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, DIM_TABLE_LATE),
                   STORY_SETUP_TYPE_PYTHON: get_backend_drop_incr_fns(options, backend_api, data_db, DIM_TABLE_LATE)}},
        {'id': 'offload_pbo_late_dim_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': f'Offload {DIM_TABLE_LATE}',
         'narrative': 'Offload 100/0 dimension',
         'options': {'owner_table': schema + '.' + DIM_TABLE_LATE,
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, DIM_TABLE_LATE),
              lambda test: True),
             ]},
        {'id': 'offload_pbo_late_dim_offload_noop1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'No-op Offload Late Arriving 100/0 Data',
         'narrative': 'Offload by predicate but when there isn\'t anything to offload yet',
         'options': {'owner_table': schema + '.' + DIM_TABLE_LATE,
                     'offload_predicate': GenericPredicate(f'column(time_id) < datetime({SALES_BASED_FACT_HV_3})')},
         'expected_status': False},
        {'id': 'offload_pbo_late_dim_add_rows1',
         'type': STORY_TYPE_SETUP,
         'title': 'Add Late Arriving Data to Dimension',
         'narrative': 'Add late arriving data to already offloaded 100/0 table',
         'setup': {STORY_SETUP_TYPE_ORACLE:
             lambda: gen_insert_late_arriving_sales_based_data(frontend_api, schema, DIM_TABLE_LATE, SALES_BASED_FACT_HV_1)},
         'assertion_pairs': [
             (lambda test: bool(frontend_api.get_table_row_count(schema, DIM_TABLE_LATE,
                                                                 filter_clause="time_id = %s"
                                                                 % const_to_date_expr(options, SALES_BASED_FACT_HV_1)) > 0),
              lambda test: True)]},
        {'id': 'offload_pbo_late_dim_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': f'Offload {DIM_TABLE_LATE}',
         'narrative': 'Offload 2nd date from dimension, includes --no-modify-hybrid-view',
         'options': {'owner_table': schema + '.' + DIM_TABLE_LATE,
                     'offload_predicate': GenericPredicate(f'column(time_id) < datetime({SALES_BASED_FACT_HV_3})'),
                     'offload_predicate_modify_hybrid_view': False},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, DIM_TABLE_LATE,
                                                 run_minus_row_count=False),
              lambda test: True),
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, DIM_TABLE_LATE,
                                         number_of_predicates=0,
                                         expected_offload_type=OFFLOAD_TYPE_FULL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type='NULL',
                                         values_not_in_predicate_value_metadata=[SALES_BASED_FACT_HV_3],
                                         columns_not_in_cnt_agg_rule=['time_id']),
               lambda test: True),
              (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, DIM_TABLE_LATE,
                                                         'offload_pbo_late_dim_offload2',
                                                         "time_id < %s"
                                                         % const_to_date_expr(options, SALES_BASED_FACT_HV_3)),
               lambda test: True),
             ]},
        {'id': 'offload_pbo_late_dim_add_rows2',
         'type': STORY_TYPE_SETUP,
         'title': 'Add Late Arriving Data to Dimension',
         'narrative': 'Add late arriving data to already offloaded 100/0 table',
         'setup': {STORY_SETUP_TYPE_ORACLE:
             lambda: gen_insert_late_arriving_sales_based_data(frontend_api, schema, DIM_TABLE_LATE, SALES_BASED_FACT_HV_2)},
         'assertion_pairs': [
             (lambda test: bool(frontend_api.get_table_row_count(schema, DIM_TABLE_LATE,
                                                                 filter_clause=f"time_id = %s"
                                                                 % const_to_date_expr(options, SALES_BASED_FACT_HV_2)) > 0),
              lambda test: True)]},
        {'id': 'offload_pbo_late_dim_offload_noop2',
         'type': STORY_TYPE_OFFLOAD,
         'title': f'Attempt Overlapping Offload of {DIM_TABLE_LATE}',
         'narrative': 'Attempt to offload overlapping predicate - should fail',
         'options': {'owner_table': schema + '.' + DIM_TABLE_LATE,
                     'offload_predicate': GenericPredicate(f'column(time_id) < datetime({SALES_BASED_FACT_HV_3})')},
         'expected_status': False},
        {'id': 'offload_pbo_late_dim_offload3',
         'type': STORY_TYPE_OFFLOAD,
         'title': f'Offload {DIM_TABLE_LATE}',
         'narrative': 'Offload 2nd date from dimension, without --no-modify-hybrid-view (should be set by default)',
         'options': {'owner_table': schema + '.' + DIM_TABLE_LATE,
                     'offload_predicate': GenericPredicate(f'column(time_id) = datetime({SALES_BASED_FACT_HV_2})')},
         'assertion_pairs': [
             (lambda test: offload_dim_assertion(test, options, backend_api, frontend_api, repo_client,
                                                 schema, hybrid_schema, data_db, DIM_TABLE_LATE,
                                                 run_minus_row_count=False),
              lambda test: True),
             (lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, DIM_TABLE_LATE,
                                         number_of_predicates=0,
                                         expected_offload_type=OFFLOAD_TYPE_FULL,
                                         expected_incremental_key='NULL',
                                         expected_incremental_range='NULL',
                                         expected_predicate_type='NULL',
                                         values_not_in_predicate_value_metadata=[SALES_BASED_FACT_HV_2],
                                         columns_not_in_cnt_agg_rule=['time_id']),
               lambda test: True),
              (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, DIM_TABLE_LATE,
                                                         'offload_pbo_late_dim_offload3',
                                                         "time_id = %s"
                                                         % const_to_date_expr(options, SALES_BASED_FACT_HV_2)),
               lambda test: True),
             ]},
    ]


def offload_pbo_intra_day_std_range_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                          repo_client, hybrid_cursor, table_name):
    def gen_pred(hv_template, hv_1, hv_2, channel_id):
        date_part = hv_template % {'hv_1': hv_1, 'hv_2': hv_2}
        if isinstance(channel_id, (list, tuple)):
            channel_part = '(column(channel_id) in (%s))' % ','.join('numeric({})'.format(_) for _ in channel_id)
        else:
            channel_part = '(column(channel_id) = numeric(%s))' % channel_id
        return date_part + ' and' + channel_part

    assert table_name in (RANGE_TABLE_INTRA, LAR_TABLE_INTRA)

    part_key_type = frontend_api.test_type_canonical_date() if frontend_api else None
    if table_name == RANGE_TABLE_INTRA:
        setup_fn = lambda: gen_sales_based_fact_create_ddl(frontend_api, schema, table_name)
        test_id = 'range'
        inc_key = 'TIME_ID'
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE
        ipa_and_predicate_type = INCREMENTAL_PREDICATE_TYPE_RANGE_AND_PREDICATE
        hv_1 = chk_hv_1 = SALES_BASED_FACT_HV_1
        hv_2 = chk_hv_2 = SALES_BASED_FACT_HV_2
        hv_pred = '((column(time_id) >= datetime(%(hv_1)s)) and (column(time_id) < datetime(%(hv_2)s)))'
        chk_cnt_filter = "time_id >= DATE' %(hv_1)s' AND time_id < DATE' %(hv_2)s' AND channel_id = %(channel)s"
        metadata_chk_hvs = [hv_1, hv_2]
    elif table_name == LAR_TABLE_INTRA:
        if options and options.db_type == DBTYPE_TERADATA:
            log('Skipping LAR tests on Teradata because CASE_N is not yet supported')
            return []
        setup_fn = lambda: gen_sales_based_list_create_ddl(frontend_api, schema, table_name,
                                                           part_key_type=part_key_type)
        test_id = 'lar'
        inc_key = 'YRMON'
        ipa_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE
        ipa_and_predicate_type = INCREMENTAL_PREDICATE_TYPE_LIST_AS_RANGE_AND_PREDICATE
        chk_hv_1 = SALES_BASED_FACT_HV_1
        hv_1 = chk_hv_2 = SALES_BASED_FACT_HV_2
        hv_2 = chk_hv_3 = SALES_BASED_FACT_HV_3
        hv_pred = '(column(yrmon) = datetime(%(hv_2)s))'
        chk_cnt_filter = "yrmon = DATE' %(hv_2)s' AND channel_id = %(channel)s"
        metadata_chk_hvs = [hv_2]

    return [
        {'id': 'offload_pbo_intra_%s_setup' % test_id,
         'type': STORY_TYPE_SETUP,
         'title': 'Create RANGE %s' % table_name,
         'setup': {STORY_SETUP_TYPE_ORACLE: setup_fn(),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, table_name),
                   STORY_SETUP_TYPE_PYTHON: get_backend_drop_incr_fns(options, backend_api, data_db, table_name)}},
        {'id': 'offload_pbo_intra_%s_fail1' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to offload for first time with predicate and predicate type ..._AND_PREDICATE',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, '2')),
                     'ipa_predicate_type': ipa_and_predicate_type,
                     'reset_backend_table': True},
         'expected_exception_string': IPA_PREDICATE_TYPE_FIRST_OFFLOAD_EXCEPTION_TEXT},
        {'id': 'offload_pbo_intra_%s_offload1' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st Partition',
         'narrative': 'Offload 1st partition putting table in "range" mode',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': hv_1,
                     'reset_backend_table': True},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db, table_name, chk_hv_1,
                                                    incremental_key=inc_key,
                                                    incremental_key_type=part_key_type,
                                                    incremental_predicate_value='NULL',
                                                    ipa_predicate_type=ipa_predicate_type)},
        {'id': 'offload_pbo_intra_%s_fail2' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempts to use a predicate without supplying the partition column which is invalid',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate('(column(channel_id) = numeric(2))'),
                     'ipa_predicate_type': ipa_and_predicate_type},
         'expected_exception_string': RANGE_AND_PREDICATE_WITHOUT_PART_KEY_EXCEPTION_TEXT},
        {'id': 'offload_pbo_intra_%s_fail3' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempts to use a predicate without supplying the partition column which is invalid',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, '2'))},
         'expected_exception_string': PREDICATE_TYPE_REQUIRED_EXCEPTION_TEXT},
        {'id': 'offload_pbo_intra_%s_offload2' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st Predicate (Verification)',
         'narrative': """Offload 1st predicate on top of HV_1 and explicitly state predicate type ..._AND_PREDICATE.
                         Runs in verification mode so we don't have to re-do prior step to test without pred type.""",
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, '2')),
                     'ipa_predicate_type': ipa_and_predicate_type},
         'config_override': {'execute': False}},
        {'id': 'offload_pbo_intra_%s_offload3' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st Predicate',
         'narrative': 'Offload 1st predicate on top of HV_1 which will switch table to ..._AND_PREDICATE',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, '2')),
                     'ipa_predicate_type': ipa_and_predicate_type},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, table_name, chk_hv_1, check_rowcount=False,
                                     incremental_key=inc_key, incremental_key_type=part_key_type,
                                     ipa_predicate_type=ipa_and_predicate_type) +
             [(lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, table_name,
                                          number_of_predicates=1,
                                          expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                          expected_incremental_range='PARTITION',
                                          values_in_predicate_value_metadata=metadata_chk_hvs + ['(2)'],
                                          columns_in_cnt_agg_rule=[inc_key, 'channel_id']),
               lambda test: True),
              (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, table_name,
                                                         'offload_pbo_intra_%s_offload3' % test_id,
                                                         chk_cnt_filter % {'hv_1': hv_1, 'hv_2': hv_2, 'channel': '2'}),
               lambda test: True),
              ]},
        {'id': 'offload_pbo_intra_%s_fail4' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload With Invalid Options',
         'narrative': 'Attempt to switch to FULL while in ..._AND_PREDICATE config',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, '2')),
                     'ipa_predicate_type': ipa_and_predicate_type,
                     'offload_type': OFFLOAD_TYPE_FULL},
         'expected_exception_string': PREDICATE_TYPE_OFFLOAD_TYPE_FULL_EXCEPTION_TEXT},
        {'id': 'offload_pbo_intra_%s_offload4' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 2nd Predicate',
         'narrative': 'Offloads 2nd predicate (IN list) on top of HV_1 to top up data',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate': GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, ['3', '4']))},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, table_name, chk_hv_1, check_rowcount=False,
                                     incremental_key=inc_key, incremental_key_type=part_key_type,
                                     ipa_predicate_type=ipa_and_predicate_type) +
             [(lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, table_name,
                                          number_of_predicates=2,
                                          expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                          values_in_predicate_value_metadata=metadata_chk_hvs + ['(2)', '(3)', '(4)']),
               lambda test: True),
              (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, table_name,
                                                         'offload_pbo_intra_%s_offload4' % test_id,
                                                         chk_cnt_filter % {'hv_1': hv_1, 'hv_2': hv_2, 'channel': '3'}),
               lambda test: True),
              (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, table_name,
                                                         'offload_pbo_intra_%s_offload4' % test_id,
                                                         chk_cnt_filter % {'hv_1': hv_1, 'hv_2': hv_2, 'channel': '4'}),
               lambda test: True),
              ]},
        {'id': 'offload_pbo_intra_%s_offload5' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Predicate No-modify Hybrid View',
         'narrative': 'Offloads 3rd predicate but with --no-modify-hybrid-view - only moves data, leaves HV as before.',
         'options': {'owner_table': schema + '.' + table_name,
                     'offload_predicate_modify_hybrid_view': False,
                     'offload_predicate': GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, '5'))},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, table_name, chk_hv_1, check_rowcount=False,
                                     incremental_key=inc_key, incremental_key_type=part_key_type,
                                     ipa_predicate_type=ipa_and_predicate_type) +
             [(lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, table_name,
                                          number_of_predicates=2,
                                          expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                          values_in_predicate_value_metadata=metadata_chk_hvs + ['(2)', '(3)', '(4)']),
               lambda test: True),
              (lambda test: check_predicate_count_matches_log(frontend_api, schema, table_name,
                                                              'offload_pbo_intra_%s_offload5' % test_id,
                                                              chk_cnt_filter % {'hv_1': hv_1, 'hv_2': hv_2,
                                                                                'channel': '5'}),
               lambda test: True),
              ]},
        {'id': 'offload_pbo_intra_%s_offload6' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Predicate Reset Hybrid View',
         'narrative': 'Offloads predicate on top of HV_1 but with --reset-hybrid-view - moves data and resets HV.',
         'options': {'owner_table': schema + '.' + table_name,
                     'reset_hybrid_view': True,
                     'offload_predicate': GenericPredicate(gen_pred(hv_pred, hv_1, hv_2, '9')),
                     'ipa_predicate_type': ipa_and_predicate_type},
         'assertion_pairs':
             offload_fact_assertions(options, backend_api, frontend_api, repo_client, schema,
                                     hybrid_schema, data_db, table_name, chk_hv_1, check_rowcount=False,
                                     incremental_key=inc_key, incremental_key_type=part_key_type,
                                     ipa_predicate_type=ipa_and_predicate_type) +
             [(lambda test: pbo_assertion(options, repo_client, frontend_api, hybrid_schema, table_name,
                                          number_of_predicates=1,
                                          expected_offload_type=OFFLOAD_TYPE_INCREMENTAL,
                                          values_in_predicate_value_metadata=metadata_chk_hvs + ['(9)']),
               lambda test: True),
              (lambda test: check_predicate_counts_match(frontend_api, schema, hybrid_schema, table_name,
                                                         'offload_pbo_intra_%s_offload6' % test_id,
                                                         chk_cnt_filter % {'hv_1': hv_1, 'hv_2': hv_2, 'channel': '9'}),
               lambda test: True),
              (lambda test: check_predicate_count_matches_log(frontend_api, schema, table_name,
                                                              'offload_pbo_intra_%s_offload6' % test_id,
                                                              chk_cnt_filter % {'hv_1': hv_1, 'hv_2': hv_2,
                                                                                'channel': '9'}),
               lambda test: True),
              ]},
        {'id': 'offload_pbo_intra_%s_offload7' % test_id,
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 2nd Partition',
         'narrative': 'Offload 2nd partition putting the table back into "range" mode, no data should be moved',
         'options': {'owner_table': schema + '.' + table_name,
                     'older_than_date': hv_2},
         'assertion_pairs': offload_fact_assertions(options, backend_api, frontend_api, repo_client,
                                                    schema, hybrid_schema, data_db,
                                                    table_name, chk_hv_2, check_rowcount=False,
                                                    incremental_key=inc_key, incremental_key_type=part_key_type,
                                                    ipa_predicate_type=ipa_predicate_type,
                                                    incremental_predicate_value='NULL') + \
         [
             (lambda test: bool(get_offload_row_count_from_log('offload_pbo_intra_%s_offload7' % test_id) in (None, 0)),
              lambda test: True),
         ]}
    ]

def offload_pbo_intra_day_story_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                      hybrid_cursor, repo_client):
    """ Tests for Intra Day Predicate Based Offload, case 3 from the design:
        https://gluent.atlassian.net/wiki/spaces/DEV/pages/779812884/Predicate-Based+Offload+Design+Notes
    """
    part_key_type = frontend_api.test_type_canonical_date() if frontend_api else None
    return offload_pbo_intra_day_std_range_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                                 repo_client, hybrid_cursor, RANGE_TABLE_INTRA) + \
           offload_pbo_intra_day_std_range_tests(schema, hybrid_schema, data_db, options, backend_api, frontend_api,
                                                 repo_client, hybrid_cursor, LAR_TABLE_INTRA) + \
    [
        #
        # LIST PARTITIONED TABLE
        #
        {'id': 'offload_pbo_intra_list_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Create RANGE %s' % LIST_TABLE_INTRA,
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_sales_based_list_create_ddl(frontend_api, schema, LIST_TABLE_INTRA,
                                                                            part_key_type=part_key_type,
                                                                            default_partition=True),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, LIST_TABLE_INTRA),
                   STORY_SETUP_TYPE_PYTHON: [lambda: drop_backend_test_table(options, backend_api,
                                                                             data_db, LIST_TABLE_INTRA)]}},
        {'id': 'offload_pbo_intra_list_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload 1st LIST Partition',
         'narrative': 'Offload 1st partition putting table in LIST mode',
         'options': {'owner_table': '%s.%s' % (schema, LIST_TABLE_INTRA),
                     'equal_to_values': [SALES_BASED_FACT_HV_1],
                     'reset_backend_table': True},
         'assertion_pairs': [
             (lambda test: offload_lpa_fact_assertion(
                 test, schema, hybrid_schema, data_db, LIST_TABLE_INTRA, options, backend_api,
                 frontend_api, repo_client, [SALES_BASED_FACT_HV_1],
                 incremental_predicate_type=INCREMENTAL_PREDICATE_TYPE_LIST),
              lambda test: True)]},
        {'id': 'offload_pbo_intra_list_fail1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload Intra Day to List',
         'narrative': 'Intra day is only supported for RANGE/LIST_AS_RANGE - not LIST',
         'options': {'owner_table': schema + '.' + LIST_TABLE_INTRA,
                     'offload_predicate': GenericPredicate(
                         '(column(time_id) = datetime(%s)) and (column(channel_id) = numeric(3))'
                         % SALES_BASED_FACT_HV_3)},
         'expected_exception_string': PREDICATE_TYPE_INCOMPATIBLE_EXCEPTION_TEXT},
    ]
