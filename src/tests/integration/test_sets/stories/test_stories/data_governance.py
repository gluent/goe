import json

from test_sets.stories.story_globals import STORY_SETUP_TYPE_HYBRID, STORY_SETUP_TYPE_ORACLE, STORY_SETUP_TYPE_PYTHON, \
    STORY_TYPE_OFFLOAD, STORY_TYPE_PRESENT, STORY_TYPE_SETUP
from test_sets.stories.story_setup_functions import gen_hybrid_drop_ddl, gen_rdbms_dim_create_ddl, story_now_name

from gluent import get_log_fh, log
from goe.data_governance.hadoop_data_governance import get_hadoop_data_governance_client_from_options, \
    is_dynamic_tag, stripped_dynamic_tag_name
from goe.data_governance.hadoop_data_governance_constants import MUTED_DATA_GOVERNANCE_DB_TAGS,\
    MUTED_DATA_GOVERNANCE_DB_PROPERTIES, \
    DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT, DATA_GOVERNANCE_DYNAMIC_PROPERTY_TARGET_RDBMS_OBJECT
from goe.offload.offload_functions import convert_backend_identifier_case, data_db_name
from goe.offload.offload_messages import OffloadMessages, VERBOSE


def data_governance_is_enabled(options):
    if options and options.data_governance_api_url:
        messages = OffloadMessages.from_options(options, get_log_fh())
        data_gov_client = get_hadoop_data_governance_client_from_options(options, messages)
        if data_gov_client:
            return True
    return False


def has_all_tags(metadata_tags, tag_list):
    if not metadata_tags:
        return False
    missing_tags = [_ for _ in tag_list if _ not in metadata_tags]
    if missing_tags:
        log('has_all_tags, missing tags: %s' % missing_tags, detail=VERBOSE)
        return False
    return True


def has_all_props(metadata_prop_dict, prop_json):
    if not metadata_prop_dict:
        return False
    custom_props = json.loads(prop_json)
    missing_keys = [_ for _ in list(custom_props.keys()) if _ not in list(metadata_prop_dict.keys())]
    if missing_keys:
        log('has_all_props, missing keys: %s' % missing_keys, detail=VERBOSE)
        return False
    wrong_values = [v for k, v in custom_props.items() if v != metadata_prop_dict[k]]
    if wrong_values:
        log('has_all_props, wrong values: %s' % wrong_values, detail=VERBOSE)
        return False
    return True


def data_gov_checks(backend_db, table_name, options, messages, custom_tags=[], custom_props='', checking_present=False, custom_prop_fn=None):
    """ check that data governance metadata exists
        if custom_tags then check if the tags are part of the metadata
        if custom_props then check if the property key/value pairs are part of the metadata
        present_messages. If this is set then we are checking a present and need to be flexible on whether metadata exists or not
            If we had a warning saying there was no metadata then extraction has not run since schema rebuild and we cannot do detailed checks
        custom_prop_fn: a function we can use for extra validation of properties
    """

    if not options or not options.data_governance_api_url:
        return True

    if checking_present:
        if [_ for _ in messages.get_warnings() if 'No metadata retrieved' in _]:
            log('No metadata for presented object therefore we cannot check our updates', detail=VERBOSE)
            return True

    data_gov_client = get_hadoop_data_governance_client_from_options(options, messages)
    dg_metadata = data_gov_client.get_hive_object_custom_metadata(backend_db, table_name)

    if not dg_metadata:
        log('Missing metadata for %s.%s' % (backend_db, table_name), VERBOSE)
        return False

    data_gov_client.validate_metadata(dg_metadata)
    dg_tags = data_gov_client.get_tags_from_metadata(dg_metadata)
    dg_props = data_gov_client.get_properties_from_metadata(dg_metadata)

    if not has_all_tags(dg_tags, custom_tags):
        log('Missing custom tags for %s.%s' % (backend_db, table_name), VERBOSE)
        return False

    if not has_all_props(dg_props, custom_props):
        log('Missing custom properties for %s.%s' % (backend_db, table_name), VERBOSE)
        return False

    if custom_prop_fn:
        log('Calling custom_prop_fn with: %s' % dg_props, VERBOSE)
        if not custom_prop_fn(dg_props):
            log('False custom_prop_fn for %s.%s' % (backend_db, table_name), VERBOSE)
            return False

    return True


def data_gov_db_checks(hadoop_db, options, messages, custom_tags=[], custom_props=''):
    """ check that data governance metadata exists
        if custom_tags then check if the tags are part of the metadata
        if custom_props then check if the property key/value pairs are part of the metadata
    """

    if not options.data_governance_api_url:
        return True

    data_gov_client = get_hadoop_data_governance_client_from_options(options, messages)
    dg_metadata = data_gov_client.get_hive_db_metadata(hadoop_db)

    if not dg_metadata:
        log('Missing metadata for %s' % hadoop_db, VERBOSE)
        return False

    dg_metadata = data_gov_client.get_custom_metadata_from_full_metadata(dg_metadata)

    data_gov_client.validate_metadata(dg_metadata)
    dg_tags = data_gov_client.get_tags_from_metadata(dg_metadata)
    dg_props = data_gov_client.get_properties_from_metadata(dg_metadata)

    if not has_all_tags(dg_tags, custom_tags):
        log('Missing custom tags for %s' % hadoop_db, VERBOSE)
        return False

    # ensure there are no object level tags on the db
    invalid_db_tags = [_ for _ in dg_tags if is_dynamic_tag(_) and stripped_dynamic_tag_name(_) in MUTED_DATA_GOVERNANCE_DB_TAGS]
    if invalid_db_tags:
        log('Invalid DB tags for %s: %s' % (hadoop_db, invalid_db_tags), VERBOSE)
        return False

    if not has_all_props(dg_props, custom_props):
        log('Missing custom properties for %s' % hadoop_db, VERBOSE)
        return False

    # ensure there are no object level props on the db
    invalid_db_properties = [_ for _ in list(dg_props.keys()) if _ in MUTED_DATA_GOVERNANCE_DB_PROPERTIES]
    if invalid_db_properties:
        log('Invalid DB properties for %s: %s' % (hadoop_db, invalid_db_properties), VERBOSE)
        return False

    return True


def data_governance_story_tests(schema, hybrid_schema, data_db, load_db, options, backend_api, frontend_api, list_only):
    def data_gov_offload1_prop_fn(props, table_name):
        log('data_gov_offload1_prop_fn(props, %s)' % table_name, detail=VERBOSE)
        # offloaded properties should contain the source rdbms property with this current table name
        if DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT not in list(props.keys()):
            log('DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT not in props', detail=VERBOSE)
            return False
        if table_name.upper() not in props[DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT]:
            log('%s not DATA_GOVERNANCE_DYNAMIC_PROPERTY_SOURCE_RDBMS_OBJECT' % table_name, detail=VERBOSE)
            return False
        return True

    def data_gov_present2_prop_fn(props, table1, table2):
        log('data_gov_present2_prop_fn(props, %s, %s)' % (table1, table2), detail=VERBOSE)
        return bool(table1.upper() in props[DATA_GOVERNANCE_DYNAMIC_PROPERTY_TARGET_RDBMS_OBJECT] and \
                    table2.upper() in props[DATA_GOVERNANCE_DYNAMIC_PROPERTY_TARGET_RDBMS_OBJECT])

    if list_only:
        def data_db_name_safe(story_now_name, options):
            return None
    else:
        if not options:
            return []

        def data_db_name_safe(story_now_name, options):
            return data_db_name(story_now_name, options)

        if not data_governance_is_enabled(options):
            log('Data Governance is not enabled, skipping tests', detail=VERBOSE)
            return []

    custom_tag_list = ['TESTTAG1', 'TESTTAG2']
    custom_tag_csv = ','.join(custom_tag_list)
    custom_props = '{"TESTPROP": "TESTPROP"}'
    rdbms_dim = story_now_name('dg_o')
    hadoop_dim = story_now_name('dg_h_only')
    hadoop_dim2 = story_now_name('dg_h_only2')
    rdbms_dim_be, hadoop_dim, hadoop_dim2 = convert_backend_identifier_case(options, rdbms_dim, hadoop_dim, hadoop_dim2)
    ctas_storage_format = backend_api.default_storage_format() if backend_api else 'TEXTFILE'

    return [
        {'id': 'data_gov_setup',
         'type': STORY_TYPE_SETUP,
         'title': 'Setup objects for data governance tests',
         'setup': {STORY_SETUP_TYPE_ORACLE: gen_rdbms_dim_create_ddl(frontend_api, schema, rdbms_dim),
                   STORY_SETUP_TYPE_HYBRID: gen_hybrid_drop_ddl(options, frontend_api, hybrid_schema, rdbms_dim),
                   STORY_SETUP_TYPE_PYTHON: [
                       lambda: backend_api.drop_table(data_db, rdbms_dim_be, sync=True),
                       lambda: backend_api.create_table_as_select(data_db, hadoop_dim, ctas_storage_format,
                                                                  [('123', 'id1'), ('456', 'id2')])]}},
        {'id': 'data_gov_offload1',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with data governance to a new database',
         'options': {'owner_table': '%s.%s' % (schema, rdbms_dim),
                     'target_owner_name': '%s.%s' % (story_now_name(schema), rdbms_dim),
                     'reset_backend_table': True,
                     'create_backend_db': True},
         'config_override': {'data_governance_custom_tags_csv': custom_tag_csv,
                             'data_governance_custom_properties': custom_props},
         'assertion_pairs': [
             (lambda test: data_gov_checks(data_db_name_safe(story_now_name(schema), options), rdbms_dim, options,
                                           test.offload_messages, custom_tags=custom_tag_list,
                                           custom_props=custom_props), lambda test: True),
             (lambda test: data_gov_db_checks(data_db_name_safe(story_now_name(schema), options), options,
                                              test.offload_messages, custom_tags=custom_tag_list,
                                              custom_props=custom_props), lambda test: True)]},
        {'id': 'data_gov_offload2',
         'type': STORY_TYPE_OFFLOAD,
         'title': 'Offload with data governance',
         'options': {'owner_table': '%s.%s' % (schema, rdbms_dim),
                     'reset_backend_table': True},
         'config_override': {'data_governance_custom_tags_csv': custom_tag_csv,
                             'data_governance_custom_properties': custom_props},
         'assertion_pairs': [
             (lambda test: data_gov_checks(data_db, rdbms_dim, options, test.offload_messages,
                                           custom_tags=custom_tag_list, custom_props=custom_props,
                                           custom_prop_fn=lambda props: data_gov_offload1_prop_fn(props, rdbms_dim)),
              lambda test: True)]},
        {'id': 'data_gov_present1',
         'type': STORY_TYPE_PRESENT,
         'title': 'Present with data governance',
         'options': {'owner_table': '%s.%s' % (data_db, hadoop_dim),
                     'target_owner_name': '%s.%s' % (schema, hadoop_dim)},
         'config_override': {'data_governance_custom_tags_csv': custom_tag_csv,
                             'data_governance_custom_properties': custom_props},
         'assertion_pairs': [(lambda test: data_gov_checks(data_db, hadoop_dim, options, test.offload_messages,
                                                           custom_tags=custom_tag_list, custom_props=custom_props,
                                                           checking_present=True), lambda test: True)]},
        {'id': 'data_gov_present2',
         'type': STORY_TYPE_PRESENT,
         'title': 'Present with data governance to a second name',
         'options': {'owner_table': '%s.%s' % (data_db, hadoop_dim),
                     'target_owner_name': '%s.%s' % (schema, hadoop_dim2)},
         'config_override': {'data_governance_custom_tags_csv': custom_tag_csv,
                             'data_governance_custom_properties': custom_props},
         'assertion_pairs': [(lambda test: data_gov_checks(data_db, hadoop_dim, options, test.offload_messages, custom_tags=custom_tag_list,
                                                           custom_props=custom_props, checking_present=True,
                                                           custom_prop_fn=lambda props: data_gov_present2_prop_fn(props, hadoop_dim, hadoop_dim2)),
                              lambda test: True)]},
        {'id': 'data_gov_join',
         'type': STORY_TYPE_PRESENT,
         'title': 'Present a join with data governance',
         'options': {'owner_table': '%s.%s' % (schema, story_now_name('dg_j')),
                     'present_join_opts': ['table(%s) alias(t1)' % rdbms_dim,
                                           'table(%s) alias(t2) inner-join(t1) join-clauses(t1.prod_id = t2.id1)' % hadoop_dim],
                     'reset_backend_table': True,
                     'sample_stats': 5},
         'config_override': {'data_governance_custom_tags_csv': custom_tag_csv,
                             'data_governance_custom_properties': custom_props},
         'assertion_pairs': [(lambda test: data_gov_checks(data_db, story_now_name('dg_j'), options,
                                                           test.offload_messages, custom_tags=custom_tag_list,
                                                           custom_props=custom_props), lambda test: True)]},
        {'id': 'data_gov_cleanup',
         'type': STORY_TYPE_SETUP,
         'title': 'Cleanup objects for data governance tests',
         'setup': {STORY_SETUP_TYPE_PYTHON: [lambda: backend_api.drop_database(data_db_name_safe(story_now_name(schema), options), cascade=True),
                                             lambda: backend_api.drop_database(data_db_name_safe(story_now_name(schema) + '_load', options), cascade=True)]}},
    ]
