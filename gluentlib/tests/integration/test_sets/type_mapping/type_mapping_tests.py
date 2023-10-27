#! /usr/bin/env python3
""" Run type-mapping test set
    LICENSE_TEXT
"""

import traceback
from typing import Callable

from gluentlib.config import orchestration_defaults
from gluentlib.offload.column_metadata import match_table_column, CANONICAL_CHAR_SEMANTICS_UNICODE
from gluentlib.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_SNOWFLAKE, DBTYPE_SYNAPSE
from gluentlib.persistence.factory.orchestration_repo_client_factory import orchestration_repo_client_factory
from testlib.test_framework import test_constants
from testlib.test_framework.test_functions import get_backend_columns_for_hybrid_view, to_hybrid_schema,\
    test_teamcity_endtest, test_teamcity_endtestsuite,\
    test_teamcity_failtest, test_teamcity_starttest, test_teamcity_starttestsuite
from test_sets.base_test import BaseTest
from test_sets.offload_test_functions import get_offload_test_fn


def is_equal_test_f(val1, val2, desc) -> Callable:
    return lambda test: test.assertEqual(val1, val2, desc)


def is_not_none_test_f(val1, desc) -> Callable:
    return lambda test: test.assertIsNotNone(val1, desc)


def run_type_mapping_orchestration_tests(options, sh_test_user, frontend_api, backend_api, sh_test_api,
                                         orchestration_config, messages, should_run_test_f):
    test_teamcity_starttestsuite(options, test_constants.SET_TYPE_MAPPING)
    try:
        run_type_mapping_offload_test(sh_test_api, backend_api, options, sh_test_user,
                                      orchestration_config, messages, BaseTest, should_run_test_f)

        run_type_mapping_offload_column_tests(frontend_api, backend_api, options, sh_test_user,
                                              orchestration_config, messages, BaseTest, should_run_test_f)
    except Exception as exc:
        messages.log('Unhandled exception in %s test set:\n%s' % (test_constants.SET_TYPE_MAPPING,
                                                                  traceback.format_exc()))
        test_teamcity_failtest(options, test_constants.SET_TYPE_MAPPING, 'Fail: %s' % str(exc))
    finally:
        test_teamcity_endtestsuite(options, test_constants.SET_TYPE_MAPPING)


def run_type_mapping_offload_test(sh_test_api, backend_api, options, current_schema,
                                  orchestration_config, messages, test_class, should_run_test_f,
                                  hybrid_query_parallelism=2):
    max_decimal_precision = backend_api.max_decimal_precision()
    max_decimal_scale = backend_api.max_decimal_scale()
    max_decimal_integral_magnitude = backend_api.max_decimal_integral_magnitude()

    # Offload the table with data type control options as defined in frontend_api
    test_name = '%s_%s' % (test_constants.SET_TYPE_MAPPING.replace('-', '_'), test_constants.GL_TYPE_MAPPING)
    if should_run_test_f(test_name):
        offload_modifiers = sh_test_api.gl_type_mapping_offload_options(max_decimal_precision,
                                                                        max_decimal_scale,
                                                                        max_decimal_integral_magnitude)
        # with_assertions=frontend_api.hybrid_schema_supported() below because type_mapping table has
        # forced data type changes which will make data validation using frontend/backend data very difficult.
        t = test_class(options, test_name,
                       get_offload_test_fn(current_schema, test_constants.GL_TYPE_MAPPING,
                                           sh_test_api, backend_api, orchestration_config, messages,
                                           offload_modifiers=offload_modifiers, create_backend_db=True,
                                           with_assertions=sh_test_api.hybrid_schema_supported(),
                                           hybrid_query_parallelism=hybrid_query_parallelism))
        t()


def run_type_mapping_offload_column_tests(frontend_api, backend_api, options, current_schema,
                                          orchestration_config, messages, test_class, should_run_test_f):
    """ Check that offloaded columns have the data type in the backend that we expected. """
    repo_client = orchestration_repo_client_factory(orchestration_config, messages)
    hybrid_schema = to_hybrid_schema(current_schema)
    max_decimal_precision = backend_api.max_decimal_precision()
    max_decimal_scale = backend_api.max_decimal_scale()
    max_decimal_integral_magnitude = backend_api.max_decimal_integral_magnitude()

    # Compare actual backend types with expected types as defined in backend_api
    backend_columns = get_backend_columns_for_hybrid_view(hybrid_schema, test_constants.GL_TYPE_MAPPING,
                                                          backend_api, repo_client)
    expected_canonical_types = frontend_api.gl_type_mapping_expected_canonical_cols(max_decimal_precision,
                                                                                    max_decimal_scale,
                                                                                    max_decimal_integral_magnitude)
    for column_name, expected_canonical_column, overrides in expected_canonical_types:
        test_name = '%s_%s_%s' % (test_constants.SET_TYPE_MAPPING.replace('-', '_'),
                                  test_constants.GL_TYPE_MAPPING, column_name)
        if should_run_test_f(test_name):
            if column_name.startswith('COL_BINARY_FLOAT') and orchestration_config.target in [DBTYPE_BIGQUERY,
                                                                                              DBTYPE_SNOWFLAKE,
                                                                                              DBTYPE_SYNAPSE]:
                # This is horrible and will be reversed by "GOE-1718" - simply remove this entire "if" block
                messages.log('Skipping column COL_BINARY_FLOAT_DOUBLE until GOE-1718 is actioned')
                continue
            use_overrides = overrides
            if expected_canonical_column.char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE and not overrides:
                use_overrides = {'unicode_string_columns_csv': expected_canonical_column.name}
            expected_backend_column = backend_api.expected_backend_column(
                expected_canonical_column,
                override_used=use_overrides,
                decimal_padding_digits=orchestration_defaults.decimal_padding_digits_default()
            )
            if expected_backend_column:
                backend_column = match_table_column(column_name, backend_columns)
                if not backend_column:
                    t = test_class(options, test_name, is_not_none_test_f(backend_column, 'Backend column is None'))
                    t()
                else:
                    if expected_backend_column.data_precision:
                        def test_fn(test):
                            is_equal_test_f(backend_column.data_type, expected_backend_column.data_type,
                                            'Backend type != expected type')(test)
                            is_equal_test_f((backend_column.data_precision,
                                             backend_column.data_scale),
                                            (expected_backend_column.data_precision,
                                             expected_backend_column.data_scale),
                                            'Backend precision/scale != expected precision/scale')(test)
                    else:
                        def test_fn(test):
                            is_equal_test_f(backend_column.data_type, expected_backend_column.data_type,
                                            'Backend type != expected type')(test)
                    t = test_class(options, test_name, test_fn)
                    t()
