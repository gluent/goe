from gluentlib.config import orchestration_defaults
from gluentlib.offload.column_metadata import match_table_column, CANONICAL_CHAR_SEMANTICS_UNICODE
from gluentlib.offload.offload_constants import DBTYPE_BIGQUERY, DBTYPE_SNOWFLAKE, DBTYPE_SYNAPSE
from gluentlib.offload.offload_messages import OffloadMessages
from gluentlib.persistence.factory.orchestration_repo_client_factory import orchestration_repo_client_factory

from tests.integration.test_sets.offload_test_functions import get_offload_test_fn
from tests.integration.test_functions import build_current_options, get_default_test_user, get_default_test_user_pass
from tests.testlib.test_framework import test_constants
from tests.testlib.test_framework.factory.backend_testing_api_factory import backend_testing_api_factory
from tests.testlib.test_framework.factory.frontend_testing_api_factory import frontend_testing_api_factory
from tests.testlib.test_framework.test_functions import get_backend_columns_for_hybrid_view, to_hybrid_schema


def check_type_mapping_offload_columns(frontend_api, backend_api, current_schema, orchestration_config, messages):
    """Check that offloaded columns have the data type in the backend that we expected."""
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
            assert backend_column is not None
            if expected_backend_column.data_precision:
                assert backend_column.data_type == expected_backend_column.data_type, 'Backend type != expected type'
                assert (backend_column.data_precision, backend_column.data_scale) == (expected_backend_column.data_precision, expected_backend_column.data_scale), 'Backend precision/scale != expected precision/scale'
            else:
                assert backend_column.data_type == expected_backend_column.data_type, 'Backend type != expected type'


def test_data_type_mapping_offload():
    config = build_current_options()
    test_schema = get_default_test_user()
    test_schema_pass = get_default_test_user_pass()
    messages = OffloadMessages()
    backend_api = backend_testing_api_factory(config.target, config, messages, dry_run=False)
    frontend_api = frontend_testing_api_factory(config.db_type, config, messages, dry_run=False)
    test_api = frontend_api.create_new_connection(test_schema, test_schema_pass)
    max_decimal_precision = backend_api.max_decimal_precision()
    max_decimal_scale = backend_api.max_decimal_scale()
    max_decimal_integral_magnitude = backend_api.max_decimal_integral_magnitude()

    # Offload the table with data type control options as defined in frontend_api.
    offload_modifiers = test_api.gl_type_mapping_offload_options(max_decimal_precision,
                                                                 max_decimal_scale,
                                                                 max_decimal_integral_magnitude)
    t = get_offload_test_fn(
        test_schema, test_constants.GL_TYPE_MAPPING,
        test_api, backend_api, config, messages,
        offload_modifiers=offload_modifiers, create_backend_db=True
    )
    assert t() == 0

    check_type_mapping_offload_columns(frontend_api, backend_api, test_schema, config, messages)
