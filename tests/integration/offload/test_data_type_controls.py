# Copyright 2024 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from typing import TYPE_CHECKING

from goe.config import orchestration_defaults
from goe.offload.column_metadata import (
    match_table_column,
    CANONICAL_CHAR_SEMANTICS_UNICODE,
)
from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_messages import OffloadMessages

from tests.integration.test_functions import (
    build_current_options,
    get_default_test_user,
    get_default_test_user_pass,
    run_offload,
)
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api,
    get_test_messages,
)

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from testlib.test_framework.backend_testing_api import BackendTestingApiInterface
    from testlib.test_framework.frontend_testing_api import FrontendTestingApiInterface


DIM_NAME = "INTEG_DC_CTRL_TAB"


@pytest.fixture
def config():
    return build_current_options()


@pytest.fixture
def schema():
    return get_default_test_user()


@pytest.fixture
def data_db(schema, config):
    data_db = data_db_name(schema, config)
    data_db = convert_backend_identifier_case(config, data_db)
    return data_db


def create_test_table(
    frontend_api: "FrontendTestingApiInterface",
    backend_api: "BackendTestingApiInterface",
    test_id: str,
    schema: str,
    ascii_only=False,
    all_chars_notnull=False,
    max_decimal_precision=None,
    max_decimal_scale=None,
    max_decimal_integral_magnitude=None,
):
    """Create and populate type mapping integration test table."""
    test_schema = get_default_test_user()
    test_schema_pass = get_default_test_user_pass()
    with frontend_api.create_new_connection_ctx(
        test_schema,
        test_schema_pass,
        trace_action_override=test_id,
    ) as sh_test_api:
        sh_test_api.create_type_mapping_table(
            schema,
            DIM_NAME,
            ascii_only=ascii_only,
            max_backend_precision=max_decimal_precision,
            max_backend_scale=max_decimal_scale,
            max_decimal_integral_magnitude=max_decimal_integral_magnitude,
            all_chars_notnull=all_chars_notnull,
            supported_canonical_types=list(
                backend_api.expected_canonical_to_backend_type_map().keys()
            ),
        )


def check_type_mapping_offload_columns(
    frontend_api: "FrontendTestingApiInterface",
    backend_api: "BackendTestingApiInterface",
    data_db: str,
    config: "OrchestrationConfig",
    messages,
):
    """Check that offloaded columns have the data type in the backend that we expected."""
    max_decimal_precision = backend_api.max_decimal_precision()
    max_decimal_scale = backend_api.max_decimal_scale()
    max_decimal_integral_magnitude = backend_api.max_decimal_integral_magnitude()
    backend_name = convert_backend_identifier_case(config, DIM_NAME)

    # Compare actual backend types with expected types as defined in backend_api
    backend_columns = backend_api.get_columns(data_db, backend_name)
    expected_canonical_types = frontend_api.goe_type_mapping_expected_canonical_cols(
        max_decimal_precision, max_decimal_scale, max_decimal_integral_magnitude
    )

    for column_name, expected_canonical_column, overrides in expected_canonical_types:
        if column_name.startswith("COL_BINARY_FLOAT") and config.target in [
            offload_constants.DBTYPE_BIGQUERY,
            offload_constants.DBTYPE_SNOWFLAKE,
            offload_constants.DBTYPE_SYNAPSE,
        ]:
            # This is horrible and will be reversed by issue-180 - simply remove this entire "if" block
            messages.log(
                "Skipping column COL_BINARY_FLOAT_DOUBLE until issue-180 is actioned"
            )
            continue
        use_overrides = overrides
        if (
            expected_canonical_column.char_semantics == CANONICAL_CHAR_SEMANTICS_UNICODE
            and not overrides
        ):
            use_overrides = {
                "unicode_string_columns_csv": expected_canonical_column.name
            }
        expected_backend_column = backend_api.expected_backend_column(
            expected_canonical_column,
            override_used=use_overrides,
            decimal_padding_digits=orchestration_defaults.decimal_padding_digits_default(),
        )
        if expected_backend_column:
            backend_column = match_table_column(column_name, backend_columns)
            assert (
                backend_column is not None
            ), f"{column_name} is not in backend columns: {backend_columns}"
            if expected_backend_column.data_precision:
                assert (
                    backend_column.data_type == expected_backend_column.data_type
                ), f"{column_name}: Backend type != expected type"
                assert (backend_column.data_precision, backend_column.data_scale) == (
                    expected_backend_column.data_precision,
                    expected_backend_column.data_scale,
                ), f"{column_name}: Backend precision/scale != expected precision/scale"
            else:
                assert (
                    backend_column.data_type == expected_backend_column.data_type
                ), f"{column_name}: Backend type != expected type"


def test_data_type_mapping_offload(config, schema, data_db):
    id = "test_data_type_mapping_offload"
    test_messages = get_test_messages(config, id)
    backend_api = get_backend_testing_api(config, test_messages)
    frontend_api = get_frontend_testing_api(config, test_messages, trace_action=id)

    messages = OffloadMessages.from_options(config, log_fh=test_messages.get_log_fh())

    max_decimal_precision = backend_api.max_decimal_precision()
    max_decimal_scale = backend_api.max_decimal_scale()
    max_decimal_integral_magnitude = backend_api.max_decimal_integral_magnitude()
    ascii_only = False
    all_chars_notnull = False
    if config.target == offload_constants.DBTYPE_SYNAPSE:
        ascii_only = True
        all_chars_notnull = True

    # Create frontend type mapping table.
    create_test_table(
        frontend_api,
        backend_api,
        id,
        schema,
        ascii_only=ascii_only,
        all_chars_notnull=all_chars_notnull,
        max_decimal_precision=max_decimal_precision,
        max_decimal_scale=max_decimal_scale,
        max_decimal_integral_magnitude=max_decimal_integral_magnitude,
    )

    # Offload the table with data type control options as defined in frontend_api.
    offload_options = {
        "owner_table": schema + "." + DIM_NAME,
        "create_backend_db": True,
        "reset_backend_table": True,
    }
    offload_modifiers = frontend_api.goe_type_mapping_offload_options(
        max_decimal_precision, max_decimal_scale, max_decimal_integral_magnitude
    )
    offload_options.update(offload_modifiers)
    run_offload(offload_options)

    check_type_mapping_offload_columns(
        frontend_api, backend_api, data_db, config, messages
    )

    # Connections are being left open, explicitly close them.
    frontend_api.close()
