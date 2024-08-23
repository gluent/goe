# Copyright 2016 The GOE Authors. All rights reserved.
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

from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    standard_dimension_assertion,
)
from tests.integration.scenarios.scenario_runner import (
    run_offload,
    run_setup,
)
from tests.integration.scenarios.setup_functions import (
    drop_backend_test_table,
)
from tests.integration.test_functions import (
    cached_current_options,
    cached_default_test_user,
)
from tests.testlib.test_framework.test_functions import (
    get_backend_testing_api,
    get_frontend_testing_api_ctx,
    get_test_messages_ctx,
)


OFFLOAD_DIM = "STORY_SYNPSE_HASH_DIM"


@pytest.fixture
def config():
    return cached_current_options()


@pytest.fixture
def schema():
    return cached_default_test_user()


@pytest.fixture
def data_db(schema, config):
    data_db = data_db_name(schema, config)
    data_db = convert_backend_identifier_case(config, data_db)
    return data_db


def synapse_distribution_assertion(
    backend_api, messages, data_db, backend_name, expected_distribution
):
    table_distribution = backend_api.table_distribution(data_db, backend_name)
    if table_distribution != expected_distribution:
        messages.log(
            f"table_distribution({data_db}, {backend_name}) {table_distribution} != {expected_distribution}"
        )
        return False
    return True


def test_offload_hash_column_synapse(config, schema, data_db):
    """Synapse tests ensuring this comment from design note remains true:
    Sep 7, 2021 Tables below default bucket threshold size will default to ROUND_ROBIN, else HASH.
    HASH keys will be chosen from hash bucket key option or automatically in order of PK, stats and fallback.
    """
    id = "test_offload_hash_column_synapse"
    if config.target != offload_constants.DBTYPE_SYNAPSE:
        pytest.skip(f"Skipping {id} for backend: {config.target}")

    with get_test_messages_ctx(config, id) as messages, get_frontend_testing_api_ctx(
        config, messages, trace_action=id
    ) as frontend_api:
        backend_api = get_backend_testing_api(config, messages)
        repo_client = orchestration_repo_client_factory(
            config, messages, trace_action=f"repo_client({id})"
        )

        # Setup
        run_setup(
            frontend_api,
            backend_api,
            config,
            messages,
            frontend_sqls=frontend_api.standard_dimension_frontend_ddl(
                schema, OFFLOAD_DIM
            ),
            python_fns=[
                lambda: drop_backend_test_table(
                    config, backend_api, messages, data_db, OFFLOAD_DIM
                ),
            ],
        )

        # Offload the dimension without --bucket-hash-column and high threshold, expect ROUND_ROBIN.
        options = {
            "owner_table": schema + "." + OFFLOAD_DIM,
            "reset_backend_table": True,
            "create_backend_db": True,
            "execute": True,
        }
        run_offload(
            options,
            config,
            messages,
            config_overrides={"hash_distribution_threshold": "10g"},
        )
        assert standard_dimension_assertion(
            config,
            backend_api,
            messages,
            repo_client,
            schema,
            data_db,
            OFFLOAD_DIM,
            bucket_column="NULL",
        )
        assert synapse_distribution_assertion(
            backend_api, messages, data_db, OFFLOAD_DIM, "ROUND_ROBIN"
        )

        # Offload the dimension with --bucket-hash-column and high threshold, expect HASH.
        options = {
            "owner_table": schema + "." + OFFLOAD_DIM,
            "bucket_hash_col": "prod_id",
            "reset_backend_table": True,
            "execute": True,
        }
        run_offload(
            options,
            config,
            messages,
            config_overrides={"hash_distribution_threshold": "10g"},
        )
        assert standard_dimension_assertion(
            config,
            backend_api,
            messages,
            repo_client,
            schema,
            data_db,
            OFFLOAD_DIM,
            bucket_column="PROD_ID",
        )
        assert synapse_distribution_assertion(
            backend_api, messages, data_db, OFFLOAD_DIM, "HASH"
        )

        # Offload the dimension without --bucket-hash-column and low threshold, expect HASH.
        options = {
            "owner_table": schema + "." + OFFLOAD_DIM,
            "reset_backend_table": True,
            "execute": True,
        }
        run_offload(
            options,
            config,
            messages,
            config_overrides={"hash_distribution_threshold": "0.1k"},
        )
        assert standard_dimension_assertion(
            config,
            backend_api,
            messages,
            repo_client,
            schema,
            data_db,
            OFFLOAD_DIM,
            bucket_column="PROD_ID",
        )
        assert synapse_distribution_assertion(
            backend_api, messages, data_db, OFFLOAD_DIM, "HASH"
        )
