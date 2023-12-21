import pytest

from goe.offload.backend_api import IMPALA_NOSHUFFLE_HINT
from goe.offload import offload_constants
from goe.offload.offload_functions import (
    convert_backend_identifier_case,
    data_db_name,
)
from goe.offload.offload_messages import VVERBOSE
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)

from tests.integration.scenarios.assertion_functions import (
    hint_text_in_log,
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
    get_frontend_testing_api,
    get_test_messages,
)


PARALLEL_V_DIM = "STORY_PARALLEL_VER_DIM"


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


def log_test_marker(messages, test_id):
    messages.log(test_id, detail=VVERBOSE)


def test_offload_verification_parallel(config, schema, data_db):
    id = "test_offload_verification_parallel"
    messages = get_test_messages(config, id)

    if config.db_type != offload_constants.DBTYPE_ORACLE:
        messages.log(f"Skipping {id} for system: {config.db_type}")
        return

    backend_api = get_backend_testing_api(config, messages)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)
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
            schema, PARALLEL_V_DIM
        ),
        python_fns=[
            lambda: drop_backend_test_table(
                config, backend_api, messages, data_db, PARALLEL_V_DIM
            ),
        ],
    )

    # Offload with count verification parallelism=3.
    # Disables data sampling to minimize risk of other hints being matched.
    options = {
        "owner_table": schema + "." + PARALLEL_V_DIM,
        "verify_parallelism": 3,
        "data_sample_pct": 0,
        "reset_backend_table": True,
    }
    log_test_marker(messages, f"{id}1")
    run_offload(options, config, messages, config_overrides={"execute": False})
    assert hint_text_in_log(messages, config, 3, f"{id}1")

    # Offload with verification parallelism=1.
    options = {
        "owner_table": schema + "." + PARALLEL_V_DIM,
        "verify_parallelism": 1,
        "data_sample_pct": 0,
        "reset_backend_table": True,
    }
    log_test_marker(messages, f"{id}2")
    run_offload(options, config, messages, config_overrides={"execute": False})
    assert hint_text_in_log(messages, config, 1, f"{id}2")

    # Offload with verification parallelism=0.
    options = {
        "owner_table": schema + "." + PARALLEL_V_DIM,
        "verify_parallelism": 0,
        "data_sample_pct": 0,
        "reset_backend_table": True,
    }
    log_test_marker(messages, f"{id}3")
    run_offload(options, config, messages, config_overrides={"execute": False})
    assert hint_text_in_log(messages, config, 0, f"{id}3")

    # Offload with aggregation verification parallelism=4.
    options = {
        "owner_table": schema + "." + PARALLEL_V_DIM,
        "verify_parallelism": 4,
        "verify_row_count": "aggregate",
        "data_sample_pct": 0,
        "reset_backend_table": True,
        "create_backend_db": True,
    }
    log_test_marker(messages, f"{id}4")
    run_offload(options, config, messages)
    assert hint_text_in_log(messages, config, 4, f"{id}4")
