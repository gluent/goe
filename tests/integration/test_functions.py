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

from functools import lru_cache
import os
from typing import TYPE_CHECKING

from goe.goe import OffloadOperation
from goe.config.orchestration_config import OrchestrationConfig
from goe.offload.offload_constants import DBTYPE_MSSQL, DBTYPE_TERADATA
from goe.offload.offload_messages import OffloadMessages, VVERBOSE
from goe.orchestration.orchestration_runner import OrchestrationRunner

from tests.testlib.test_framework.offload_test_messages import OffloadTestMessages

if TYPE_CHECKING:
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )
    from testlib.test_framework.frontend_testing_api import FrontendTestingApiInterface


def build_current_options():
    return OrchestrationConfig.from_dict({"verbose": False})


@lru_cache(maxsize=None)
def cached_current_options():
    return build_current_options()


def build_offload_operation(operation_dict=None, options=None, messages=None):
    if options:
        offload_options = options
    else:
        offload_options = build_current_options()
    if messages:
        offload_messages = messages
    else:
        offload_messages = OffloadMessages()
    if not operation_dict:
        operation_dict = {"owner_table": "x.y"}
    offload_operation = OffloadOperation.from_dict(
        operation_dict, offload_options, offload_messages
    )
    return offload_operation


def get_default_test_user():
    return os.environ.get("GOE_TEST_USER", "GOE_TEST")


@lru_cache(maxsize=None)
def cached_default_test_user():
    return get_default_test_user()


def get_default_test_user_pass():
    return os.environ.get("GOE_TEST_USER_PASS", "GOE_TEST")


def run_offload(option_dict: dict) -> bool:
    return OrchestrationRunner().offload(
        option_dict,
    )


def get_offload_home():
    offload_home = os.environ.get("OFFLOAD_HOME")
    assert offload_home, "OFFLOAD_HOME must be set in order to run tests"
    return offload_home


def run_setup_ddl(
    config: "OrchestrationRepoClientInterface",
    frontend_api: "FrontendTestingApiInterface",
    messages: "OffloadTestMessages",
    sqls: list[str],
    trace_action: str = "run_setup_ddl",
):
    test_schema = get_default_test_user()
    test_schema_pass = get_default_test_user_pass()
    with frontend_api.create_new_connection_ctx(
        test_schema,
        test_schema_pass,
        trace_action_override=trace_action,
    ) as sh_test_api:
        if config.db_type == DBTYPE_MSSQL:
            sh_test_api.execute_ddl("BEGIN TRAN")
        for sql in sqls:
            messages.log(f"Setup SQL: {sql}", detail=VVERBOSE)
            try:
                sh_test_api.execute_ddl(sql)
            except Exception as exc:
                if "does not exist" in str(exc) and sql.upper().startswith("DROP"):
                    messages.log("Ignoring: " + str(exc), detail=VVERBOSE)
                else:
                    messages.log(str(exc))
                    raise
        if config.db_type != DBTYPE_TERADATA:
            # We have autocommit enabled on Teradata:
            #   COMMIT WORK not allowed for a DBC/SQL session. (-3706)
            sh_test_api.execute_ddl("COMMIT")
