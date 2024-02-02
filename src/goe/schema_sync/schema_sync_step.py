# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import TYPE_CHECKING

from goe.util.ora_query import get_oracle_connection
from goe.offload.factory.backend_api_factory import backend_api_factory

if TYPE_CHECKING:
    from goe.offload.offload_messages import OffloadMessages
    from goe.orchestration.execution_id import ExecutionId
    from goe.persistence.orchestration_repo_client import (
        OrchestrationRepoClientInterface,
    )


class SchemaSyncStep(object):
    """Base class for individual steps within the Schema Sync routine. The purpose of this
    class is to enforce some basic invariants that each step must follow, and to implement
    some boilerplate routines in terms of those invariants
    """

    def __init__(
        self,
        name,
        options,
        orchestration_options,
        messages: "OffloadMessages",
        execution_id: "ExecutionId",
        repo_client: "OrchestrationRepoClientInterface",
    ):
        self.name = name
        self._options = options
        self._orchestration_options = orchestration_options
        self._messages = messages
        self._execution_id = execution_id
        self._repo_client = repo_client
        self._ora_adm_conn = get_oracle_connection(
            self._orchestration_options.ora_adm_user,
            self._orchestration_options.ora_adm_pass,
            self._orchestration_options.rdbms_dsn,
            self._orchestration_options.use_oracle_wallet,
        )
        self._backend_api = backend_api_factory(
            self._orchestration_options.target,
            self._orchestration_options,
            self._messages,
            dry_run=(not self._options.execute),
        )

    def __del__(self):
        """Clean up connections"""
        if hasattr(self, "_ora_adm_conn"):
            self._ora_adm_conn.close()

    def check_preconditions(self, run_params):
        """Assert that all of the bindings required by this step are present *before* running this step"""
        assert "type" in run_params, "No change type detected"
        assert (
            run_params["type"] == self.name
        ), 'Incorrect change type "{}" detected'.format(run_params["type"])

    def check_postconditions(self, run_commands):
        """Assert that all of the bindings that are supposed to be set as results by this step are
        present *after* running this step
        """
        if run_commands:
            assert isinstance(
                run_commands, list
            ), "Commands returned from step must be list"

    def run(self, run_params):
        """Delegate to the subclass's _do_run implementation to perform the actions entailed by this step,
        checking pre-conditions before invoking _do_run
        """
        self.check_preconditions(run_params)
        run_status, run_commands = self._do_run(run_params)
        self.check_postconditions(run_commands)

        return run_status, run_commands
