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

"""
TestOrchestrationRepoClient: Unit test Orchestration Repo client library.
Excludes metadata specific methods, they are tested in TestOrchestrationMetadata.
"""

from unittest import TestCase, main

from numpy import datetime64

from goe.config.orchestration_config import OrchestrationConfig
from goe.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from goe.offload.offload_source_data import OffloadSourcePartition
from goe.orchestration import command_steps, orchestration_constants
from goe.orchestration.execution_id import ExecutionId

from tests.testlib.test_framework.test_functions import (
    get_test_messages,
)


GB = 1024**3


class TestOrchestrationRepoClient(TestCase):
    """
    TestOrchestrationRepoClient: Unit test Orchestration Repo client library.
    Excludes metadata specific methods, they are tested in TestOrchestrationMetadata.
    """

    def test_orchestration_command_logging_cli(self):
        """
        Tests a command is if launched from the CLI. Pretends to offload a multi chunk partitioned table.
        """
        # execute=True because we want to actually insert and update the repo records for this test.
        config = OrchestrationConfig.from_dict({"verbose": False})
        execution_id = ExecutionId()
        messages = get_test_messages(
            config, "test_orchestration_command_logging_cli", execution_id=execution_id
        )
        client = orchestration_repo_client_factory(
            config,
            messages,
            dry_run=False,
            trace_action="repo_client(test_orchestration_command_logging_cli)",
        )

        offload_partitions = [
            OffloadSourcePartition(
                "P1",
                "2020-01-01",
                (datetime64("2020-01-01"),),
                "2020-01-01",
                1e6 * 30,
                GB * 300,
                None,
                None,
            ),
            OffloadSourcePartition(
                "P2",
                "2020-02-01",
                (datetime64("2020-02-01"),),
                "2020-02-01",
                1e6 * 10,
                GB * 100,
                None,
                None,
            ),
            OffloadSourcePartition(
                "P3",
                "2020-02-01",
                (datetime64("2020-02-01"),),
                "2020-02-01",
                1e6 * 5,
                GB * 50,
                None,
                None,
            ),
        ]

        # Start a CLI based Offload
        cid = client.start_command(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            "./offload -t acme.unit_test_table",
            {"owner_table": "acme.unit_test_table"},
        )

        # Log start of partitionwise offload transport
        chid = client.start_offload_chunk(
            execution_id,
            "acme",
            "unit_test_table",
            "acme",
            "unit_test_table",
            chunk_number=1,
            offload_partitions=offload_partitions[:1],
            offload_partition_level=1,
        )

        # Log transport step
        sid = client.start_command_step(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            command_steps.STEP_STAGING_TRANSPORT,
        )

        # Finish the step
        client.end_command_step(
            sid, orchestration_constants.COMMAND_SUCCESS, {"some_attribute": 123}
        )

        # Log an offload step
        sid = client.start_command_step(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            command_steps.STEP_FINAL_LOAD,
        )

        # Finish the step
        client.end_command_step(
            sid, orchestration_constants.COMMAND_SUCCESS, {"some_attribute": 123}
        )

        # Log completion of offload transport (400G in frontend, grew to 600G when staged and finally 300G in backend)
        client.end_offload_chunk(
            chid,
            orchestration_constants.COMMAND_SUCCESS,
            int(1e6 * 40),
            GB * 400,
            GB * 600,
            GB * 300,
        )

        # Log start of 2nd partition chunk
        chid = client.start_offload_chunk(
            execution_id,
            "acme",
            "unit_test_table",
            "acme",
            "unit_test_table",
            chunk_number=2,
            offload_partitions=offload_partitions[1:],
            offload_partition_level=1,
        )

        # Log transport step
        sid = client.start_command_step(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            command_steps.STEP_STAGING_TRANSPORT,
        )

        # Finish the step after it failed
        client.end_command_step(sid, orchestration_constants.COMMAND_ERROR)

        # Log failed completion of chunk
        client.end_offload_chunk(
            chid, orchestration_constants.COMMAND_ERROR, int(1e6 * 40), GB * 400
        )

        # Finish the Offload command
        client.end_command(cid, orchestration_constants.COMMAND_SUCCESS)

    def test_orchestration_command_logging_api(self):
        """
        Tests a command is if launched from an API. Pretends to offload a non-partitioned table.
        """
        # execute=True because we want to actually insert and update the repo records for this test.
        config = OrchestrationConfig.from_dict({"verbose": False})
        execution_id = ExecutionId()
        messages = get_test_messages(
            config, "test_orchestration_command_logging_api", execution_id=execution_id
        )
        client = orchestration_repo_client_factory(
            config,
            messages,
            dry_run=False,
            trace_action="repo_client(test_orchestration_command_logging_api)",
        )
        # Start an API based Offload
        cid = client.start_command(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            {
                "owner_table": "acme.unit_test_table",
                "execute": True,
                "reset_backend_table": True,
            },
            {
                "owner_table": "acme.unit_test_table",
                "execute": True,
                "reset_backend_table": True,
                "a_bunch_of_other_stuff": 123,
            },
        )

        # Log start of non-partitioned offload transport
        chid = client.start_offload_chunk(
            execution_id, "acme", "unit_test_table", "acme", "unit_test_table"
        )

        # Log an offload step
        sid = client.start_command_step(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            command_steps.STEP_STAGING_TRANSPORT,
        )

        # Finish the step
        client.end_command_step(
            sid, orchestration_constants.COMMAND_SUCCESS, {"some_attribute": 123}
        )

        # Log completion of offload transport (400G in frontend, grew to 600G when staged and finally 300G in backend)
        client.end_offload_chunk(
            chid,
            orchestration_constants.COMMAND_SUCCESS,
            int(1e6 * 40),
            GB * 400,
            GB * 600,
            GB * 300,
        )

        # Finish the step
        client.end_command_step(
            sid, orchestration_constants.COMMAND_ERROR, {"some_attribute": 456}
        )

        # Finish the command
        client.end_command(cid, orchestration_constants.COMMAND_SUCCESS)


if __name__ == "__main__":
    main()
