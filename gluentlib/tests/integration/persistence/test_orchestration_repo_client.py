"""
TestOrchestrationRepoClient: Unit test Orchestration Repo client library.
Excludes metadata specific methods, they are tested in TestOrchestrationMetadata.
"""

from unittest import TestCase, main

from numpy import datetime64

from gluentlib.config.orchestration_config import OrchestrationConfig
from gluentlib.persistence.factory.orchestration_repo_client_factory import (
    orchestration_repo_client_factory,
)
from gluentlib.offload.offload_messages import OffloadMessages
from gluentlib.offload.offload_source_data import OffloadSourcePartition
from gluentlib.orchestration import command_steps, orchestration_constants
from gluentlib.orchestration.execution_id import ExecutionId


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
        config = OrchestrationConfig.from_dict({"verbose": False, "execute": True})
        execution_id = ExecutionId()
        messages = OffloadMessages.from_options(config, execution_id=execution_id)
        client = orchestration_repo_client_factory(config, messages)

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
        # TODO log name is no longer passed in
        cid = client.start_command(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            "/tmp/offload_unit_test.log",
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
        config = OrchestrationConfig.from_dict({"verbose": False, "execute": True})
        execution_id = ExecutionId()
        messages = OffloadMessages.from_options(config, execution_id=execution_id)
        client = orchestration_repo_client_factory(config, messages)
        # Start an API based Offload
        # TODO log name is no longer passed in
        cid = client.start_command(
            execution_id,
            orchestration_constants.COMMAND_OFFLOAD,
            "/tmp/offload_unit_test_api.log",
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

        # Log a recursive present step within an offload
        sid = client.start_command_step(
            execution_id,
            orchestration_constants.COMMAND_PRESENT,
            command_steps.STEP_CREATE_HYBRID_VIEW,
        )

        # Finish the step
        client.end_command_step(
            sid, orchestration_constants.COMMAND_ERROR, {"some_attribute": 456}
        )

        # Finish the command
        client.end_command(cid, orchestration_constants.COMMAND_SUCCESS)


if __name__ == "__main__":
    main()
