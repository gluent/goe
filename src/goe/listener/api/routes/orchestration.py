# -*- coding: utf-8 -*-

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

# Standard Library
import logging

# Third Party Libraries
from anyio import Path, open_file
from fastapi import APIRouter
from pydantic import UUID4
from starlette import status

# GOE
from goe.listener import exceptions, schemas, services, utils
from goe.orchestration.execution_id import ExecutionId
from goelib_contrib.asyncer import asyncify, run_and_detach

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


router = APIRouter()


@router.get(
    "/executions/",
    response_model=schemas.CommandExecutions,
    summary="Fetches executions from the environment",
    status_code=status.HTTP_200_OK,
    operation_id="getCommandExecutions",
)
async def get_command_executions(include_steps: bool = False):
    """Initiate an offload operation.

    Returns:
        response (OrchestrationResponse): object instance of OrchestrationResponse.

    Raises:
        HTTPException: if there's an error

    """

    command_executions = services.system.get_command_executions()
    if include_steps:
        all_steps = await asyncify(services.system.get_command_execution_steps)(
            execution_id=None
        )  # fetch for multiple execution ID`s
        steps = utils.groupby(
            lambda pair: ExecutionId.from_bytes(pair.get("execution_id")).as_str(),
            all_steps,
        )
        for command_execution in command_executions:
            command_execution.update(
                {
                    "steps": steps.get(
                        ExecutionId.from_bytes(
                            command_execution["execution_id"]
                        ).as_str(),
                        [],
                    )
                }
            )
    return {"count": len(command_executions), "results": command_executions}


@router.get(
    "/executions/{execution_id}/",
    response_model=schemas.CommandExecution,
    summary="Fetches information about a command execution.",
    status_code=status.HTTP_200_OK,
    operation_id="getCommandExecution",
)
async def get_command_execution(execution_id: UUID4, include_steps: bool = False):
    """Initiate an offload operation.

    Returns:
        response (OrchestrationResponse): object instance of OrchestrationResponse.

    Raises:
        HTTPException: if there's an error

    """

    execution_identifier = ExecutionId.from_uuid(execution_id)
    command_execution = await asyncify(services.system.get_command_execution)(
        execution_identifier
    )
    if not command_execution:
        raise exceptions.CommandExecutionNotFound(execution_id)

    if include_steps:
        steps = await asyncify(services.system.get_command_execution_steps)(
            execution_identifier
        )
        if steps:
            command_execution.update({"steps": steps})
    return command_execution


@router.get(
    "/executions/{execution_id}/execution-log/",
    response_model=schemas.CommandExecutionLog,
    summary="Fetches execution log file related to a specific command execution.",
    status_code=status.HTTP_200_OK,
    operation_id="getCommandExecutionExecLog",
)
async def get_command_execution_log(execution_id: UUID4):
    """Returns execution log file related to a specific command execution.

    Returns:
        response (PlainTextResponse): object instance of PlainTextResponse.

    Raises:
        File: if there's an error
    """

    execution_identifier = ExecutionId.from_uuid(execution_id)
    command_execution = await asyncify(services.system.get_command_execution)(
        execution_identifier
    )
    if not command_execution:
        raise exceptions.CommandExecutionNotFound(execution_id)

    # read command_log_path from command_execution
    command_log_path = command_execution.get("command_log_path", "")
    exists = await Path(command_log_path).exists()
    if exists:
        file_name = command_log_path.split("/")[-1].replace(".log", "")
        async with await open_file(Path(command_log_path)) as log_file:
            contents = await log_file.read()
        return {"name": file_name, "is_file": True, "message": contents}
    # we will return an empty log file here instead of raising an exception.
    # we may want to add some additional properties to the ListenerResponse model
    # to better indicate what type of message is contained in the log
    # - this is primarily done so that console will mark a record as complete and stop trying to fetch it.
    # -  It's easier to send back a valid response than add the logic to
    # parse the exception message on the console side.
    return {
        "name": file_name,
        "is_file": True,
        "message": f"Log file for execution {execution_identifier.id} not found.",
    }


@router.post(
    "/offload/",
    response_model=schemas.CommandScheduled,
    summary="Initiates an offload operation.",
    status_code=status.HTTP_200_OK,
    operation_id="executeOffloadCommand",
)
def execute_offload_command(parameters: schemas.OffloadOptions):
    """Initiate an offload operation.

    Returns:
        response (OrchestrationResponse): object instance of OrchestrationResponse.

    Raises:
        HTTPException: if there's an error

    """

    utils.orchestrate.check_for_running_command(parameters.owner_table)
    execution_identifier = ExecutionId()

    logger.info(f"Submitting offload: {str(execution_identifier)}")
    run_and_detach(services.orchestration_runner.offload)(
        params=parameters.dict(exclude_unset=True), execution_id=execution_identifier
    )

    return {"execution_id": execution_identifier.id}
