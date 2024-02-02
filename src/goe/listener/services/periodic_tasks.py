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
import anyio
from pydantic import UUID3

# GOE
from goe.listener import schemas, utils
from goe.listener.config import settings
from goe.listener.services.system import system
from goe.orchestration.execution_id import ExecutionId
from goelib_contrib.asyncer import asyncify
from goelib_contrib.worker import monitored_job

group_id: UUID3 = system.generate_listener_group_id()
endpoint_id: UUID3 = system.generate_listener_endpoint_id()

logger = logging.getLogger()


async def publish_heartbeat(context) -> None:
    listener_group_id: UUID3 = context["listener_group_id"]
    endpoint_id: UUID3 = context["endpoint_id"]
    local_ip: str = utils.system.get_ip_address()
    await utils.cache.set(
        f"goe:listener:endpoints:{listener_group_id}:{endpoint_id}",
        f"{'https' if settings.ssl_enabled else 'http'}://{local_ip}:{settings.port}",
        settings.heartbeat_interval * 2,
    )
    logger.debug("Published Heartbeat")


async def publish_schemas(context) -> None:
    """Offload the environment to the GOE Listener.

    Args:
        environment_id (int): The id of the environment to offload.
        options (dict): The options to pass to the offload command.
        db: The database connection.
    Returns:
        dict: The result of the offload command.
    """
    listener_group_id: UUID3 = context["listener_group_id"]
    offloadable_schemas = await asyncify(system.get_schemas)()

    await utils.cache.set(
        f"goe:listener:metadata:{listener_group_id}:schemas",
        schemas.OffloadableSchemas.parse_obj(
            {"count": len(offloadable_schemas), "results": offloadable_schemas}
        ).json(),
        ttl=10000,
    )
    concurrency_limit = anyio.Semaphore(4)
    async with anyio.create_task_group() as tg:
        for schema in offloadable_schemas:
            schema_name = schema.get("schema_name", None)
            if schema_name:
                tg.start_soon(
                    _publish_schema, listener_group_id, schema_name, concurrency_limit
                )


# all functions take in context dict and kwargs
@monitored_job
async def publish_schema_tables(context) -> None:
    """Offload the environment to the GOE Listener.

    Args:
        environment_id (int): The id of the environment to offload.
        options (dict): The options to pass to the offload command.
        db: The database connection.
    Returns:
        dict: The result of the offload command.
    """
    listener_group_id: UUID3 = context["listener_group_id"]
    offloadable_schemas = await asyncify(system.get_schemas)()

    await utils.cache.set(
        f"goe:listener:metadata:{listener_group_id}:schemas",
        schemas.OffloadableSchemas.parse_obj(
            {"count": len(offloadable_schemas), "results": offloadable_schemas}
        ).json(),
        ttl=10000,
    )
    concurrency_limit = anyio.Semaphore(4)
    async with anyio.create_task_group() as tg:
        for schema in offloadable_schemas:
            schema_name = schema.get("schema_name", None)
            if schema_name:
                tg.start_soon(
                    _publish_schema_tables,
                    listener_group_id,
                    schema_name,
                    concurrency_limit,
                )


async def publish_command_executions(context) -> None:
    """Offload the environment to the GOE Listener.

    Args:
        environment_id (int): The id of the environment to offload.
        options (dict): The options to pass to the offload command.
        db: The database connection.
    Returns:
        dict: The result of the offload command.
    """
    listener_group_id: UUID3 = context["listener_group_id"]
    command_executions = await asyncify(system.get_command_executions)()
    all_steps = await asyncify(system.get_command_execution_steps)(
        execution_id=None
    )  # fetch for multiple execution ID`s
    steps_by_execution_id = utils.groupby(
        lambda pair: ExecutionId.from_bytes(pair.get("execution_id")).as_str(),
        all_steps,
    )
    for command_execution in command_executions:
        command_execution.update(
            {
                "steps": steps_by_execution_id.get(
                    ExecutionId.from_bytes(command_execution["execution_id"]).as_str(),
                    [],
                )
            }
        )
    await utils.cache.set(
        f"goe:listener:metadata:{listener_group_id}:command-executions",
        schemas.CommandExecutions.parse_obj(
            {"count": len(command_executions), "results": command_executions}
        ).json(),
        ttl=10000,
    )


async def _publish_schema(
    listener_group_id,
    schema_name,
    concurrency_limit: anyio.Semaphore,
) -> None:
    async with concurrency_limit:
        schema_tables = await asyncify(system.get_schema_tables)(schema_name)
        await utils.cache.set(
            f"goe:listener:metadata:{listener_group_id}:schemas:{schema_name}",
            schemas.TableDetails.parse_obj(
                {"count": len(schema_tables), "results": schema_tables}
            ).json(),
            ttl=86400,
        )


async def _publish_schema_tables(
    listener_group_id,
    schema_name,
    table_name,
    concurrency_limit: anyio.Semaphore,
) -> None:
    async with concurrency_limit:
        schema_table_columns = await asyncify(system.get_table_columns)(
            schema_name, table_name
        )

        await utils.cache.set(
            f"goe:listener:metadata:{listener_group_id}:schemas:{schema_name}:{table_name}:columns",
            schemas.ColumnDetails.parse_obj(
                {"count": len(schema_table_columns), "results": schema_table_columns}
            ).json(),
            ttl=86400,
        )
        schema_table_partitions = await asyncify(system.get_table_partitions)(
            schema_name, table_name
        )
        await utils.cache.set(
            f"goe:listener:metadata:{listener_group_id}:schemas:{schema_name}:{table_name}:partitions",
            schemas.PartitionDetails.parse_obj(
                {
                    "count": len(schema_table_partitions),
                    "results": schema_table_partitions,
                }
            ).json(),
            ttl=86400,
        )
