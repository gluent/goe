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

# Third Party Libraries
import orjson as json
from fastapi import APIRouter, status

# GOE
from goe.listener import schemas, services, utils
from goelib_contrib.asyncer import asyncify

STATUS_OK = "OK"

router = APIRouter()


@router.get(
    "/status/",
    response_model=schemas.HealthCheck,
    summary="Simple health check.",
    status_code=status.HTTP_200_OK,
)
async def health_check():
    """Run basic application health check.

    If the application is up and running then this endpoint will return simple
    response with status ok. Moreover, if it has Redis enabled then connection
    to it will be tested. If Redis ping fails, then this endpoint will return
    502 HTTP error.


    Returns:
        response (HealthCheck): HealthCheck model object instance.

    Raises:
        HTTPException: If applications has enabled Redis and can not connect
            to it. NOTE! This is the custom exception, not to be mistaken with
            FastAPI.HTTPException class.

    """
    return {"status": STATUS_OK}


@router.get(
    "/config/",
    status_code=status.HTTP_200_OK,
    summary="Returns all shareable configuration infomration about the running listener. ",
    response_model=schemas.ListenerConfig,
    operation_id="getConfiguration",
)
async def get_configuration():
    """Get a listener group id.

    Returns:
        response (ListenerGroup)

    Raises:
        HTTPException: If listner cannot determine the group id.

    """

    return {
        "endpoint_id": services.system.generate_listener_endpoint_id(),
        "listener_group_id": services.system.generate_listener_group_id(),
        "db_unique_name": services.system.get_db_unique_name(),
        "active_listeners": await services.system.get_active_listener_endpoints(),
        "version": services.system.get_version(),
        "frontend_type": services.system.get_frontend_type(),
        "backend_type": services.system.get_backend_type(),
        "offload_options": json.dumps(schemas.OffloadOptions.schema(by_alias=False)),
        "present_options": None,
        "prepare_options": None,
    }


@router.get(
    "/schemas/",
    response_model=schemas.OffloadableSchemas,
    summary="Get list of schemas.",
    status_code=status.HTTP_200_OK,
    operation_id="getSchemas",
)
def get_offloadable_schemas():
    """Get list of schemas.

    Returns:
        response (SchemaList): SchemaList model object instance.

    Raises:
        HTTPException: If listener cannot determine the version.

    """
    schemas = services.system.get_schemas()
    return {"count": len(schemas), "results": schemas}


@router.get(
    "/schemas/{schema_name}/",
    response_model=schemas.TableDetails,  # New Pydantic model
    summary="Get list of Schema's tables.",
    status_code=status.HTTP_200_OK,
    operation_id="getSchemaTables",
)
def get_offloadable_tables(schema_name: str):
    """Get list of tables details.

    Returns:
        response (TableList): TableDetails model object instance.

    Raises:
        HTTPException: If listener cannot determine the version.

    """
    tables = services.system.get_schema_tables(schema_name)
    return {"count": len(tables), "results": tables}


@router.get(
    "/schemas/{schema_name}/{table_name}/columns/",
    response_model=schemas.ColumnDetails,  # New Pydantic model
    summary="Get list of Columns for a table.",
    status_code=status.HTTP_200_OK,
    operation_id="getColumnDetails",
)
def get_table_columns(schema_name: str, table_name: str):
    """Get list of Column details.

    Returns:
        response (ColumnList): ColumnDetails model object instance.

    Raises:
        HTTPException: If listener cannot determine the version.

    """
    columns = services.system.get_table_columns(schema_name, table_name)
    return {"count": len(columns), "results": columns}


# Get table partitions and subpartitions
@router.get(
    "/schemas/{schema_name}/{table_name}/partitions/",
    response_model=schemas.PartitionDetails,  # New Pydantic model
    summary="Get list of Partitions and SubPartitions for a table.",
    status_code=status.HTTP_200_OK,
    operation_id="getTablePartitions",
)
async def get_table_partitions(schema_name: str, table_name: str):
    """Get list of Partition and SubPartition details for a particular schema.

    Returns:
        response (PartitionDetails): PartitionDetails model object instance.

    Raises:
        HTTPException: If listener cannot determine the version.

    """
    partitions = await asyncify(services.system.get_table_partitions)(
        schema_name, table_name
    )
    subpartitions = await asyncify(services.system.get_table_subpartitions)(
        schema_name, table_name
    )
    # partitions_obj = jsonable_encoder(partitions)
    for partition in partitions:
        partition.update(
            {
                "subpartitions": utils.groupby(
                    lambda partition: partition.partition_name, subpartitions
                ).get(
                    partition.get("partition_name", None),
                    [],
                )
            }
        )
    return {"count": len(partitions), "results": partitions}
