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

"""Metadata Schemas"""

# Standard Library
import datetime
from typing import Any, Dict, List, Optional

# Third Party Libraries
from pydantic import UUID3, AnyHttpUrl, Json

# GOE
from goe.listener.schemas.base import BaseSchema, TotaledResults


class HealthCheck(BaseSchema):
    """Health Check response model definition.

    Attributes:
        status(str): Strings are accepted as-is, int float and Decimal are
            coerced using str(v), bytes and bytearray are converted using
            v.decode(), enums inheriting from str are converted using
            v.value, and all other types cause an error.

    Raises:
        pydantic.error_wrappers.ValidationError: If any of provided attribute
            doesn't pass type validation.

    """

    status: str

    class Config:
        """Config sub-class needed to extend/override the generated JSON schema.

        More details can be found in pydantic documentation:
        https://pydantic-docs.helpmanual.io/usage/schema/#schema-customization

        """

        @staticmethod
        def schema_extra(schema: Dict[str, Any]) -> None:
            """Post-process the generated schema.

            Mathod can have one or two positional arguments. The first will be
            the schema dictionary. The second, if accepted, will be the model
            class. The callable is expected to mutate the schema dictionary
            in-place; the return value is not used.

            Args:
                schema(Dict[str, Any]): The schema dictionary.

            """
            # Override schema description, by default is taken from docstring.
            schema["description"] = "Health check response model."


class ListenerConfig(BaseSchema):
    """Listener Configuration

    Attributes:
        listener_group_id (UUID3): Currently installed version of GOE running.

    Raises:
        pydantic.error_wrappers.ValidationError: If any of provided attribute
            doesn't pass type validation.

    """

    endpoint_id: UUID3
    listener_group_id: UUID3
    version: str
    db_unique_name: str
    active_listeners: List[AnyHttpUrl] = []
    frontend_type: str
    backend_type: str
    offload_options: Optional[Json]
    present_options: Optional[Json]
    prepare_options: Optional[Json]


class OffloadableSchema(BaseSchema):
    schema_name: str
    hybrid_schema_exists: bool
    table_count: int
    schema_size_in_bytes: float


class OffloadableSchemas(TotaledResults[OffloadableSchema]):
    """Totaled Offload Results"""


class ColumnDetail(BaseSchema):
    """Column Details"""

    column_name: str
    data_type: str
    data_precision: Optional[int]
    data_scale: Optional[int]
    is_nullable: bool
    partition_position: Optional[int]
    subpartition_position: Optional[int]


class ColumnDetails(TotaledResults[ColumnDetail]):
    """Totaled Column Details Results"""


class SubPartitionDetail(BaseSchema):
    """SubPartition Details"""

    subpartition_name: str
    subpartition_position: int
    partition_name: str
    partition_position: int
    high_values_individual: Optional[List[str]] = []
    partition_size: int
    num_rows: int


class SubPartitionDetails(TotaledResults[SubPartitionDetail]):
    """Totaled Partition Details Results"""


class PartitionDetail(BaseSchema):
    """Partition Details"""

    partition_name: str
    partition_position: int
    subpartition_count: Optional[int]
    subpartition_names: Optional[List[str]] = []
    high_values_individual: Optional[List[str]] = []
    partition_size: int
    num_rows: int
    is_subpartitioned: Optional[bool]
    subpartitions: Optional[List[SubPartitionDetail]] = []

    @classmethod
    def from_orm(cls, obj: Any) -> "PartitionDetail":
        """
        Format Partiton details for the schema
        """
        obj.is_subpartitioned = False

        # `obj` is the orm model instance
        if getattr(obj, "subpartition_count", 0) > 0:
            obj.is_subpartitioned = True
        return super().from_orm(obj)


class PartitionDetails(TotaledResults[PartitionDetail]):
    """Totaled Partition Details Results"""


class TableDetail(BaseSchema):
    """Table Details"""

    table_name: str
    table_size_in_bytes: int
    table_offloaded_size_in_bytes: Optional[int] = 0
    table_reclaimed_size_in_bytes: Optional[int] = 0
    estimated_row_count: Optional[int]
    statistics_last_gathered_on: Optional[datetime.datetime]
    partitioning_type: Optional[str]
    subpartitioning_type: Optional[str]
    table_compression: Optional[str]
    table_compress_for: Optional[str]
    is_offloadable: bool
    is_offloaded: bool
    is_compressed: bool
    is_partitioned: bool
    is_subpartitioned: bool
    reason_not_offloadable: Optional[str]
    column_details: Optional[List[ColumnDetail]]
    partition_details: Optional[List[PartitionDetail]]


class TableDetails(TotaledResults[TableDetail]):
    """Totaled Table Details Results"""
