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
from typing import Generic, List, TypeVar

# Third Party Libraries
from pydantic.generics import GenericModel

# GOE
from goe.listener.schemas.base import BaseSchema

BaseSchemaType = TypeVar("BaseSchemaType", bound=BaseSchema)


class TotaledResults(GenericModel, Generic[BaseSchemaType]):
    """Provides count and result of resultset"""

    count: int
    results: List[BaseSchemaType]


class PaginatedResults(GenericModel, Generic[BaseSchemaType]):
    """Provides count, result, and page information of resultset"""

    count: int
    limit: int
    offset: int
    results: List[BaseSchemaType]
