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
