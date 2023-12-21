# Standard Library
import datetime
from typing import Generic, List, TypeVar

# Third Party Libraries
from pydantic import BaseModel as PydanticBaseModel
from pydantic import BaseSettings as PydanticBaseSettings
from pydantic import SecretStr
from pydantic.generics import GenericModel

# GOE
from goe.util.json_tools import (
    convert_field_to_camel_case,
    deserialize_object,
    encode_datetime_object,
    serialize_object,
)


class BaseSchema(PydanticBaseModel):
    """Base pydantic schema"""

    class Config:
        json_loads = deserialize_object
        json_dumps = serialize_object
        arbitrary_types_allowed = True
        json_encoders = {
            datetime.datetime: encode_datetime_object,
            SecretStr: lambda secret: secret.get_secret_value() if secret else None,
        }
        orm_mode = True

class CamelizedBaseSchema(BaseSchema):
    """Camelized Base pydantic schema"""

    class Config:
        allow_population_by_field_name = True
        alias_generator = convert_field_to_camel_case


class BaseSettings(PydanticBaseSettings):
    """Base pydantic schema"""

    class Config:
        """Advisor Configuration to validate settings.

        More details can be found in pydantic documentation:
        https://pydantic-docs.helpmanual.io/usage/settings/

        """

        json_loads = deserialize_object
        json_dumps = serialize_object
        arbitrary_types_allowed = True
        json_encoders = {
            datetime.datetime: encode_datetime_object,
            SecretStr: lambda secret: secret.get_secret_value() if secret else None,
        }

        validate_assignment = True
        # env_file = ".env"
        # env_file_encoding = "utf-8"


class Message(BaseSchema):
    """Properties included on a generic response message"""

    message: str


PM = TypeVar("PM", bound=BaseSchema)


class TotaledResults(GenericModel, Generic[PM]):
    """Provides count and result of resultset"""

    count: int
    results: List[PM]


class PaginatedResults(GenericModel, Generic[PM]):
    """Provides count, result, and page information of resultset"""

    count: int
    limit: int
    offset: int
    results: List[PM]
