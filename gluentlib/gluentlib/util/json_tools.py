# Standard Library
import datetime
from typing import Any, Union

# Third Party Libraries
import orjson


# orjson.dumps returns bytearray, so you'll can't pass it directly as json_serializer
def serialize_object(obj) -> str:
    """
    Encodes json with the optimized ORJSON package

    orjson.dumps returns bytearray, so you can't pass it directly as json_serializer
    """
    return orjson.dumps(
        obj,
        option=orjson.OPT_NAIVE_UTC | orjson.OPT_SERIALIZE_NUMPY,
    ).decode()


def deserialize_object(obj: Union[bytes, bytearray, memoryview, str]) -> Any:
    """
    Decodes to an object with the optimized ORJSON package

    orjson.dumps returns bytearray, so you can't pass it directly as json_serializer
    """
    return orjson.loads(obj)


def encode_datetime_object(dt: datetime.datetime) -> str:
    """Handles datetime serialization for nested timestamps in models/dataclasses"""
    return dt.replace(tzinfo=datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def convert_field_to_camel_case(string: str) -> str:
    """
    Cameilize field name

    most frontend ui frameworks use camel case
    this camelizes fields
    """
    return "".join(
        word if index == 0 else word.capitalize()
        for index, word in enumerate(string.split("_"))
    )
