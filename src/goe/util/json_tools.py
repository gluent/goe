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
