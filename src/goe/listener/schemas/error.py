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

"""Response models."""
# Standard Library
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Union

# Third Party Libraries
from pydantic import root_validator

# GOE
from goe.listener.schemas.base import BaseSchema


class ErrorDetail(BaseSchema):
    """Error model definition.

    Attributes:
        code(int): HTTP error status code.
        message(str): Detail on HTTP error.
        status(str): HTTP error reason-phrase as per in RFC7235.
        -- NOTE! Set  automatically based on HTTP error status code.

    Raises:
        pydantic.error_wrappers.ValidationError: If any of provided attribute
            doesn't pass type validation.

    """

    code: int
    message: str
    details: Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]

    @root_validator(pre=False, skip_on_failure=True)
    def _set_status(cls, values: dict) -> dict:
        """Set the status field value based on the code attribute value.

        Args:
            values(dict): Stores the attributes of the ErrorModel object.

        Returns:
            dict: The attributes of the ErrorModel object with the status field.

        """
        values["status"] = HTTPStatus(values["code"]).name
        return values

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
            schema["description"] = "Error model."
            # Add status to schema properties.
            schema["properties"].update(
                {"status": {"title": "Status", "type": "string"}},
            )
            schema["required"].append("status")


class ErrorMessage(BaseSchema):
    """Error response model definition.

    Attributes:
        error(ErrorModel): ErrorModel class object instance.

    Raises:
        pydantic.error_wrappers.ValidationError: If any of provided attribute
            doesn't pass type validation.

    """

    error: ErrorDetail

    def __init__(self, **kwargs):
        """Initialize ErrorMessage class object instance."""
        # Neat trick to still use kwargs on ErrorMessage model.
        super().__init__(error=ErrorDetail(**kwargs))

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
            schema["description"] = "Error response model."
