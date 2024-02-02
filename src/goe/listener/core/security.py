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
from fastapi import Security
from fastapi.security.api_key import APIKeyHeader

# GOE
from goe.listener.config import settings
from goe.listener.exceptions.errors import CredentialValidationError

# Note: By default, nginx silently drops headers with underscores. Use hyphens instead.
API_KEY_NAME = "x-goe-console-key"


api_token_auth = APIKeyHeader(
    name=API_KEY_NAME, scheme_name="GOE Listener Token", auto_error=False
)


async def valid_api_token(
    shared_token_header: str = Security(api_token_auth),
) -> bool:
    """Validates an API Token

    Args:
        header_param: parsed header field secret_header
    Returns:
        True if the authentication was successful
    Raises:
        CredentialValidationError if the authentication failed
    """
    if shared_token_header == settings.shared_token.get_secret_value():
        return True
    raise CredentialValidationError
