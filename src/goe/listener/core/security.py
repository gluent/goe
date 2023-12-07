# Third Party Libraries
from fastapi import Security
from fastapi.security.api_key import APIKeyHeader

# Gluent
from goe.listener.config import settings
from goe.listener.exceptions.errors import CredentialValidationError

# Note: By default, nginx silently drops headers with underscores. Use hyphens instead.
API_KEY_NAME = "x-gluent-console-key"


api_token_auth = APIKeyHeader(
    name=API_KEY_NAME, scheme_name="Gluent Listener Token", auto_error=False
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
