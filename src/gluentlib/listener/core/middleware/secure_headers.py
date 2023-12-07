# Standard Library

# Third Party Libraries
from fastapi.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request


class SecurityHeaderMiddleware(BaseHTTPMiddleware):
    """Class to manage exceptions in FastAPI"""

    async def dispatch(
        self,
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        """Exception middleware"""
        response = await call_next(request)
        response.headers["Strict-Transport-Security"] = "max-age=31536000 ; includeSubDomains"

        return response
