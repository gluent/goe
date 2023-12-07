# Standard Library
import re

# Third Party Libraries
from fastapi.middleware.cors import CORSMiddleware as BaseCORSMiddleware
from starlette.types import Receive, Scope, Send


class CORSMiddleware(BaseCORSMiddleware):
    def __init__(self, path_regex: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.path_regex = re.compile(path_regex)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":  # pragma: no cover
            await self.app(scope, receive, send)
            return  # noqa: WPS324

        path = scope["path"]
        if not self.path_regex.match(path):
            await self.app(scope, receive, send)
            return  # noqa: WPS324

        return await super().__call__(scope, receive, send)
