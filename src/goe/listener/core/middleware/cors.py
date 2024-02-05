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
