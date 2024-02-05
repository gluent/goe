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

"""Application routes configuration.

In this file all application endpoints are being defined.
"""

# Third Party Libraries
from fastapi import APIRouter, Security

# GOE
from goe.listener.api.routes import docs, orchestration, system
from goe.listener.core import security

api = APIRouter()
router = APIRouter()
api.include_router(
    system.router,
    prefix="/system",
    tags=["System"],
)
api.include_router(
    orchestration.router,
    prefix="/orchestration",
    tags=["Orchestration"],
)

router.include_router(docs.router)

router.include_router(
    api, prefix="/api", dependencies=[Security(security.valid_api_token)]
)
