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

"""
Simple utility to request status of GOE Listener.
"""

import requests
from typing import TYPE_CHECKING

from goe.listener.exceptions import ApplicationError
from goe.listener.api.routes.system import STATUS_OK
from goe.orchestration.orchestration_constants import PRODUCT_NAME_GEL

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig


def ping(orchestration_config: "OrchestrationConfig") -> bool:
    """
    Submit a status call to GOE Listener.
    """
    url = f"http://{orchestration_config.listener_host}:{orchestration_config.listener_port}/api/system/status"
    headers = {"Content-Type": "application/json"}
    if orchestration_config.listener_shared_token:
        headers.update(
            {"x-goe-console-key": orchestration_config.listener_shared_token}
        )
    r = requests.get(url, headers=headers)
    if r.ok:
        if r.json().get("status") == STATUS_OK:
            return True
        else:
            raise ApplicationError(message=f"{PRODUCT_NAME_GEL} not OK: {r.text}")
    else:
        raise ApplicationError(status_code=r.status_code, message=r.text)
