"""
Simple utility to request status of Gluent Enterprise Listener.
LICENSE_TEXT
"""

import requests
from typing import TYPE_CHECKING

from gluentlib.listener.exceptions import ApplicationError
from gluentlib.listener.api.routes.system import STATUS_OK
from gluentlib.orchestration.orchestration_constants import PRODUCT_NAME_GEL

if TYPE_CHECKING:
    from gluentlib.config.orchestration_config import OrchestrationConfig


def ping(orchestration_config: "OrchestrationConfig") -> bool:
    """
    Submit a status call to Gluent Enterprise Listener.
    """
    url = f'http://{orchestration_config.listener_host}:{orchestration_config.listener_port}/api/system/status'
    headers = {"Content-Type": "application/json"}
    if orchestration_config.listener_shared_token:
        headers.update({"x-gluent-console-key": orchestration_config.listener_shared_token})
    r = requests.get(url, headers=headers)
    if r.ok:
        if r.json().get('status') == STATUS_OK:
            return True
        else:
            raise ApplicationError(message=f'{PRODUCT_NAME_GEL} not OK: {r.text}')
    else:
        raise ApplicationError(status_code=r.status_code, message=r.text)
