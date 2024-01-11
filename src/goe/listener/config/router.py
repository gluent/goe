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
