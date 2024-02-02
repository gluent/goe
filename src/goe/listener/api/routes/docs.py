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
from fastapi import APIRouter, Request
from fastapi.openapi.docs import (
    get_redoc_html,
    get_swagger_ui_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.responses import HTMLResponse

router = APIRouter()


@router.get("/docs/", include_in_schema=False)
async def custom_swagger_ui_html(request: Request) -> HTMLResponse:
    """
    Swagger UI.

    :param request: current request.
    :return: rendered swagger UI.
    """
    title = request.app.title
    return get_swagger_ui_html(
        openapi_url=request.app.openapi_url,
        title=f"{title} - Swagger UI",
        oauth2_redirect_url=request.url_for("swagger_ui_redirect"),
        swagger_js_url="/public/docs/swagger-ui-bundle.js",
        swagger_css_url="/public/docs/swagger-ui.css",
    )


@router.get("/swagger-redirect/", include_in_schema=False)
async def swagger_ui_redirect() -> HTMLResponse:
    """
    Redirect to swagger.

    :return: redirect.
    """
    return get_swagger_ui_oauth2_redirect_html()


@router.get("/redoc/", include_in_schema=False)
async def redoc_html(request: Request) -> HTMLResponse:
    """
    Redoc UI.

    :param request: current request.
    :return: rendered redoc UI.
    """
    title = request.app.title
    return get_redoc_html(
        openapi_url=request.app.openapi_url,
        title=f"{title} - ReDoc",
        redoc_js_url="/public/docs/redoc.standalone.js",
    )
