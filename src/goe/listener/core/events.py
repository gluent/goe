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
import logging

# GOE
from goe.listener import utils
from goe.listener.config import settings

logger = logging.getLogger()


async def on_startup():
    """GOE listener startup event handler.

    Performs startup activities for GOE Listener

    """
    if settings.cache_enabled:
        utils.cache.get_client()
    logger.debug("Listener API HTTP worker process started successfully")


async def on_shutdown():
    """GOE listener shutdown event handler.

    Performs shutdown activities for GOE Listener

    """
    if settings.cache_enabled:
        await utils.cache.close_client()
    logger.debug("Listener API HTTP worker process shutdown complete")
