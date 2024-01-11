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
