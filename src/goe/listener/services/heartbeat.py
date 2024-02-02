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
import atexit
import logging
import signal
import threading
from multiprocessing.util import _exit_function  # noqa: WPS433 WPS450
from typing import Optional

# Third Party Libraries
import anyio
from anyio import create_task_group, open_signal_receiver
from anyio.abc import CancelScope
from pydantic import UUID3
from redis.exceptions import RedisError

# GOE
from goe.listener import utils
from goe.listener.config import settings
from goe.listener.services.system import system

logger = logging.getLoggerClass()

if threading.current_thread() is not threading.main_thread():
    atexit.unregister(_exit_function)


async def _signal_handler(scope: CancelScope, instance: "ListenerHeartbeat"):
    with open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for signum in signals:
            scope.cancel()
            instance.stop()
            return


class ListenerHeartbeat(object):
    """Class to manage agent Heartbeats while the agent is running

    Posts a message to a redis key with a lmited TTL.

    Attributes:
        redis_client (aioredis.Redis, optional): Redis client object instance.
        logger (logging.Logger): Logging handler for this class.

    """

    _instance: Optional["ListenerHeartbeat"] = None
    _running: bool = True
    logger: logging.Logger = logging.getLogger(__name__)
    local_ip: str = utils.system.get_ip_address()
    group_id: UUID3 = system.generate_listener_group_id()
    endpoint_id: UUID3 = system.generate_listener_endpoint_id()

    def __new__(cls):
        """Singleton loader"""
        if cls._instance is not None:
            return cls._instance

        cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    async def start(cls):
        """
        Create Vite client session object instance.

        Returns:
            Future

        """
        cls.logger.info("Starting Listener Heartbeat service")
        async with create_task_group() as tg:
            tg.start_soon(_signal_handler, tg.cancel_scope, cls)
            await cls._periodically_publish()

    @classmethod
    def stop(cls):
        """Stop heartbeat service."""
        cls.logger.info("Stopping Listener Heartbeat Service")

        if cls._running:
            cls._running = False

    @classmethod
    async def _periodically_publish(cls):
        """[summary]"""
        while cls._running:
            try:
                await utils.cache.set(
                    f"goe:listener:endpoints:{cls.group_id}:{cls.endpoint_id}",
                    f"{'https' if settings.ssl_enabled else 'http'}://{cls.local_ip}:{settings.port}",
                    settings.heartbeat_interval * 2,
                )
                cls.logger.debug("Published Heartbeat")
            except RedisError:
                cls.logger.warning(
                    "Failed to publish heartbeat to caching backend.  Will try again."
                )
            await anyio.sleep(settings.heartbeat_interval)


heartbeat = ListenerHeartbeat()
