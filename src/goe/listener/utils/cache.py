# -*- coding: utf-8 -*-

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

"""Redis client class utility."""
# Standard Library
import logging
from datetime import timedelta
from typing import List, Optional, Union

# Third Party Libraries
from redis import asyncio as aioredis
from redis.asyncio import sentinel as aioredis_sentinel
from redis.exceptions import RedisError

# GOE
from goe.listener.config import settings


class RedisClient(object):
    """Redis client utility.

    Utility class for handling Redis database connection and operations.

    Attributes:
        redis_client (aioredis.Redis, optional): Redis client object instance.
        log (logging.Logger): Logging handler for this class.
        base_redis_init_kwargs (dict): Common kwargs for Redis configuration
        connection_kwargs (dict, optional): Extra kwargs for Redis object init.

    """

    _instance: Optional["RedisClient"] = None
    redis_client: Optional[aioredis.Redis] = None
    logger: logging.Logger = logging.getLogger(__name__)
    base_redis_init_kwargs: dict = {
        "encoding": "utf-8",
        "port": settings.redis_port,
        "decode_responses": True,
        "socket_connect_timeout": 2,
        # "socket_timeout": 1,
        "socket_keepalive": 5,
        "health_check_interval": 5,
    }
    connection_kwargs: dict = {}

    def __new__(cls):
        """Singleton loader"""
        if cls._instance is not None:
            return cls._instance

        cls._instance = super().__new__(cls)
        cls.get_client()
        return cls._instance

    @classmethod
    def get_client(cls):
        """Create Redis client session object instance.

        Based on configuration create either Redis client or Redis Sentinel.

        Returns:
            Redis: Redis object instance.

        """
        if cls.redis_client is None:
            cls.logger.debug("Initializing Redis connection pool.")
            if settings.redis_username and settings.redis_password:
                cls.connection_kwargs = {
                    "username": settings.redis_username,
                    "password": settings.redis_password,
                }
            if not settings.redis_username and settings.redis_password:
                cls.connection_kwargs = {
                    "password": settings.redis_password,
                }

            if settings.redis_use_sentinel:
                sentinel = aioredis_sentinel.Sentinel(
                    [(settings.redis_host, settings.redis_port)],
                    sentinel_kwargs=cls.connection_kwargs,
                )
                cls.redis_client = sentinel.master_for(settings.redis_sentinel_master)
            else:
                cls.base_redis_init_kwargs.update(cls.connection_kwargs)
                cls.redis_client = aioredis.from_url(
                    settings.redis_url,
                    **cls.base_redis_init_kwargs,
                )

        return cls.redis_client

    @classmethod
    async def close_client(cls):
        """Close Redis client."""
        if cls.redis_client:
            cls.logger.debug("Closing Redis connection pool")
            try:
                await cls.redis_client.close()
            except RedisError as exc:
                cls.logger.error(f"Redis error closing  - {exc.__class__.__qualname__}")

    @classmethod
    async def ping(cls):
        """Execute Redis PING command.

        Ping the Redis server.

        Returns:
            response: Boolean, whether Redis client could ping Redis server.

        Raises:
            RedisError: If Redis client failed while executing command.

        """
        # Note: Not sure if this shouldn't be deep copy instead?
        redis_client = cls.redis_client

        cls.logger.debug("Executing Redis PING command")
        try:
            return await redis_client.ping()
        except RedisError as exc:
            cls.logger.error(
                f"Redis PING command finished with exception  - {exc.__class__.__qualname__}"
            )
            return False

    @classmethod
    async def set(
        cls, key: str, value: str, ttl: Optional[Union[int, timedelta]] = None
    ):
        """Execute Redis SET command.

        Set key to hold the string value. If key already holds a value, it is
        overwritten, regardless of its type.

        Args:
            key (str): Redis db key.
            value (str): Value to be set.
            ttl (int): Time to live in seconds
        Returns:
            response: Redis SET command response, for more info
                look: https://redis.io/commands/set#return-value

        Raises:
            RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis SET command, key: {key} with TTL: {ttl}")
        try:
            await redis_client.set(key, value, ex=ttl)
        except RedisError as exc:
            cls.logger.error(
                f"Redis SET command finished with exception - {exc.__class__.__qualname__}"
            )
            raise exc

    @classmethod
    async def scan(cls, match: str = None, count: int = None):
        """Execute Redis SCAN command with pattern matching.

        Scan the keyspace for keys.

        Args:
            match (str, optional): Pattern to match.
            count (int, optional): Number of keys to return.

        Returns:
            response: Redis SCAN command response, for more info
                look: https://redis.io/commands/scan
        """
        redis_client = cls.redis_client

        cls.logger.debug(
            f"Executing Redis SCAN command, match: {match}, count: {count}"
        )
        try:
            return await redis_client.scan(match=match, count=count)
        except RedisError as exc:
            cls.logger.error(
                f"Redis SCAN command finished with exception  - {exc.__class__.__qualname__}"
            )
            raise exc

    @classmethod
    async def keys(cls, pattern: str):
        """Execute Redis KEYS command.

        Get the value of keys. If the keys do not exist the special value None
        is returned. An error is returned if the value stored at key is not a
        string, because GET only handles string values.

        Args:
            key (pattern): Redis db keys to lookup.

        Returns:
            response: Value of key.

        Raises:
            RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis KEY command, pattern: {pattern}")
        try:
            return await redis_client.keys(pattern)
        except RedisError as exc:
            cls.logger.error(
                f"Redis KEYS command finished with exception  - {exc.__class__.__qualname__}"
            )
            raise exc

    @classmethod
    async def rpush(
        cls, key: str, value: str, ttl: Optional[Union[int, timedelta]] = None
    ):
        """Execute Redis RPUSH command.

        Insert all the specified values at the tail of the list stored at key.
        If key does not exist, it is created as empty list before performing
        the push operation. When key holds a value that is not a list, an
        error is returned.

        Args:
            key (str): Redis db key.
            value (str, list): Single or multiple values to append.
            ttl (int): TTL for message


        Returns:
            response: Length of the list after the push operation.

        Raises:
            RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Execute Redis RPUSH command, key: {key}")
        try:
            if ttl:
                async with redis_client.pipeline(transaction=True) as pipe:
                    await pipe.rpush(key, value)
                    await pipe.expire(key, ttl).execute()
            else:
                await redis_client.rpush(key, value)
        except RedisError as exc:
            cls.logger.error(
                f"Redis RPUSH command finished with exception  - {exc.__class__.__qualname__}"
            )
            raise exc

    @classmethod
    async def expire(cls, key: str, ttl: Union[int, timedelta]):
        """Execute Redis EXPIRE command.

        Sets the TTL for a key.

        Args:
            key (str): Redis db key.
            ttl (int|timedelta): Time till expiration for a key

        Returns:
            response: TTL for the key.

        Raises:
            RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Execute Redis EXPIRE command, key: {key}, value: {ttl}")
        try:
            await redis_client.expire(key, ttl)
        except RedisError as exc:
            cls.logger.error(
                f"Redis EXPIRE command finished with exception  - {exc.__class__.__qualname__}"
            )
            raise exc

    @classmethod
    async def exists(cls, key: str):
        """Execute Redis EXISTS command.

        Returns if key exists.

        Args:
            key (str): Redis db key.

        Returns:
            response: Boolean whether key exists in Redis db.

        Raises:
            RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis EXISTS command, key: {key}, exists")
        try:
            return await redis_client.exists(key)
        except RedisError as exc:
            cls.logger.error(
                f"Redis EXISTS command finished with exception  - {exc.__class__.__qualname__}"
            )
            raise exc

    @classmethod
    async def get(cls, key: str):
        """Execute Redis GET command.

        Get the value of key. If the key does not exist the special value None
        is returned. An error is returned if the value stored at key is not a
        string, because GET only handles string values.

        Args:
            key (str): Redis db key.

        Returns:
            response: Value of key.

        Raises:
            RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis GET command, key: {key}")
        try:
            return await redis_client.get(key)
        except RedisError as exc:
            cls.logger.error(
                f"Redis GET command finished with exception  - {exc.__class__.__qualname__}"
            )
            raise exc

    @classmethod
    async def mget(cls, keys: List[str]):
        """Execute Redis MGET command.

        Get the value of keys. If the keys do not exist the special value None
        is returned. An error is returned if the value stored at key is not a
        string, because GET only handles string values.

        Args:
            key (list[str]): Redis db keys to lookup.

        Returns:
            response: Value of key.

        Raises:
            RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis MGET command, keys: {keys}")
        try:
            return await redis_client.mget(keys)
        except RedisError as exc:
            cls.logger.error(
                f"Redis MGET command finished with exception  - {exc.__class__.__qualname__}"
            )
            raise exc

    @classmethod
    async def lrange(cls, key: str, start: int, end: int):
        """Execute Redis LRANGE command.

        Returns the specified elements of the list stored at key. The offsets
        start and stop are zero-based indexes, with 0 being the first element
        of the list (the head of the list), 1 being the next element and so on.
        These offsets can also be negative numbers indicating offsets starting
        at the end of the list. For example, -1 is the last element of the
        list, -2 the penultimate, and so on.

        Args:
            key (str): Redis db key.
            start (int): Start offset value.
            end (int): End offset value.

        Returns:
            response: Returns the specified elements of the list stored at key.

        Raises:
            RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(
            f"Executing Redis LRANGE command, key: {key}, start: {start}, end: {end}",
        )
        try:
            return await redis_client.lrange(key, start, end)
        except RedisError as exc:
            cls.logger.error(
                f"Redis LRANGE command finished with exception  - {exc.__class__.__qualname__}"
            )
            raise exc

    @classmethod
    async def delete_keys(cls, pattern: str) -> int:
        """Delete keys matching a pattern.

        Args:
            pattern (str): Pattern to match.

        Returns:
            response: Number of keys deleted.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis KEYS command, pattern: {pattern}")
        try:
            keys = await redis_client.keys(pattern)
        except RedisError as exc:
            cls.logger.error(
                f"Redis KEYS (DELETE_KEYS) command finished with exception  - {exc.__class__.__qualname__}",
            )
            raise exc
        if keys:
            cls.logger.debug(f"Found {len(keys)} keys matching pattern: {pattern}")
            return await redis_client.delete(*keys)
        return 0


cache = RedisClient()
