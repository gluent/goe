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

# import os
from datetime import timedelta
from typing import Any, Dict, List, Optional, Union

# Third Party Libraries
import redis
from redis import sentinel as redis_sentinel
from redis.exceptions import RedisError

# GOE
from goe.config import orchestration_defaults

# this is a synchronous implementation of the caching class in listener.
# This is useful when you are outside of an event loop and need to use the redis convenience functions


class RedisClient(object):
    """Redis client utility.

    Utility class for handling Redis database connection and operations.

    Attributes:
        redis_client (redis.Redis, optional): Redis client object instance.
        log (logging.Logger): Logging handler for this class.
        base_redis_init_kwargs (dict): Common kwargs for Redis configuration
        connection_kwargs (dict, optional): Extra kwargs for Redis object init.

    """

    redis_client: redis.Redis = None
    logger: logging.Logger = logging.getLogger(__name__)
    base_redis_init_kwargs: dict = {
        "encoding": "utf-8",
        "decode_responses": True,
        "socket_connect_timeout": 2,
        # "socket_timeout": 1,
        "socket_keepalive": 5,
        "health_check_interval": 5,
    }
    connection_kwargs: dict = {}

    @classmethod
    def connect(cls, redis_connection_kwargs: Dict[str, Any] = {}):
        """Create Redis client session object instance.

        Based on configuration create either Redis client or Redis Sentinel.

        Returns:
            aioredis.Redis: Redis object instance.

        """
        _ = cls.get_client(redis_connection_kwargs)

        return cls

    @classmethod
    def get_client(cls, redis_connection_kwargs: Dict[str, Any] = {}):
        """Create Redis client session object instance.

        Based on configuration create either Redis client or Redis Sentinel.

        Returns:
            aioredis.Redis: Redis object instance.

        """
        redis_username = orchestration_defaults.listener_redis_username_default()
        redis_password = orchestration_defaults.listener_redis_password_default()
        redis_host = orchestration_defaults.listener_redis_host_default()
        redis_port = orchestration_defaults.listener_redis_port_default()
        redis_db = orchestration_defaults.listener_redis_db_default()
        redis_use_ssl = orchestration_defaults.listener_redis_use_ssl_default()
        # redis_ssl_cert = os.environ.get("OFFLOAD_LISTENER_REDIS_CERT", None)
        redis_use_sentinel = (
            orchestration_defaults.listener_redis_use_sentinel_default()
        )

        if cls.redis_client is None:
            cls.logger.debug("Initializing Redis client.")
            if redis_username and redis_password:
                cls.connection_kwargs = {
                    "username": redis_username,
                    "password": redis_password,
                }
            if redis_password and not redis_username:
                cls.connection_kwargs = {
                    "password": redis_password,
                }

            if redis_use_sentinel:
                sentinel = redis_sentinel.Sentinel(
                    [(redis_host, redis_port)],
                    sentinel_kwargs={
                        **cls.connection_kwargs,
                        **redis_connection_kwargs,
                    },
                )
                cls.redis_client = sentinel.master_for("goe-console")
            else:
                proto = "rediss" if redis_use_ssl else "redis"
                cls.base_redis_init_kwargs.update(
                    {**cls.connection_kwargs, **redis_connection_kwargs}
                )
                cls.redis_client = redis.from_url(
                    f"{proto}://{redis_host:s}:{redis_port}/{redis_db}",
                    **cls.base_redis_init_kwargs,
                )

        return cls.redis_client

    @classmethod
    def close_client(cls):
        """Close Redis client."""
        if cls.redis_client:
            cls.logger.debug("Closing Redis client")
            cls.redis_client.close()

    @classmethod
    def ping(cls):
        """Execute Redis PING command.

        Ping the Redis server.

        Returns:
            response: Boolean, whether Redis client could ping Redis server.

        Raises:
            aioredis.RedisError: If Redis client failed while executing command.

        """
        # Note: Not sure if this shouldn't be deep copy instead?
        redis_client = cls.redis_client

        cls.logger.debug("Executing Redis PING command")
        try:
            return redis_client.ping()
        except RedisError:
            return False

    @classmethod
    def set(cls, key: str, value: str, ttl: Optional[Union[int, timedelta]]):
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
            aioredis.RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(
            f"Executing Redis SET command, key: {key}, value: {value} with TTL: {ttl}"
        )
        try:
            redis_client.set(key, value, ex=ttl)
        except RedisError as exc:
            # cls.logger.exception(
            #     "Redis SET command finished with exception",
            #     exc_info=(type(exc), exc, exc.__traceback__),
            # )
            raise exc

    @classmethod
    def scan(cls, match: str = None, count: int = None):
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
            return redis_client.scan(match=match, count=count)
        except RedisError as exc:
            # cls.logger.exception(
            #     "Redis SCAN command finished with exception",
            #     exc_info=(type(exc), exc, exc.__traceback__),
            # )
            raise exc

    @classmethod
    def keys(cls, pattern: str):
        """Execute Redis KEYS command.

        Get the value of keys. If the keys do not exist the special value None
        is returned. An error is returned if the value stored at key is not a
        string, because GET only handles string values.

        Args:
            key (pattern): Redis db keys to lookup.

        Returns:
            response: Value of key.

        Raises:
            aioredis.RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis KEY command, pattern: {pattern}")
        try:
            return redis_client.keys(pattern)
        except RedisError as exc:
            # cls.logger.exception(
            #     "Redis KEYS command finished with exception",
            #     exc_info=(type(exc), exc, exc.__traceback__),
            # )
            raise exc

    @classmethod
    def rpush(cls, key: str, value: str, ttl: Optional[Union[int, timedelta]] = None):
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
            aioredis.RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Execute Redis RPUSH command, key: {key}, value: {value}")
        try:
            if ttl:
                with redis_client.pipeline(transaction=True) as pipe:
                    pipe.rpush(key, value)
                    pipe.expire(key, ttl).execute()
            else:
                redis_client.rpush(key, value)
        except RedisError as exc:
            # cls.logger.debug(
            #     "Redis RPUSH command finished with exception",
            #     exc_info=(type(exc), exc, exc.__traceback__),
            # )
            raise exc

    @classmethod
    def exists(cls, key: str):
        """Execute Redis EXISTS command.

        Returns if key exists.

        Args:
            key (str): Redis db key.

        Returns:
            response: Boolean whether key exists in Redis db.

        Raises:
            aioredis.RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis EXISTS command, key: {key}, exists")
        try:
            return redis_client.exists(key)
        except RedisError as exc:
            # cls.logger.exception(
            #     "Redis EXISTS command finished with exception",
            #     exc_info=(type(exc), exc, exc.__traceback__),
            # )
            raise exc

    @classmethod
    def get(cls, key: str):
        """Execute Redis GET command.

        Get the value of key. If the key does not exist the special value None
        is returned. An error is returned if the value stored at key is not a
        string, because GET only handles string values.

        Args:
            key (str): Redis db key.

        Returns:
            response: Value of key.

        Raises:
            aioredis.RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis GET command, key: {key}")
        try:
            return redis_client.get(key)
        except RedisError as exc:
            # cls.logger.exception(
            #     "Redis GET command finished with exception",
            #     exc_info=(type(exc), exc, exc.__traceback__),
            # )
            raise exc

    @classmethod
    def mget(cls, keys: List[str]):
        """Execute Redis MGET command.

        Get the value of keys. If the keys do not exist the special value None
        is returned. An error is returned if the value stored at key is not a
        string, because GET only handles string values.

        Args:
            key (list[str]): Redis db keys to lookup.

        Returns:
            response: Value of key.

        Raises:
            aioredis.RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Executing Redis MGET command, keys: {keys}")
        try:
            return redis_client.mget(keys)
        except RedisError as exc:
            # cls.logger.exception(
            #     "Redis MGET command finished with exception",
            #     exc_info=(type(exc), exc, exc.__traceback__),
            # )
            raise exc

    @classmethod
    def lrange(cls, key: str, start: int, end: int):
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
            aioredis.RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(
            f"Executing Redis LRANGE command, key: {key}, start: {start}, end: {end}",
        )
        try:
            return redis_client.lrange(key, start, end)
        except RedisError as exc:
            # cls.logger.exception(
            #     "Redis LRANGE command finished with exception",
            #     exc_info=(type(exc), exc, exc.__traceback__),
            # )
            raise exc

    @classmethod
    def expire(cls, key: str, ttl: Union[int, timedelta]):
        """Execute Redis EXPIRE command.

        Sets the TTL for a key.

        Args:
            key (str): Redis db key.
            ttl (int|timedelta): Time till expiration for a key

        Returns:
            response: TTL for the key.

        Raises:
            aioredis.RedisError: If Redis client failed while executing command.

        """
        redis_client = cls.redis_client

        cls.logger.debug(f"Execute Redis EXPIRE command, key: {key}, value: {ttl}")
        try:
            redis_client.expire(key, ttl)
        except RedisError as exc:
            # cls.logger.exception(
            #     "Redis EXPIRE command finished with exception",
            #     exc_info=(type(exc), exc, exc.__traceback__),
            # )
            raise exc


cache = RedisClient
