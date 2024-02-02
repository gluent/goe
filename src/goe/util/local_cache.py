#! /usr/bin/env python3

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

""" Data Cache abstractions
"""

import datetime
import json
import logging
import os.path
import tempfile
import time

from abc import ABCMeta, abstractmethod
from functools import wraps, partial


###############################################################################
# EXCEPTIONS
###############################################################################
class LocalCacheException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################
DEFAULT_EXPIRATION = 24 * 60 * 60  # Default cache expiration: 24 hours

###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default

###########################################################################
# Abstract Class: LocalCache
###########################################################################


class LocalCache(object, metaclass=ABCMeta):
    """ABSTRACT CLASS: LocalCache: (Local file) data cache"""

    def __init__(self, cache_id, cache_dir, expiration=DEFAULT_EXPIRATION):
        """CONSTRUCTOR"""
        assert cache_id and cache_dir
        if not os.path.isdir(cache_dir):
            raise LocalCacheException("Bad local cache directory: %s" % cache_dir)

        # Local cache
        self._id = cache_id  # ... id
        self._dir = cache_dir  # ... directory
        self._expiration = expiration  # ... expiration, in seconds

        # Cache file name
        self._cache_file = self.cache_file_name(
            self._sanitize_cache_name(self._id), self._dir
        )
        self._cache = None  # Cache data

        # Load cache if cache file exists
        if self._exists():
            self.load()
        else:
            logger.debug("Cache file does NOT exist. Nothing to load")

        logger.debug(
            "LocalCache() object: %s successfully initialized with file: %s"
            % (self._id, self._cache_file)
        )

    ###########################################################################
    # ABSTRACT ROUTINES
    ###########################################################################

    @abstractmethod
    def cache_file_name(self, cache_id, cache_dir):
        """Construct cache file name"""
        pass

    @abstractmethod
    def load(self):
        """Load contents of self._cache_file into self._cache"""
        pass

    @abstractmethod
    def save(self):
        """Save self._cache to self._cache_file"""
        pass

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _sanitize_cache_name(self, original_name):
        """Make cache file name 'pretty'"""
        better_name = (
            original_name.replace("/", ".").replace("..", ".").replace("_.", "_")
        )
        if better_name.startswith("."):
            better_name = better_name[1:]
        if better_name.endswith("."):
            better_name = better_name[:-1]

        logger.debug("Transcoded a better cache file name: %s" % better_name)
        return better_name

    def _exists(self):
        """Return True if self._cache_file exists, False otherwise"""

        if not os.path.exists(self._cache_file):
            logger.debug("Local cache file: %s does NOT exist" % self._cache_file)
            return False
        elif not os.path.isfile(self._cache_file):
            raise LocalCacheException(
                "Serious cache error: %s is not a file" % self._cache_file
            )
        else:
            logger.debug("Local cache file: %s exists" % self._cache_file)
            return True

    def _expired(self):
        """Return True if cache contents has expired, False otherwise"""
        cache_expiration = int(os.path.getmtime(self._cache_file))
        cache_expiration_str = datetime.datetime.fromtimestamp(
            cache_expiration + self._expiration
        ).strftime("%Y-%m-%d %H:%M:%S")

        if time.time() - cache_expiration >= self._expiration:
            logger.debug(
                "Cache file: %s already expired: %s"
                % (self._cache_file, cache_expiration_str)
            )
            return True
        else:
            logger.debug(
                "Cache file: %s is not yet expired. Current expiration: %s"
                % (self._cache_file, cache_expiration_str)
            )
            return False

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def cache(self):
        return self.get_cache()

    @cache.setter
    def cache(self, val):
        self.set_cache(val)

    @property
    def expired(self):
        """Return True if cache needs a refresh"""
        return not self._exists() or self._expired()

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def get_cache(self):
        """Get cache data"""
        return self._cache

    def set_cache(self, val):
        """Replace cache data"""
        logger.debug("Replacing local cache. New length: %d" % len(val))
        self._cache = val
        self.save()


###########################################################################
# Class: JsonCache
###########################################################################


class JsonCache(LocalCache):
    """Local cache implemented as JSON file"""

    def __init__(self, cache_id, cache_dir, expiration=DEFAULT_EXPIRATION):
        """CONSTRUCTOR"""

        super(JsonCache, self).__init__(cache_id, cache_dir, expiration)
        logger.debug("JsonCache() object: %s successfully initialized" % self._id)

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _serialize(self, obj):
        """Properly serialize Python objects to JSON"""

        # 'datetime'
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()

        return obj

    ###########################################################################
    # IMPLEMENTATION OF PARENT ABSTRACT ROUTINES
    ###########################################################################

    def cache_file_name(self, cache_id, cache_dir):
        """Construct cache file name"""
        return os.path.join(cache_dir, "%s.%s" % (cache_id, "json"))

    def load(self):
        """Load contents of self._cache_file into self._cache"""

        logger.debug("Loading cache from JSON file: %s" % self._cache_file)
        with open(self._cache_file) as f:
            self._cache = json.load(f)

    def save(self):
        """Save self._cache to self._cache_file"""

        if self._cache is not None:
            logger.debug("Saving cache to JSON file: %s" % self._cache_file)
            with open(self._cache_file, "w") as f:
                json.dump(self._cache, f, default=self._serialize)
        else:
            logger.debug("Cache is empty. No need to save")


###########################################################################
# DECORATOR: json_cached
###########################################################################


def attach_wrapper(obj, func=None):
    """Let user adjust decorator attributes

    i.e. <decorated callable>.set_force()
    """
    if func is None:
        return partial(attach_wrapper, obj)

    setattr(obj, func.__name__, func)
    return func


def json_cached(
    cache_id, cache_dir=tempfile.gettempdir(), expiration=DEFAULT_EXPIRATION
):
    """Wrap function in the following logic:

    If cache exists and not expired:
        Return cache data
    Otherwise:
        Run the function, save result in cache and return it
    """
    json_args = locals()

    def decorate(func):
        for attr in ("cache_id", "cache_dir", "expiration"):
            if not hasattr(decorate, attr):
                setattr(decorate, attr, json_args[attr])
        decorate.force = False

        @wraps(func)
        def wrapper(*args, **kwargs):
            cache_obj = JsonCache(
                decorate.cache_id, decorate.cache_dir, decorate.expiration
            )

            if decorate.force or cache_obj.expired:
                cache_obj.cache = func(*args, **kwargs)

            return cache_obj.get_cache()

        @attach_wrapper(wrapper)
        def set_force(val=True):
            logger.debug("Setting json_cache decorator FORCE attribute to: %s" % val)
            decorate.force = val

        @attach_wrapper(wrapper)
        def set_id(cache_id):
            logger.debug("Setting json_cache decorator ID attribute to: %s" % cache_id)
            decorate.cache_id = cache_id

        @attach_wrapper(wrapper)
        def set_dir(cache_dir):
            logger.debug(
                "Setting json_cache decorator DIR attribute to: %s" % cache_dir
            )
            decorate.cache_dir = cache_dir

        @attach_wrapper(wrapper)
        def set_expiration(expiration):
            logger.debug(
                "Setting json_cache decorator EXPIRATION attribute to: %s" % expiration
            )
            decorate.expiration = expiration

        return wrapper

    return decorate
