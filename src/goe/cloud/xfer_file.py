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

""" xfer_file: Abstract 'file transfer' object

    Concrete implementations (a.k.a "children") can be used as "transfer destinations"
    with xfer_xfer module.
"""

import logging

from abc import ABCMeta, abstractmethod, abstractproperty


###############################################################################
# EXCEPTIONS
###############################################################################
class XferFileException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# Destination file states
DST_MATCH = "MATCH"  # Destination file exists and is exactly the same as source
DST_INVALID = (
    "INVALID"  # Destination file exists, but is invalid (needs to be re-copied)
)
DST_PARTIAL = "PARTIAL"  # Destination file exists and copy can resume
DST_NONE = "NONE"  # Destination file does NOT exist


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class XferFile(object, metaclass=ABCMeta):
    def __init__(self, client, section, cfg):
        assert client and section

        self._client = client
        self._section = section
        self._cfg = cfg

        self._stats = None

    # -- Properties

    @property
    def exists(self):
        return True if self.stats else False

    @property
    def stats(self):
        if self._stats is None:
            self._stats = self._get_stats()
        return self._stats

    @property
    def client(self):
        return self._client

    @property
    def section(self):
        return self._section

    # -- Abstract Properties

    @abstractproperty
    def filetype(self):
        pass

    # -- Methods

    def refresh(self):
        self._stats = None

    def equals(self, other):
        """Return True if 'other' XferFile is "the same" as current (based on stats)
        False otherwise

        Right now, "the same" == "the same": length + HDFS checksum
        """
        for stat in ("length", "checksum"):
            self_val = self.stats.get(stat, None)
            other_val = other.stats.get(stat, None)
            if self_val != other_val:
                logger.debug(
                    "Stat: %s mismatch between: %s [%s] and: %s [%s]"
                    % (stat, self, str(self_val), other, str(other_val))
                )
                return False

        logger.debug("Xfer objects equal: %s == %s" % (self, other))
        return True

    # -- Abstract Methods

    @abstractmethod
    def _get_stats(self):
        """Should return: {stat: val} dictionary, with, at least: 'length', 'checksum' stats defined"""
        pass

    @abstractmethod
    def __str__(self):
        """Should be: [section] filename"""
        pass

    @abstractmethod
    def copy(self, other, force=False):
        pass
