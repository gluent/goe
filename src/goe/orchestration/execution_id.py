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

"""
ExecutionId: Simple class allowing an Orchestration execution id to be generated and converted from/to str/bytes.
Unit tests in TestOrchestrationRepoClient.
"""

# Standard Library
import uuid
from dataclasses import dataclass

# Third Party Libraries
from pydantic import UUID4

###########################################################################
# ExecutionId
###########################################################################


@dataclass
class ExecutionId:
    """
    Simple class allowing an Orchestration execution id to be generated and converted from/to str/bytes.

    Create a new id:
        i = ExecutionId()
    Create a new id from a str variable (perhaps it came from JSON):
        i = ExecutionId.from_str(s)
    Create a new id from a bytes variable (perhaps it came from a database):
        i = ExecutionId.from_bytes(s)
    """

    id: uuid.UUID = None

    def __init__(self, **kwargs):
        if "from_str" in kwargs:
            self.id = self._str_as_uuid(kwargs["from_str"])
        elif "from_bytes" in kwargs:
            self.id = self._bytes_as_uuid(kwargs["from_bytes"])
        elif "from_uuid" in kwargs:
            self.id = kwargs["from_uuid"]
        else:
            self.id = self._new_id()

    def __str__(self):
        return self.as_str()

    def __bytes__(self):
        return self.as_bytes()

    @staticmethod
    def _bytes_as_uuid(b):
        if b is None:
            return None
        else:
            assert isinstance(b, bytes)
            assert len(b) == 16
            return uuid.UUID(bytes=b)

    @staticmethod
    def _str_as_uuid(s):
        if s is None:
            return None
        else:
            assert isinstance(s, str)
            assert len(s) == 36, "len(str) ({}) != 36".format(len(s))
            return uuid.UUID(s)

    @staticmethod
    def _new_id():
        return uuid.uuid4()

    @staticmethod
    def from_str(s: str):
        return ExecutionId(from_str=s)

    @staticmethod
    def from_bytes(b: bytes):
        return ExecutionId(from_bytes=b)

    @staticmethod
    def from_uuid(b: UUID4):
        return ExecutionId(from_uuid=b)

    def as_str(self):
        return str(self.id)

    def as_bytes(self):
        return self.id.bytes
