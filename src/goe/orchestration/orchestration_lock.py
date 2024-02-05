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

""" OrchestrationLock: Library providing simple locking mechanism for orchestration commands.
    In general the lock is expected to be the source owner/table name of a command.
"""

# Standard Library
import logging
import os
from abc import ABCMeta, abstractmethod

# Third Party Libraries
from filelock import FileLock, Timeout

# GOE
from goe.persistence.orchestration_metadata import OrchestrationMetadata

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


class OrchestrationLockTimeout(Exception):
    pass


###########################################################################
# CONSTANTS
###########################################################################

LOCK_FILE_PREFIX = "orchestration_"
LOCK_FILE_SUFFIX = ".lock"


###########################################################################
# GLOBAL FUNCTIONS
###########################################################################


def orchestration_lock_for_table(owner, table_name, dry_run=False):
    """Return lock handler using owner/table_name as lock ids.
    For now we only support FileLock implementation but others could be slotted in in the future.
    """
    return FileLockOrchestrationLock([owner, table_name], dry_run=dry_run)


def orchestration_lock_from_hybrid_metadata(hybrid_metadata, dry_run=False):
    """Return lock handler using offloaded owner/table metadata as lock ids.
    For now we only support FileLock implementation but others could be slotted in in the future.
    """
    assert hybrid_metadata
    assert isinstance(hybrid_metadata, OrchestrationMetadata)
    assert hybrid_metadata.offloaded_owner
    assert hybrid_metadata.offloaded_table
    return FileLockOrchestrationLock(
        [hybrid_metadata.offloaded_owner, hybrid_metadata.offloaded_table],
        dry_run=dry_run,
    )


###########################################################################
# OrchestrationLockInterface
###########################################################################


class OrchestrationLockInterface(metaclass=ABCMeta):
    """Library providing simple locking mechanism for orchestration commands.
    In general the lock is expected to be the source owner/table name of a command.
    Lock files are located in $OFFLOAD_HOME/run.
    """

    def __init__(self, lock_ids, dry_run=False):
        assert lock_ids
        assert isinstance(lock_ids, (list, str))
        if isinstance(lock_ids, str):
            self._lock_ids = [lock_ids]
        else:
            self._lock_ids = lock_ids
        self._dry_run = dry_run
        self._lock = None

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.release()

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _exception_message(self):
        return "Another Orchestration process has locked id: {}".format(
            ".".join(self._lock_ids)
        )

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @abstractmethod
    def acquire(self):
        pass

    @abstractmethod
    def release(self):
        pass


###########################################################################
# FileLockOrchestrationLock
###########################################################################


class FileLockOrchestrationLock(OrchestrationLockInterface):
    """FileLock implementation of OrchestrationLockInterface.
    Lock files are located in $OFFLOAD_HOME/run.
    """

    def __init__(self, lock_ids, dry_run=False):
        super(FileLockOrchestrationLock, self).__init__(lock_ids, dry_run=dry_run)
        self._file_name = self._lock_file_name()
        logger.info(f"Orchestration lock filename: {self._file_name}")
        self._lock = FileLock(self._file_name)

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _lock_file_name(self):
        file_name = LOCK_FILE_PREFIX + "_".join(self._lock_ids) + LOCK_FILE_SUFFIX
        return os.path.join(os.environ.get("OFFLOAD_HOME"), "run", file_name)

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def acquire(self):
        try:
            if not self._dry_run:
                # FileLock.acquire() will create a lock file with default umask, this may prevent other users from
                # running commands in the future if they cannot write to the lock file.
                # We may choose to relax permissions on lock files here at some point either with umask() or by
                # running a chmod() after acquiring the lock.
                self._lock.acquire(timeout=0)
        except Timeout as exc:
            logger.info("Orchestration lock acquire timeout: {}".format(str(exc)))
            raise OrchestrationLockTimeout(self._exception_message()) from exc

    def release(self):
        if not self._dry_run:
            self._lock.release()
