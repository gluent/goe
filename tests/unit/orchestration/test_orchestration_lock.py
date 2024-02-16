# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" TestOrchestrationLock: Unit test library to test OrchestrationLock functionality
"""
import os
import threading
import time
from unittest import mock

from goe.offload.offload_messages import OffloadMessages
from goe.orchestration.orchestration_lock import (
    OrchestrationLockTimeout,
    orchestration_lock_for_table,
    orchestration_lock_from_hybrid_metadata,
)
from goe.persistence.orchestration_metadata import OrchestrationMetadata

from tests.unit.test_functions import FAKE_ORACLE_BQ_ENV


LOCK_OWNER = "SH_TEST"
LOCK_TABLE1 = "UNIT_TABLE1"
LOCK_TABLE2 = "UNIT_TABLE2"
LOCK_TABLE3 = "UNIT_TABLE3"


@mock.patch.dict(os.environ, FAKE_ORACLE_BQ_ENV)
def test_orchestration_lock_for_table():
    # Ensure works as context manager
    lock = orchestration_lock_for_table(LOCK_OWNER, LOCK_TABLE1)
    with lock:
        pass
    with orchestration_lock_for_table(LOCK_OWNER, LOCK_TABLE1):
        pass
    # Ensure works with explicit acquisition.
    # Should pass because above code will release lock automatically.
    lock.acquire()
    lock.release()


@mock.patch.dict(os.environ, FAKE_ORACLE_BQ_ENV)
def test_orchestration_lock_from_hybrid_metadata():
    messages = OffloadMessages()
    metadata = OrchestrationMetadata.from_attributes(
        messages=messages, offloaded_owner=LOCK_OWNER, offloaded_table=LOCK_TABLE2
    )
    # Ensure works as context manager
    lock = orchestration_lock_from_hybrid_metadata(metadata)
    with lock:
        pass
    # Ensure works with explicit acquisition.
    # Should pass because above code will release lock automatically.
    lock.acquire()
    lock.release()


@mock.patch.dict(os.environ, FAKE_ORACLE_BQ_ENV)
def test_orchestration_blocking_lock():
    """Test that locks are blocking using two threads
    Code to check for exceptions in threads taken from:
        https://stackoverflow.com/questions/12484175/make-python-unittest-fail-on-exception-from-any-thread
    """
    exceptions_caught_in_threads = {}

    def custom_excepthook(args):
        thread_name = args.thread.name
        exceptions_caught_in_threads[thread_name] = {
            "thread": args.thread,
            "exception": {
                "type": args.exc_type,
                "value": args.exc_value,
                "traceback": args.exc_traceback,
            },
        }

    # Registering custom excepthook to catch the exception in the threads
    threading.excepthook = custom_excepthook

    def block_fn(t):
        if t == 2:
            # Ensure thread 1 has the lock
            time.sleep(0.1)
        lock = orchestration_lock_for_table(LOCK_OWNER, LOCK_TABLE3)
        lock.acquire()
        time.sleep(1)
        lock.release()

    t1 = threading.Thread(name="blocker", target=block_fn, args=(1,))
    t2 = threading.Thread(name="waiter", target=block_fn, args=(2,))
    t1.start()
    time.sleep(0.1)
    t2.start()
    t2.join()
    assert "blocker" not in exceptions_caught_in_threads
    assert "waiter" in exceptions_caught_in_threads
    assert (
        exceptions_caught_in_threads["waiter"]["exception"]["type"]
        == OrchestrationLockTimeout
    )
    t1.join()
