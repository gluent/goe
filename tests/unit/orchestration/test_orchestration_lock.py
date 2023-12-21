""" TestOrchestrationLock: Unit test library to test OrchestrationLock functionality
"""
import threading
import time
from unittest import TestCase, main

from goe.offload.offload_messages import OffloadMessages
from goe.orchestration.orchestration_lock import (
    OrchestrationLockTimeout,
    orchestration_lock_for_table,
    orchestration_lock_from_hybrid_metadata,
)
from goe.persistence.orchestration_metadata import OrchestrationMetadata


LOCK_OWNER = "SH_TEST"
LOCK_TABLE1 = "UNIT_TABLE1"
LOCK_TABLE2 = "UNIT_TABLE2"
LOCK_TABLE3 = "UNIT_TABLE3"


class TestOrchestrationLock(TestCase):
    def test_orchestration_lock_for_table(self):
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

    def test_orchestration_lock_from_hybrid_metadata(self):
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

    def test_orchestration_blocking_lock(self):
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
        self.assertNotIn("blocker", exceptions_caught_in_threads)
        self.assertIn("waiter", exceptions_caught_in_threads)
        self.assertEqual(
            exceptions_caught_in_threads["waiter"]["exception"]["type"],
            OrchestrationLockTimeout,
        )
        t1.join()


if __name__ == "__main__":
    main()
