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

"""
Test library for orchestration commands.
We cannot test without making DB connections. These tests create a table in order to complete.
"""

import pytest
import threading
import traceback
from typing import TYPE_CHECKING

from goe.offload.offload_messages import VERBOSE, VVERBOSE
from goe.orchestration.orchestration_lock import OrchestrationLockTimeout
from goe.orchestration.orchestration_runner import OrchestrationRunner
from goe.schema_sync.schema_sync import SCHEMA_SYNC_LOCKED_MESSAGE_TEXT

from tests.integration.test_functions import (
    cached_current_options,
    get_default_test_user,
    run_setup_ddl,
)
from tests.testlib.test_framework.test_functions import (
    get_frontend_testing_api,
    get_test_messages,
    text_in_log,
)

if TYPE_CHECKING:
    from tests.testlib.test_framework.offload_test_messages import OffloadTestMessages


TABLE_NAME = "ORCH_RUNNER_TABLE"
LOCK_TABLE = "ORCH_LOCK_TABLE"

THREAD1_NAME = "thread1"
THREAD2_NAME = "thread2"


@pytest.fixture
def config():
    return cached_current_options()


@pytest.fixture
def schema():
    return get_default_test_user()


def competing_commands(
    thread1_fn,
    thread2_fn,
    messages: "OffloadTestMessages",
    test_id: str,
    schema_sync=False,
):
    """Run two function concurrently, with a very short delay between 1 and 2, and check that
    process 2 fails with a lock timeout.
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

    t1 = threading.Thread(name=THREAD1_NAME, target=thread1_fn)
    t2 = threading.Thread(name=THREAD2_NAME, target=thread2_fn)
    t1.start()
    # Initially I had a sleep here to try and force which thread would block the other but I was then having
    # problems with some threads finishing inside the fraction of the second I was pausing. And sometimes the delay
    # was not large enough to force the order. Instead we now let them run at the same time and ensure that one
    # worked and one failed acquiring lock.
    t2.start()
    t1.join()
    t2.join()

    if schema_sync:
        # Schema Sync doesn't abort with an exception when locked, instead it logs a notice and moves on.
        # It is not trivial to get at the messages object it populates so instead we're looking for no exceptions
        # at all because one thread will add the column and the other will skip, if the locking didn't work then
        # both threads would add the column and one would fail but again, SS catches any exception and logs it.
        # So the plan is:
        #   1) Ensure no exception at all in either thread
        #   2) Ensure a message was logged saying a thread was blocked
        if exceptions_caught_in_threads:
            messages.log("Unexpected exceptions were detected:\n")
            for thread_name in exceptions_caught_in_threads:
                messages.log(
                    "Unexpected %s exception: %s"
                    % (
                        thread_name,
                        str(
                            exceptions_caught_in_threads[thread_name]["exception"][
                                "value"
                            ]
                        ),
                    )
                )
                messages.log(
                    "".join(
                        traceback.format_exception(
                            exceptions_caught_in_threads[thread_name]["exception"][
                                "type"
                            ],
                            exceptions_caught_in_threads[thread_name]["exception"][
                                "value"
                            ],
                            exceptions_caught_in_threads[thread_name]["exception"][
                                "traceback"
                            ],
                        )
                    )
                )
        assert (
            not exceptions_caught_in_threads
        ), "Unexpected Schema Sync exceptions detected, something is amiss"
        assert text_in_log(
            SCHEMA_SYNC_LOCKED_MESSAGE_TEXT, search_from_text=test_id
        ), "Did not find Schema Sync lock message in log file"
    else:
        assert (
            exceptions_caught_in_threads
        ), "No sessions were blocked, something is amiss"
        if THREAD2_NAME in exceptions_caught_in_threads:
            # Assuming thread 2 is being blocked
            blocker = THREAD1_NAME
            waiter = THREAD2_NAME
        else:
            # Assuming thread 1 is being blocked
            blocker = THREAD2_NAME
            waiter = THREAD1_NAME
        messages.log(
            "Assuming %s is blocker due to %s exception: %s"
            % (
                blocker,
                waiter,
                str(exceptions_caught_in_threads[waiter]["exception"]["value"]),
            ),
            detail=VERBOSE,
        )
        if blocker in exceptions_caught_in_threads:
            messages.log(
                "Unexpected blocker exception: %s"
                % str(exceptions_caught_in_threads[blocker]["exception"]["value"])
            )
            messages.log(
                "".join(
                    traceback.format_exception(
                        exceptions_caught_in_threads[blocker]["exception"]["type"],
                        exceptions_caught_in_threads[blocker]["exception"]["value"],
                        exceptions_caught_in_threads[blocker]["exception"]["traceback"],
                    )
                )
            )
        assert blocker not in exceptions_caught_in_threads
        assert waiter in exceptions_caught_in_threads
        assert (
            exceptions_caught_in_threads[waiter]["exception"]["type"]
            is OrchestrationLockTimeout
        )
        messages.log(
            "Waiter encountered correct exception: %s"
            % str(exceptions_caught_in_threads[waiter]["exception"]["type"]),
            detail=VERBOSE,
        )


def test_orchestration_runner_offload(config, schema):
    messages = get_test_messages(config, "test_orchestration_runner_offload")
    frontend_api = get_frontend_testing_api(
        config, messages, trace_action="test_orchestration_runner_offload"
    )

    # Setup
    run_setup_ddl(
        config,
        frontend_api,
        messages,
        frontend_api.standard_dimension_frontend_ddl(schema, TABLE_NAME),
    )

    params = {
        "owner_table": f"{schema}.{TABLE_NAME}",
        "reset_backend_table": True,
        "execute": True,
    }
    status = OrchestrationRunner().offload(params)
    assert status


def test_orchestration_locks(config, schema):
    id = "test_orchestration_locks"
    log_id = f"{id}:offload-vs-offload"
    messages = get_test_messages(config, id)
    messages.log(log_id, detail=VVERBOSE)
    frontend_api = get_frontend_testing_api(config, messages, trace_action=id)

    # Setup
    run_setup_ddl(
        config,
        frontend_api,
        messages,
        frontend_api.standard_dimension_frontend_ddl(schema, LOCK_TABLE),
    )

    def offload_fn(schema):
        def test_fn():
            params = {
                "owner_table": f"{schema}.{LOCK_TABLE}",
                "reset_backend_table": True,
                "execute": True,
            }
            OrchestrationRunner().offload(params)

        return test_fn

    competing_commands(offload_fn(schema), offload_fn(schema), messages, log_id)
