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

""" ParallelExecutor: Parallel execution of tasks with various methods
"""

import logging

import concurrent.futures

from goe.util.misc_functions import typed_property


###############################################################################
# EXCEPTIONS
###############################################################################
class ParallelExecException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# 'not executed' job completion status
# I.e.: file was not copied because it was deemed the same
STATUS_NOOP = "<noop>"


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class ParallelExecutor(object):
    """Execute 'tasks' in parallel"""

    submitted = typed_property("submitted", int)
    executed = typed_property("executed", int)
    successful = typed_property("successful", int)
    failed = typed_property("failed", int)
    noop = typed_property("noop", int)

    def __init__(self):
        """CONSTRUCTOR"""
        self._initialize()

        logger.debug("ParallelExecutor() object successfully initialized")

    def _initialize(self):
        """Initialize internal counters"""
        self._submitted = 0  # Number of jobs submitted
        self._executed = 0  # .. executed
        self._successful = 0  # .. successful
        self._failed = 0  # .. failed
        self._noop = 0  # .. Noop
        self._tasks = {}  # Task-by-id tracking

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def tasks(self):
        return self._tasks

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def execute_sequentially(self, job_tasks, ids=False):
        """Execute 'job_tasks' sequentially (aka: 'not in threads')

        No need to 'kill_on_first_error' as the first exception will take care of that

        This may be useful for debugging
        """
        assert job_tasks

        status = []
        self._initialize()

        logger.debug("Starting sequential execution")

        for job_id, task_item in enumerate(job_tasks):
            task, args = None, []
            if not isinstance(task_item, list):
                if ids:
                    raise ParallelExecException(
                        "'ids' parameter is incompatible with 'simple task' mode"
                    )
                task = task_item
            else:
                task, args = task_item[0], task_item[1:]
                if ids:
                    job_id = args[0]
                    del args[0]

            self._submitted += 1
            st = task(*args)  # Execute the command
            logger.debug("Result for job: %s is: %s" % (job_id, st))
            self._tasks[job_id] = st

            self._executed += 1
            if STATUS_NOOP == st:
                self._noop += 1
            elif st:
                self._successful += 1
            else:
                self._failed += 1

        # Display final status
        logger.debug(
            "OVERALL STATUS=%s. Submitted: %d Executed: %d Successful: %d Failed: %d Noop: %d"
            % (
                self._tasks,
                self._submitted,
                self._executed,
                self._successful,
                self._failed,
                self._noop,
            )
        )

        return list(self._tasks.values())

    def execute_in_threads(
        self, parallel, job_tasks, kill_on_first_error=True, ids=False
    ):
        """Execute 'job_tasks' in multiple parallel threads

        'job_tasks': List of [callable, arg1, arg2, ...]
        'kill_on_first_error': Stop executing as soon as the 1st exception is detected
        'ids': True: Expect an additional 'id' parameter after the task 'function'
                     + track tasks by id

        Returns 'overall' status (individual statuses ANDed together)
        """
        assert job_tasks

        if not parallel:
            return self.execute_sequentially(job_tasks, ids)

        self._initialize()

        logger.debug("Starting execution with: %s parallel threads" % parallel)

        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
            # Submit 'individual tasks' for parallel execution
            submitted_threads = {}
            for thread_id, task_item in enumerate(job_tasks):
                if not isinstance(task_item, list):
                    if ids:
                        raise ParallelExecException(
                            "'ids' parameter is incompatible with 'simple task' mode"
                        )
                    task = task_item
                    submitted_threads[executor.submit(task)] = thread_id
                else:
                    task, args = task_item[0], task_item[1:]
                    if ids:
                        thread_id = args[0]
                        del args[0]
                    submitted_threads[executor.submit(task, *args)] = thread_id
                self._submitted += 1

            # Wait for and grab results
            for future in concurrent.futures.as_completed(submitted_threads):
                thread_id = submitted_threads[future]
                try:
                    st = future.result()
                    logger.debug("Result for thread: %s is: %s" % (thread_id, st))
                    self._tasks[thread_id] = st
                    self._executed += 1
                    if STATUS_NOOP == st:
                        self._noop += 1
                    elif st:
                        self._successful += 1
                    else:
                        self._failed += 1
                except Exception as e:
                    msg = "Exception detected: %s in parallel thread: %s" % (
                        e,
                        thread_id,
                    )
                    if kill_on_first_error:
                        raise ParallelExecException(msg)
                    else:
                        logger.warn(msg)
                    self._tasks[thread_id] = False
                    self._executed += 1
                    self._failed += 1

        # Display final status
        logger.debug(
            "OVERALL STATUS=%s. Submitted: %d Executed: %d Successful: %d Failed: %d Noop: %d"
            % (
                self._tasks,
                self._submitted,
                self._executed,
                self._successful,
                self._failed,
                self._noop,
            )
        )

        return list(self._tasks.values())
