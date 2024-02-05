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

# Standard Library
import atexit
import logging
import multiprocessing
import signal
import threading
from multiprocessing.util import _exit_function  # noqa: WPS433 WPS450

# Third Party Libraries
from anyio import create_task_group, open_signal_receiver
from anyio.abc import CancelScope

# GOE
from goe.listener.config.logging import Logger
from goe.listener.core.worker import background_worker
from goelib_contrib.asyncer import runnify

logger = logging.getLogger()
# the following is used to prevent this error when running using mutliprocessing
# Exception ignored in atexit callback: <function _exit_function at 0x10800bd90>
# Traceback (most recent call last):
#   File "/Users/cody/.pyenv/versions/3.10.2/lib/python3.10/multiprocessing/util.py", line 357, in _exit_function
#     p.join()
#   File "/Users/cody/.pyenv/versions/3.10.2/lib/python3.10/multiprocessing/process.py", line 147, in join
#     assert self._parent_pid == os.getpid(), 'can only join a child process'
# AssertionError: can only join a child process
if threading.current_thread() is not threading.main_thread():
    atexit.unregister(_exit_function)


def run_worker(preserve_signals: bool = True) -> None:
    multiprocessing.freeze_support()
    Logger.configure_logger()
    runnify(_run_worker)(preserve_signals=preserve_signals)


async def _signal_handler(scope: CancelScope):
    with open_signal_receiver(signal.SIGINT, signal.SIGTERM) as signals:
        async for signum in signals:
            if signum == signal.SIGINT:
                logger.debug(
                    "...signal interrupt detected.  shutting down worker process."
                )
            else:
                logger.debug("...shutting down worker process")
            scope.cancel()
            return


async def _run_worker(preserve_signals: bool = True) -> None:
    async with create_task_group() as tg:
        tg.start_soon(_signal_handler, tg.cancel_scope)
        await background_worker.start(preserve_signals=preserve_signals)
