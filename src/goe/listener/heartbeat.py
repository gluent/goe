# Standard Library
import atexit
import logging
import multiprocessing
import threading
from multiprocessing.util import _exit_function  # noqa: WPS433 WPS450

# GOE
from goe.listener import services
from goe.listener.config.logging import Logger
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


def run_heartbeat() -> None:
    multiprocessing.freeze_support()
    Logger.configure_logger()
    runnify(services.heartbeat.start)()


if __name__ == "__main__":

    run_heartbeat()
