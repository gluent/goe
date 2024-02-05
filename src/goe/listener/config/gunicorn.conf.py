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
Gunicorn configuration file.

List of all available settings:
https://docs.gunicorn.org/en/latest/settings.html

"""
# Standard Library
import os

# GOE
from goe.listener.config import settings
from goe.listener.config.logging import Logger, StubbedGunicornLogger

logger = Logger.configure_logger()
# Server socket
#   bind - The socket to bind.
#       A string of the form: 'HOST', 'HOST:PORT', 'unix:PATH'.
#       An IP is a valid HOST.
#   backlog - The number of pending connections. This refers
#       to the number of clients that can be waiting to be
#       served. Exceeding this number results in the client
#       getting an error when attempting to connect. It should
#       only affect servers under significant load.
# #
#       Must be a positive integer. Generally set in the 64-2048
#       range.

bind = "{host}:{port}".format(
    host=settings.host,
    port=settings.port,
)
backlog = 2048
certfile = settings.certfile
keyfile = settings.keyfile


# Worker processes
#
#   workers - The number of worker processes that this server
#       should keep alive for handling requests.
#
#       A positive integer generally in the 2-4 x $(NUM_CORES)
#       range. You'll want to vary this a bit to find the best
#       for your particular application's work load.
#
#   worker_class - The type of workers to use. The default
#       sync class should handle most 'normal' types of work
#       loads. You'll want to read
#       http://docs.gunicorn.org/en/latest/design.html#choosing-a-worker-type
#       for information on when you might want to choose one
#       of the other worker classes.
#
#       A string referring to a Python path to a subclass of
#       gunicorn.workers.base.Worker. The default provided values
#       can be seen at
#       http://docs.gunicorn.org/en/latest/settings.html#worker-class
#
#   worker_connections - For the eventlet and gevent worker classes
#       this limits the maximum number of simultaneous clients that
#       a single process can handle.
#
#       A positive integer generally set to around 1000.
#
#   timeout - If a worker does not notify the master process in this
#       number of seconds it is killed and a new worker is spawned
#       to replace it.
#
#       Generally set to thirty seconds. Only set this noticeably
#       higher if you're sure of the repercussions for sync workers.
#       For the non sync workers it just means that the worker
#       process is still communicating and is not tied to the length
#       of time required to handle a single request.
#
#   keepalive - The number of seconds to wait for the next request
#       on a Keep-Alive HTTP connection.
#
#       A positive integer. Generally set in the 1-5 seconds range.
#
#   reload - Restart workers when code changes.
#
#       True or False

workers = settings.http_workers
worker_class = "goe.listener.wsgi.UvicornWorker"
worker_connections = 1000
timeout = 120
keepalive = 10
preload = True
graceful_timeout = 30
reload = settings.reload
#   spew - Install a trace function that spews every line of Python
#       that is executed when running the server. This is the
#       nuclear option.
#
#       True or False

spew = False


# Server mechanics
#
#   daemon - Detach the main Gunicorn process from the controlling
#       terminal with a standard fork/fork sequence.
#
#       True or False
#
#   raw_env - Pass environment variables to the execution environment.
#
#   pidfile - The path to a pid file to write
#
#       A path string or None to not write a pid file.
#
#   user - Switch worker processes to run as this user.
#
#       A valid user id (as an integer) or the name of a user that
#       can be retrieved with a call to pwd.getpwnam(value) or None
#       to not change the worker process user.
#
#   group - Switch worker process to run as this group.
#
#       A valid group id (as an integer) or the name of a user that
#       can be retrieved with a call to pwd.getgrnam(value) or None
#       to change the worker processes group.
#
#   umask - A mask for file permissions written by Gunicorn. Note that
#       this affects unix socket permissions.
#
#       A valid value for the os.umask(mode) call or a string
#       compatible with int(value, 0) (0 means Python guesses
#       the base, so values like "0", "0xFF", "0022" are valid
#       for decimal, hex, and octal representations)
#
#   tmp_upload_dir - A directory to store temporary request data when
#       requests are read. This will most likely be disappearing soon.
#
#       A path to a directory where the process owner can write. Or
#       None to signal that Python should choose one on its own.

daemon = False
# raw_env = [
#     'DJANGO_SECRET_KEY=something',
#     'SPAM=eggs',
# ]
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

#   Logging
#
#   logfile - The path to a log file to write to.
#
#       A path string. "-" means log to stdout.
#
#   loglevel - The granularity of log output
#
#       A string of "debug", "info", "warning", "error", "critical"

errorlog = "-"
accesslog = "-"
access_log_format = os.getenv(
    "OFFLOAD_LISTENER_GUNICORN_LOG_FORMAT",
    '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"',  # noqa: WPS323
)
logger_class = StubbedGunicornLogger

# LOG_LEVEL: str = "DEBUG"
# FORMAT: str = (
#     "timestamp=%(asctime)s pid=%(process)d loglevel=%(levelname)s msg=%(message)s"
# )
# LOGGING = {
#     "version": 1,  # mandatory field
#     # if you want to overwrite existing loggers' configs
#     "disable_existing_loggers": True,
#     "formatters": {
#         "key_value": {
#             "format": FORMAT,
#         },
#         "access": {
#             "()": "uvicorn.logging.AccessFormatter",
#             "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',  # noqa: WPS323 E501
#         },
#     },
#     "handlers": {
#         "console": {
#             "level": LOG_LEVEL,
#             "class": "logging.StreamHandler",
#             "formatter": "key_value",
#             "stream": "ext://sys.stdout",
#         },
#     },
#     "loggers": {
#         "gunicorn.error": {
#             "handlers": ["console"],
#             "level": LOG_LEVEL,
#             "propagate": True,
#         },
#     },
# }
# logging.config.dictConfig(LOGGING)


# Process naming
#
#   proc_name - A base to use with setproctitle to change the way
#       that Gunicorn processes are reported in the system process
#       table. This affects things like 'ps' and 'top'. If you're
#       going to be running more than one instance of Gunicorn you'll
#       probably want to set a name to tell them apart. This requires
#       that you install the setproctitle module.
#
#       A string or None to choose a default of something like 'gunicorn'.

proc_name = "goe-listener"


# Server hooks
#
#   post_fork - Called just after a worker has been forked.
#
#       A callable that takes a server and worker instance
#       as arguments.
#
#   pre_fork - Called just prior to forking the worker subprocess.
#
#       A callable that accepts the same arguments as after_fork
#
#   pre_exec - Called just prior to forking off a secondary
#       master process during things like config reloading.
#
#       A callable that takes a server instance as the sole argument.


def post_fork(server, worker):
    """Execute after a worker is forked."""


def pre_fork(server, worker):
    """Execute before a worker is forked."""


def pre_exec(server):
    """Execute before a new master process is forked."""


def when_ready(server):
    """Execute just after the server is started."""
    logger.info("GOE Listener HTTP workers started successfully.")


def worker_int(worker):
    """Execute just after a worker exited on SIGINT or SIGQUIT."""


def worker_abort(worker):
    """Execute when worker received the SIGABRT signal."""


def on_exit(server):
    logger.info("GOE Listener HTTP workers stopped")


def post_worker_init(worker):
    # Standard Library
    import atexit  # noqa: WPS433
    from multiprocessing.util import _exit_function  # noqa: WPS433 WPS450

    atexit.unregister(_exit_function)
