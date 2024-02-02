# -*- coding: utf-8 -*-

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

"""Application Web Server Gateway Interface - gunicorn."""
# Standard Library
import asyncio
import multiprocessing
import os
import signal
import sys
import threading

# Third Party Libraries
from gunicorn.app.base import Application
from gunicorn.arbiter import Arbiter
from uvicorn.main import Server
from uvicorn.workers import UvicornWorker as BaseUvicornWorker

# GOE
from goe.listener.asgi import get_app
from goe.listener.config import settings


class UvicornWorker(BaseUvicornWorker):
    CONFIG_KWARGS = {"loop": "uvloop", "http": "httptools", "lifespan": "auto"}

    def _install_sigquit_handler(self, server: Server) -> None:
        """Workaround to install a SIGQUIT handler on workers.
        Ref.:
        - https://github.com/encode/uvicorn/issues/1116
        - https://github.com/benoitc/gunicorn/issues/2604
        """
        if threading.current_thread() is not threading.main_thread():
            # Signals can only be listened to from the main thread.
            return

        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGQUIT, self.handle_exit, signal.SIGQUIT, None)

    async def _serve(self) -> None:
        self.config.app = self.wsgi
        server = Server(config=self.config)
        self._install_sigquit_handler(server)
        await server.serve(sockets=self.sockets)
        if not server.started:
            sys.exit(Arbiter.WORKER_BOOT_ERROR)


class ApplicationLoader(Application):
    """Bootstraps the WSGI app"""

    def __init__(
        self,
        options=None,
    ):
        self.options = options or {}
        self.config_path = self.options.pop("config", None)
        super().__init__()

    def init(self, parser, options, args):
        """Class ApplicationLoader object constructor."""
        self.options = options
        self.cfg.set("default_proc_name", args[0])

    def load_config(self):
        """Load config from passed options"""
        if self.config_path:
            if not os.path.exists(self.config_path):
                self.config_path = f"{self.config_path}c"
            self.load_config_from_file(self.config_path)
        config = {
            key: value
            for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        """Load application."""
        return get_app()


def run_wsgi(
    host: str,
    port: int,
    http_workers: int,
    reload: bool,
):
    """Run gunicorn WSGI with ASGI workers."""
    # next line required for pyinstaller to prevent endless reboot
    multiprocessing.freeze_support()
    sys.argv = [
        "--gunicorn",
    ]
    if reload:
        sys.argv.append("-r")
    sys.argv.append("goe.listener.asgi:application")
    ApplicationLoader(
        options={
            "host": host,
            "workers": str(http_workers),
            "port": str(port),
            "reload": reload,
            "loglevel": "info",
            "config": str(settings.gunicorn_conf),
        },
    ).run()


if __name__ == "__main__":
    run_wsgi(
        host=settings.server.host,
        port=settings.server.port,
        http_workers=settings.server.http_workers,
        reload=settings.server.reload,
    )
