#! /usr/bin/env python3
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


# GOE
from goe.listener.config.application import settings
from goe.listener.wsgi import run_wsgi


def run_listener(
    host: str = settings.host,
    port: int = settings.port,
    workers: int = settings.http_workers,
):
    """Run GOE Listener."""
    run_wsgi(host, port, workers)


if __name__ == "__main__":
    run_listener()
