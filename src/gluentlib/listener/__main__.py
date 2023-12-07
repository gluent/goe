#! /usr/bin/env python3
# -*- coding: utf-8 -*-
"""Application Web Server Gateway Interface - gunicorn."""


# Gluent
from gluentlib.listener.config.application import settings
from gluentlib.listener.wsgi import run_wsgi


def run_listener(
    host: str = settings.host,
    port: int = settings.port,
    workers: int = settings.http_workers,
):
    """Run Gluent Listener."""
    run_wsgi(host, port, workers)


if __name__ == "__main__":

    run_listener()
