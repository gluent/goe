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
import logging
import logging.handlers
from logging import Logger as BaseLogger

# Third Party Libraries
from gunicorn.glogging import Logger as GunciornLogger


class LogFormatter(logging.Formatter):
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = "[%(asctime)s] [%(levelname)s ]    %(message)s [%(process)s]"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


HANDLER = logging.StreamHandler()
HANDLER.setFormatter(LogFormatter())
FORMAT = "[%(asctime)s] - %(process)s - %(levelname)s - %(message)s"


class StubbedGunicornLogger(GunciornLogger):
    """Customized Gunicorn Logger"""

    def setup(self, cfg):
        """Configures logger"""
        self.error_logger = logging.getLogger("gunicorn.error")
        self.error_logger.addHandler(HANDLER)
        self.error_logger.setLevel("INFO")
        self.access_logger = logging.getLogger("gunicorn.access")
        self.access_logger.addHandler(HANDLER)
        self.error_logger.setLevel("ERROR")
        # self.access_logger.setLevel(str(settings.server.log_level).upper())


class Logger:
    """Application logger"""

    @classmethod
    def configure_logger(cls) -> BaseLogger:
        logger = logging.getLogger()
        while logger.hasHandlers():
            logger.removeHandler(logger.handlers[0])
        """Configure unified appplciation logger"""
        for name in logging.root.manager.loggerDict.keys():
            logging.getLogger(name).handlers = []
            logging.getLogger(name).propagate = True
        handler = logging.StreamHandler()
        handler.setFormatter(LogFormatter())
        logging.basicConfig(handlers=[handler], level=0)
        logging.root.setLevel("INFO")
        return logger
