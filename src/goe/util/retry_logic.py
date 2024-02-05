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

""" retry_logic: Functions to support re-trying of unsuccessful calls
"""

import logging
import time

from functools import wraps


###############################################################################
# EXCEPTIONS
###############################################################################
class RetryLogicException(Exception):
    pass


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


def uniform_backoff(retries=1, delay_seconds=1):
    """Return "uniform" sequence of "backoff delays" """
    return [delay_seconds for _ in range(retries)]


def exponential_backoff(
    retries=1, initial_delay_seconds=1, max_delay_seconds=64, multiplier=1.2
):
    """Return "exponential backoff" sequence of "delays"
    Delays are capped at 'max_delay' so that not to become unreasonable
    """
    ret = []
    delay_seconds = float(initial_delay_seconds)
    for _ in range(retries):
        ret.append(round(min(delay_seconds, max_delay_seconds), 2))
        delay_seconds *= multiplier

    return ret


def retry_call(retries=1, delay=1, retriable_exceptions=[Exception]):
    """(original) Retry routine with uniform delay"""
    return retry_call_base(uniform_backoff(retries, delay), retriable_exceptions)


def retry_call_base(retry_generator, retriable_exceptions=[Exception]):
    """DECORATOR: Retry call until it returns success or # of retries is exhausted

    To use: callable = retry_call(retry=..., msg=...)(callable)
    """

    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            result = None

            for i, delay in enumerate(retry_generator):
                logger.debug("Try: %d running function: %s" % (i, func.__name__))
                try:
                    result = func(*args, **kwargs)
                    if result:
                        logger.debug("Try: %d is SUCCESSFUL" % i)
                        return result
                    else:
                        logger.debug("Try: %d has FAILED" % i)
                except Exception as e:
                    if any(isinstance(e, _) for _ in retriable_exceptions):
                        logger.debug(
                            "Try: %d raised a re-triable EXCEPTION: %s" % (i, e),
                            exc_info=True,
                        )
                    else:
                        logger.debug(
                            "Try: %d raised a critical EXCEPTION: %s" % (i, e),
                            exc_info=True,
                        )
                        raise

                logger.debug("Sleeping: %.2f seconds" % delay)
                time.sleep(delay)

            if not result:
                raise RetryLogicException(
                    "Function: %s is still unsuccessful after ALL retries"
                    % func.__name__
                )

        return wrapper

    return decorate


if __name__ == "__main__":
    import random

    def a(p):
        print(p)

        choice = random.choice([True, False, None])

        if choice is None:
            raise RetryLogicException("Test exception")
        else:
            return choice

    class b:
        def __init__(self):
            setattr(
                self, "f", retry_call_base(exponential_backoff(3))(getattr(self, "f"))
            )

        def f(self, p):
            return a(p)

    def main():
        logging.basicConfig(level=logging.DEBUG)

        print("\nUNIFORM BACKOFF\n")
        ad = retry_call_base(uniform_backoff(3))(a)
        try:
            ad("Hello")
        except RetryLogicException as e:
            print("Exception: %s" % e)

        print("\nEXPONENTIAL BACKOFF\n")
        ad = retry_call_base(exponential_backoff(3))(a)
        try:
            ad("Hello")
        except RetryLogicException as e:
            print("Exception: %s" % e)

        print("\nLEGACY\n")
        ad = retry_call(3)(a)
        try:
            ad("Hello")
        except RetryLogicException as e:
            print("Exception: %s" % e)

        print("\nCLASS\n")
        b_obj = b()
        try:
            b_obj.f("Hello")
        except RetryLogicException as e:
            print("Exception: %s" % e)

    main()
