""" SimpleTimer: Library for providing simple elapsed times for logging
    LICENSE_TEXT
"""

import time


class SimpleTimer(object):
    """Library for providing simple elapsed times for logging"""

    def __init__(self, name="timer"):
        self.name = name
        self.start = None
        self.last_call = None
        self.duration = None
        self.reset()

    @property
    def elapsed(self):
        return self.duration or (time.time() - self.start)

    def reset(self):
        self.start = time.time()
        self.last_call = self.start
        self.duration = 0

    def show(self):
        return "{desc} elapsed: {elapsed:5.3f} seconds".format(
            desc=self.name, elapsed=self.elapsed
        )

    def stop(self):
        self.duration = self.elapsed
        return "{desc} elapsed: {elapsed:5.3f} seconds".format(
            desc=self.name, elapsed=self.duration
        )
