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

""" SimpleTimer: Library for providing simple elapsed times for logging
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
