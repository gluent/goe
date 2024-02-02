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

import threading
import queue
import traceback
import time

###########################################################################
# PollingThread
###########################################################################


class PollingThread(threading.Thread):
    """Run a function on a separate thread on a specified interval
    Store results in a queue.
    """

    def __init__(self, run_function, name=None, interval=1):
        """CONSTRUCTOR"""
        assert run_function
        assert callable(run_function), "run_function must be a function"
        assert isinstance(interval, (int, float)), "interval must be an int or float"
        assert isinstance(name, str), "name must be a str"
        threading.Thread.__init__(self, name=name)
        self._event = threading.Event()
        self._results_queue = queue.Queue()
        self._interval = interval
        self._exception = None
        self._run_function = run_function

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def run(self):
        """Override threading.Thread.run()
        Run the supplied function every interval and add any returned payload to the queue
        """
        while True:
            try:
                # Fetch the results from the function and add to the queue
                self.add_to_queue(self._run_function())
                time.sleep(self._interval)
            except:
                # Store thread exception as a property to be checked by the caller
                self._exception = traceback.format_exc()
                break
            finally:
                if self.event.is_set():
                    break

    def stop(self):
        """Stop the polling thread and let it finish any in-flight run function by joining,
        blocking main thread until done
        """
        self.event.set()
        self.join()

    def get_queue_length(self):
        return self._results_queue.qsize()

    def add_to_queue(self, item):
        self._results_queue.put(item)

    def drain_queue(self):
        queue_contents = []
        while not self._results_queue.empty():
            queue_contents.append(self._results_queue.get())
        return queue_contents

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def event(self):
        return self._event

    @property
    def exception(self):
        return self._exception

    @property
    def interval(self):
        return self._interval
