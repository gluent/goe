# Copyright 2016 The GOE Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import redis
import json

from multiprocessing import Process

redis_conn = redis.Redis(charset="utf-8", decode_responses=True)


def sub(name: str):
    pubsub = redis_conn.pubsub()
    pubsub.subscribe("goe_log")
    for message in pubsub.listen():
        print(message)


if __name__ == "__main__":
    Process(target=sub, args=("reader1",)).start()
