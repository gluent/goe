import os
import redis
import json

from multiprocessing import Process

redis_conn = redis.Redis(charset="utf-8", decode_responses=True)


def sub(name: str):
    pubsub = redis_conn.pubsub()
    pubsub.subscribe("gdp_log")
    for message in pubsub.listen():
        print(message)


if __name__ == "__main__":
   Process(target=sub, args=("reader1",)).start()