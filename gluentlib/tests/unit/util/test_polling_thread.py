""" TestPollingThread: Unit test library to test polling_thread module.
"""
import threading
import time
import traceback

from unittest import TestCase, main
from itertools import groupby
from gluentlib.util.polling_thread import PollingThread


class TestPollingThread(TestCase):
    def test_OffloadTransportSqlStatsThread(self):
        """Test the OffloadTransportSqlStatsThread implementation of PollingThread
        using cx_Oracle output data
        """
        test_payload = [
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [],
            [("8w3d3hhnz2m3v", 0, 5000, 0, 0, 1), ("bux0xas0983mu", 0, 5000, 0, 0, 1)],
            [("8w3d3hhnz2m3v", 0, 5000, 0, 0, 1), ("bux0xas0983mu", 0, 5000, 0, 0, 1)],
            [
                ("8w3d3hhnz2m3v", 0, 45000, 0, 0, 1),
                ("bux0xas0983mu", 0, 35000, 0, 0, 1),
            ],
            [
                ("8w3d3hhnz2m3v", 0, 170000, 0, 0, 1),
                ("bux0xas0983mu", 0, 160000, 0, 0, 1),
            ],
            [
                ("8w3d3hhnz2m3v", 0, 221893, 0, 0, 1),
                ("bux0xas0983mu", 0, 301549, 0, 0, 1),
            ],
            [
                ("8w3d3hhnz2m3v", 0, 298323, 0, 0, 1),
                ("bux0xas0983mu", 0, 359409, 0, 0, 1),
            ],
        ]

        def get_test_stats_function(test_data):
            c = 0

            def stats_function():
                nonlocal c
                if c < (len(test_data) - 1):
                    p = test_data[c]
                    c += 1
                    return p
                else:
                    return test_data[-1]

            return stats_function

        # Exception hook is required to catch unhandled exceptions from threads
        exceptions_caught_in_threads = {}

        def custom_excepthook(args):
            thread_name = args.thread.name
            exceptions_caught_in_threads[thread_name] = {
                "thread": args.thread,
                "exception": {
                    "etype": args.exc_type,
                    "value": args.exc_value,
                    "tb": args.exc_traceback,
                },
            }

        polling_interval = 0.1
        wait_time = len(test_payload) * polling_interval
        threading.excepthook = custom_excepthook
        test_thread = PollingThread(
            run_function=get_test_stats_function(test_payload),
            name="test_OffloadTransportSqlStatsThread",
            interval=polling_interval,
        )

        test_thread.start()
        time.sleep(wait_time)
        test_thread.stop()
        self.assertFalse(test_thread.is_alive())
        self.assertEqual(test_thread.get_queue_length(), len(test_payload))
        queue_payload = test_thread.drain_queue()
        self.assertEqual(test_thread.get_queue_length(), 0)

        sql_info = []
        row_snapshots = [item for sublist in queue_payload for item in sublist]
        for sql_id_child_num, row_data in groupby(
            sorted(row_snapshots), lambda x: "%s:%s" % (x[0], x[1])
        ):
            rows = list(row_data)
            sql_id, child_number = sql_id_child_num.split(":")
            sql_info.append(
                (
                    sql_id,
                    child_number,
                    max([_[2] for _ in rows]),
                    max([_[3] for _ in rows]),
                    max([_[4] for _ in rows]),
                    max([_[5] for _ in rows]),
                )
            )
        self.assertTrue(sql_info)
        self.assertIn("8w3d3hhnz2m3v", [_[0] for _ in sql_info])
        self.assertIn(298323, [_[2] for _ in sql_info])
        self.assertEqual(657732, sum([_[2] for _ in sql_info]))

        if test_thread.name in exceptions_caught_in_threads:
            self.fail(
                "".join(
                    traceback.format_exception(
                        **exceptions_caught_in_threads[test_thread.name]["exception"]
                    )
                )
            )


if __name__ == "__main__":
    main()
