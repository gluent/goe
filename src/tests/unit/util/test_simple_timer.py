""" TestSimpleTimer: Unit test library to test SimpleTimer functionality
"""
import time
from unittest import TestCase, main

from goe.util.simple_timer import SimpleTimer


class TestSimpleTimer(TestCase):
    def test_simple_timer(self):
        t = SimpleTimer()
        time.sleep(1)
        self.assertIsInstance(t.elapsed, float)
        self.assertGreater(t.elapsed, 1)
        self.assertIsInstance(t.show(), str)
        t.reset()
        self.assertLess(t.elapsed, 1)
        t.stop()
        t1 = t.elapsed
        time.sleep(0.5)
        # We stopped the timer before sleeping another second so elapsed should not be incrementing
        self.assertEqual(t.elapsed, t1)

        # Named timer
        t = SimpleTimer("test_timer")
        self.assertIsInstance(t.show(), str)
        self.assertIn("test_timer", t.show())


if __name__ == "__main__":
    main()
