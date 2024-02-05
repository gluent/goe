#! /usr/bin/env python3

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

""" Test module for goelib.cloud.distcp_wrap
"""

import unittest

from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner

from goe.cloud.distcp_wrap import DistcpWrap, DistcpWrapException


class TestDistcpWrap(unittest.TestCase):
    def setUp(self):
        self.distcp = DistcpWrap()

    ###############################################################
    # Testing 'prepare' phase
    ###############################################################

    def test_args_not_supplied(self):
        with self.assertRaises(TypeError):
            self.distcp.prepare()
        with self.assertRaises(TypeError):
            self.distcp.prepare(source="source")
        with self.assertRaises(AssertionError):
            self.distcp.prepare(source="source", destination="")
        with self.assertRaises(TypeError):
            self.distcp.prepare(destination="destination")
        with self.assertRaises(AssertionError):
            self.distcp.prepare(source="", destination="destination")

    def test_invalid_destinations(self):
        with self.assertRaises(DistcpWrapException):
            self.distcp.prepare(
                source="s3://invalid/source", destination="hdfs://valid/destination"
            )
        with self.assertRaises(DistcpWrapException):
            self.distcp.prepare(
                source="s3n://valid/source", destination="http://invalid/destination"
            )
        with self.assertRaises(DistcpWrapException):
            self.distcp.prepare(
                source="s3://invalid/source", destination="http://invalid/destination"
            )

    def test_valid_destination_list(self):
        sources = ["hdfs://valid/source", "s3n://valid/source"]
        with DistcpWrap() as distcp:
            distcp.prepare(source=sources, destination="s3n://valid/destination")
            self.assertEqual(distcp.source, sources)

    def test_invalid_destination_list(self):
        sources = ["http://valid/source", "s3n://valid/source"]
        with self.assertRaises(DistcpWrapException):
            self.distcp.prepare(source=sources, destination="s3n://valid/destination")

    def test_default_input(self):
        with DistcpWrap() as distcp:
            distcp.prepare(
                source="hdfs://valid/source", destination="s3n://valid/destination"
            )
            self.assertEqual(distcp.source, "hdfs://valid/source")
            self.assertEqual(distcp.destination, "s3n://valid/destination")
            self.assertIsNone(distcp.user)
            self.assertTrue(distcp.update_flag)
            self.assertFalse(distcp.delete_flag)

    def test_non_default_input(self):
        with DistcpWrap() as distcp:
            distcp.prepare(
                source="hdfs://valid/source",
                destination="s3n://valid/destination",
                user="goe",
                update=False,
                delete=True,
            )
            self.assertEqual(distcp.source, "hdfs://valid/source")
            self.assertEqual(distcp.destination, "s3n://valid/destination")
            self.assertEqual(distcp.user, "goe")
            self.assertFalse(distcp.update_flag)
            self.assertTrue(distcp.delete_flag)

    def test_update_mode(self):
        with DistcpWrap() as distcp:
            distcp.prepare(
                source="hdfs://valid/source",
                destination="s3n://valid/destination",
                update=True,
            )
            self.assertTrue(distcp.update_flag)
            self.assertEqual(
                distcp.cmd,
                "hadoop distcp -i -update hdfs://valid/source s3n://valid/destination",
            )

    def test_delete_mode(self):
        with DistcpWrap() as distcp:
            distcp.prepare(
                source="hdfs://valid/source",
                destination="s3n://valid/destination",
                delete=True,
                update=False,
            )
            self.assertTrue(distcp.delete_flag)
            self.assertEqual(
                distcp.cmd,
                "hadoop distcp -i -delete hdfs://valid/source s3n://valid/destination",
            )

    def test_update_and_delete_mode(self):
        with DistcpWrap() as distcp:
            distcp.prepare(
                source="hdfs://valid/source",
                destination="s3n://valid/destination",
                delete=True,
                update=True,
            )
            self.assertTrue(distcp.update_flag)
            self.assertTrue(distcp.delete_flag)
            self.assertEqual(
                distcp.cmd,
                "hadoop distcp -i -update -delete hdfs://valid/source s3n://valid/destination",
            )

    def test_hdfs_to_s3(self):
        with DistcpWrap() as distcp:
            distcp.prepare(
                source="hdfs://valid/source", destination="s3n://valid/destination"
            )
            self.assertIsNone(distcp.user)
            self.assertTrue(distcp.update_flag)
            self.assertFalse(distcp.delete_flag)
            self.assertEqual(
                distcp.cmd,
                "hadoop distcp -i -update hdfs://valid/source s3n://valid/destination",
            )

    def test_s3_to_hdfs(self):
        with DistcpWrap() as distcp:
            distcp.prepare(
                source="s3n://valid/source", destination="hdfs://valid/destination"
            )
            self.assertIsNone(distcp.user)
            self.assertTrue(distcp.update_flag)
            self.assertFalse(distcp.delete_flag)
            self.assertEqual(
                distcp.cmd,
                "hadoop distcp -i -update s3n://valid/source hdfs://valid/destination",
            )

    def test_s3_to_s3(self):
        with DistcpWrap() as distcp:
            distcp.prepare(
                source="s3n://valid/source", destination="s3n://valid/destination"
            )
            self.assertIsNone(distcp.user)
            self.assertTrue(distcp.update_flag)
            self.assertFalse(distcp.delete_flag)
            self.assertEqual(
                distcp.cmd,
                "hadoop distcp -i -update s3n://valid/source s3n://valid/destination",
            )

    def test_hdfs_to_hdfs(self):
        with DistcpWrap() as distcp:
            distcp.prepare(
                source="hdfs://valid/source", destination="hdfs://valid/destination"
            )
            self.assertIsNone(distcp.user)
            self.assertTrue(distcp.update_flag)
            self.assertFalse(distcp.delete_flag)
            self.assertEqual(
                distcp.cmd,
                "hadoop distcp -i -update hdfs://valid/source hdfs://valid/destination",
            )


if __name__ == "__main__":
    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
    unittest.main(testRunner=runner)
