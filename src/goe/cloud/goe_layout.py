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

""" goe_layout: Representation of GOE 'HDFS tree' layout
"""

import datetime
import logging
import re


###############################################################################
# EXCEPTIONS
###############################################################################
class GOELayoutException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# Expected length of 'timestamp' in 'goe timestamp' column
TS_EXPECTED_LENGTH = {"d": 10, "m": 7, "y": 4}

# Regex to parse 'goe timestamp' column
TS_REGEX = re.compile("goe_part_(m|d|y)_time_id=([^/]+)")

# Regex to parse 'partitions'
PARTITION_REGEX = re.compile("^(\S+)=(\S+)$")


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class GOEHdfsLayout(object):
    """MIXIN: GOEHdfsLayout: Representation of goe hive/impala 'file tree'

    Specifically, provides primitives to parse/search 'goe timestamp' columns
    """

    __slots__ = ()  # No internal data

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def add_partitions(self, data, file_key):
        """Parse 'partition key/values' and add them to 'data'
        if possible
        """

        partitions = self.extract_partitions(file_key)
        if partitions:
            data["partitions"] = partitions

        return data

    def extract_partitions(self, file_key):
        """Extract 'hive partitions' from 'file_key' string

        Partitions are /key=value/ chunks in the string
        """
        partitions = []

        for chunk in file_key.split("/"):
            partition_match = PARTITION_REGEX.match(chunk)
            if partition_match:
                key, value = partition_match.groups()
                if value.isdigit():
                    value = int(value)
                partitions.append((key, value))

        return partitions

    def add_goets(self, data, file_key):
        """Parse 'goe timestamp' and add 'timestamp' key/value pair to 'data'
        if possible
        """

        goe_ts = self.extract_goets(file_key)
        if goe_ts is not None:
            data["timestamp"] = goe_ts

        return data

    def extract_goets(self, file_key, missing_value=99999999):
        """Extract 'goe timestamp' from a 'file key' and return as a number

        I.e.:
        offload_bucket_id=9/goe_part_m_time_id=2015-06/data.0.parq -> 20150600
        offload_bucket_id=9/goe_part_y_time_id=2015/data.0.parq -> 20150000
        offload_bucket_id=9/goe_part_d_time_id=2015-06-01/data.0.parq -> 20150601

        If no 'goe timestamp' columns, return 'missing_value'
        offload_bucket_id=9/data.0.parq -> 99999999 (default)

        Missing value is: max(): 99999999 by default, on the assumption that
        we want to copy the entire table in such case (i.e. such tables are 'small')
        """
        assert file_key

        match = TS_REGEX.search(file_key)
        if match:
            ts_resolution, ts = match.group(1), match.group(2)

            # Basic timestamp validation
            if len(ts) != TS_EXPECTED_LENGTH[ts_resolution]:
                logger.warn(
                    "Invalid timestamp: %s detected for: '%s' granularity."
                    % (ts, ts_resolution)
                )

                return None

            ts_number = int(ts.replace("-", ""))
            if "y" == ts_resolution:
                ts_number *= 10000
            elif "m" == ts_resolution:
                ts_number *= 100

            logger.debug(
                "Detected 'goe timestamp': %s.%s in file key: %s. Extracting timestamp: %d"
                % (ts_resolution, ts, file_key, ts_number)
            )
            return ts_number
        else:
            logger.debug(
                "Did not detect 'goe' timestamp in file key: %s. Assigning default: %d"
                % (file_key, missing_value)
            )
            return missing_value


###############################################################################
# STANDALONE ROUTINES
###############################################################################


def datestr_to_goets(date_str, date_format="%Y-%m-%d"):
    """Convert date string (with format) into 'goe timestamp'

    i.e. '2015-01-15' to 20150115
    """

    ts = datetime.datetime.strptime(date_str, date_format)
    timestamp = ts.year * 10000 + ts.month * 100 + ts.day

    return timestamp
