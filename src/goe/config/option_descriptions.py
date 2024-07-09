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

""" option_descriptions: Library of constants defining descriptions for options.
    In the future we expect to refactor all option processing, including descriptions, and this module will
    may become redundant at that time.
"""

DATA_SAMPLE_PARALLELISM = (
    "Degree of parallelism to use when sampling RDBMS data for columns with no precision/scale properties. "
    "Values of 0 or 1 will execute the query without parallelism"
)

RESET_BACKEND_TABLE = "Remove backend data table. Use with caution - this will delete previously offloaded data for this table!"

REUSE_BACKEND_TABLE = (
    "Allow Offload to re-use an empty backend table when there is already Offload metadata. "
    "This may be useful if a backend table had data removed by an administrator and a re-offload is required"
)

VERIFY_PARALLELISM = (
    "Degree of parallelism to use for the RDBMS query executed when validating an offload. "
    "Values of 0 or 1 will execute the query without parallelism. Values > 1 will force a parallel query of the given degree. "
    "If unset, the RDBMS query will fall back to using the behavior specified by RDBMS defaults"
)
