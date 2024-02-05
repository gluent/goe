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

import random

from goe.connect.connect_constants import (
    TEST_HDFS_DIRS_SERVICE_HDFS,
)
from goe.offload.offload_messages import VVERBOSE
from goe.filesystem.goe_dfs import (
    OFFLOAD_WEBHDFS_COMPATIBLE_FS_SCHEMES,
)
from goe.goe import ansi, log as offload_log, normal


class FatalTestFailure(Exception):
    # Use this instead of sys.exit(1)
    pass


def log(line: str, detail: int = normal, ansi_code=None):
    """Write log entry but without Redis interaction."""
    offload_log(line, detail=detail, ansi_code=ansi_code, redis_publish=False)


def section_header(h):
    log("\n%s" % h, ansi_code="underline")


def test_header(h):
    log("\n%s" % h)


def detail(d):
    log(str(d), ansi_code="grey")


def success(t):
    log("%s %s" % (t, ansi("Passed", "green")))


def failure(t, hint=None):
    log("%s %s" % (t, ansi("Failed", "red")))
    global failures
    failures = True
    if hint:
        log(hint, ansi_code="magenta")


def skipped(t):
    log("%s %s" % (t, ansi("Skipped", "yellow")))


def warning(t, hint=None):
    log("%s %s" % (t, ansi("Warning", "yellow")))
    global warnings
    warnings = True
    if hint:
        log(hint, ansi_code="magenta")


def debug(d):
    if not isinstance(d, str):
        log(str(d), detail=VVERBOSE)
    else:
        log(d, detail=VVERBOSE)


def get_one_host_from_option(option_host_value):
    """simple function but there were at least 3 different techniques in play for this so
    standardising here
    """
    return random.choice(option_host_value.split(",")) if option_host_value else None


def get_hdfs_dirs(
    orchestration_config,
    dfs_client,
    service_name=TEST_HDFS_DIRS_SERVICE_HDFS,
    include_hdfs_home=True,
):
    """return a list of HDFS directories but NOT as a set(), we want to retain the order so
    using an "if" to ensure no duplicate output
    """
    dirs = []
    if include_hdfs_home:
        dirs.append(orchestration_config.hdfs_home)
    dirs.append(orchestration_config.hdfs_load)
    offload_data_uri = dfs_client.gen_uri(
        orchestration_config.offload_fs_scheme,
        orchestration_config.offload_fs_container,
        orchestration_config.offload_fs_prefix,
    )
    if offload_data_uri not in dirs:
        if (
            service_name == TEST_HDFS_DIRS_SERVICE_HDFS
            or orchestration_config.offload_fs_scheme
            in OFFLOAD_WEBHDFS_COMPATIBLE_FS_SCHEMES
        ):
            dirs.append(offload_data_uri)
    return dirs
