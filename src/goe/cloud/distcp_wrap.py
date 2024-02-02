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

""" DistcpWrap: Abstraction over 'hadoop distcp'
"""

import datetime
import logging
import os, os.path
import re
import tempfile

from ..util.linux_cmd import LinuxCmd


###############################################################################
# EXCEPTIONS
###############################################################################
class DistcpWrapException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

# Default configuration file 'section' for optional parameters
DEFAULT_CFG_SECTION = "distcp"


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class DistcpWrap(object):
    """DistcpWrap: Abstraction over 'hadoop distcp' command"""

    # CONSTANTS: 'supported' sources and destinations
    VALID_SOURCES = ["hdfs", "s3n"]
    VALID_DESTINATIONS = ["hdfs", "s3n"]

    def __init__(self, cfg=None):
        """CONSTRUCTOR"""

        self._linux = LinuxCmd()  # Linux 'command executor'
        self._cfg = cfg  # Configuration object

        # Initialize transfer variables
        self._init_transfer()

        logger.debug("DistcpWrap() object successfully initialized")

    ###########################################################################
    # CONTEXT MANAGER ENTRIES
    ###########################################################################

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._remove_source_file()

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _init_transfer(self):
        """Initialize transfer objects"""

        self._src = None  # Distcopy source      (IN)
        self._dst = None  # Distcopy destination  (IN)
        self._user = None  # OS user to perform distcp (IN)
        self._update = None  # Whether to use distcp 'update' semantics (IN)
        self._delete = None  # Whether to use distcp 'delete' option (IN)

        self._cmd = None  # Distcopy command (INTERMEDIATE)

        self._status = None  # Distcopy status (OUT)

        self._tmp_src = None  # 'File list' file for distcp

    def _make_source_file(self, source_files):
        """Create 'temporary file' with a list of files to copy"""

        temp_src = "%s/goe.distcp.filelist.%s" % (
            tempfile.gettempdir(),
            datetime.datetime.now().strftime("%Y.%m.%d.%H.%M.%S.%f"),
        )
        # Saving it, so that we can remove it later
        self._tmp_src = temp_src
        logger.debug("Creating temporary 'file list' file: %s" % temp_src)

        with open(temp_src, "w") as sfile:
            for f in source_files:
                logger.debug("Adding file: %s to source list" % f)
                sfile.write("%s\n" % f)

        return temp_src

    def _remove_source_file(self):
        """Remove 'source file' if it exists"""

        if self._tmp_src and os.path.isfile(self._tmp_src):
            logger.debug("Removing temporary source file: %s" % self._tmp_src)
            os.remove(self._tmp_src)

    def _validate_dst(self, destination):
        """Validate destination"""

        def dest_valid(dest, valid_values):
            """Return True if 'destination' is within valid values,
            False otherwise
            """
            logger.debug("Validating destination: %s" % dest)
            return dest[: dest.index(":")] in valid_values

        if not dest_valid(destination, self.VALID_DESTINATIONS):
            raise DistcpWrapException(
                "Destination: %s is invalid. Supported destinations: %s"
                % (destination, self.VALID_DESTINATIONS)
            )

    def _construct_distcp_command(self, source, destination, update, delete):
        """Construct and return distcp command"""
        assert source, destination
        cmd = None

        update_semantics = "-update" if update else ""
        delete_marker = "-delete" if delete else ""

        no_of_mappers = (
            self._cfg.getint(DEFAULT_CFG_SECTION, "distcp_mappers")
            if self._cfg
            else None
        )
        mappers = "-m %d" % no_of_mappers if no_of_mappers else ""

        # If we have a list of files
        if isinstance(source, list):
            source_file = self._make_source_file(source)
            cmd = "hadoop distcp -i %s %s %s -f file://%s %s" % (
                mappers,
                update_semantics,
                delete_marker,
                source_file,
                destination,
            )
        else:
            cmd = "hadoop distcp -i %s %s %s %s %s" % (
                mappers,
                update_semantics,
                delete_marker,
                source,
                destination,
            )

        # Clean extra spaces
        cmd = re.sub(" +", " ", cmd)
        logger.debug("Hadoop distcp command: %s" % cmd)

        return cmd

    def _execute(self, cmd, user):
        """Execute distcp command (needs to be previously constructed)"""
        assert cmd

        logger.debug("Executing distcp command: %s" % cmd)
        status = self._linux.execute(cmd, user=user)
        logger.debug("Executing distcp command: %s. Status: %s" % (cmd, status))

        return status

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def source(self):
        return self._src

    @property
    def destination(self):
        return self._dst

    @property
    def user(self):
        return self._user

    @property
    def update_flag(self):
        return self._update

    @property
    def delete_flag(self):
        return self._delete

    @property
    def status(self):
        return self._status

    @property
    def cmd(self):
        return self._cmd

    @property
    def ready(self):
        """True if 'transfer request' is 'ready for execution'"""
        return True if self._cmd else False

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def prepare(self, source, destination, user=None, update=True, delete=False):
        """Prepare transfer between 'source' and 'destination':
            Validate src/dst, create transfer command, etc

        Both source and destination can be either hdfs or s3n
        i.e.
            hdfs://localhost:8020/user/goe/offload
            hdfs://server1.domain:8020/user/goe/offload
            s3n://goe.backup/user/goe/backup

        'source' can either be a string, in which case it is treated as a 'source url'
            or a list, in which case it is treated as a 'list of files to copy'
        Full path with 'type prefix' is expected for each 'file' in the list, i.e.:

        hdfs://localhost:8020/user/goe/offload/offload_bucket_id=0/data.0.parq
        s3n://goe.backup/user/goe/backup/offload_bucket_id=0/data.0.parq

        or, if None, everything under <source> is copied

        'user': the user to run distcp command, or 'current user' if None

        'update': whether to use distcp 'update' mode
        IMPORTANT: update and insert have DIFFERENT copy semantics !!! - check the doc!

        'delete': Delete files found on destination that do NOT exist on source
        """
        assert source and destination

        logger.debug(
            "PREPARING DISTCP: Source: %s, Destination: %s, User: %s"
            % (
                "<files>" if isinstance(source, list) else source,
                destination,
                user if user else "current user",
            )
        )

        # Initialize transfer objects
        self._init_transfer()

        # Validate source and destination
        self._validate_dst(destination)
        if isinstance(source, list):
            for f in source:
                self._validate_dst(f)
        else:
            self._validate_dst(source)

        self._src = source
        self._dst = destination
        self._user = user
        self._update = update
        self._delete = delete

        # Construct distcp command
        self._cmd = self._construct_distcp_command(
            self._src, self._dst, self._update, self._delete
        )
        logger.info("DISTCP command: %s" % self._cmd)

    def execute(self):
        """Execute previously prepared transfer request

        IMPORTANT:
            1. Files are always 'updated'
                (i.e. not overwritten if considered 'the same')

        Returns True if execution was successful, False otherwise
        """
        assert self.ready

        logger.info("Executing DISTCP command: %s" % self._cmd)
        self._status = self._execute(self._cmd, self._user)

        return self._status
