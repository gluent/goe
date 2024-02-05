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

""" GOE system environment extractor
"""

import datetime
import logging
import os
import os.path
import re
import socket

from goe.util.linux_cmd import LinuxCmd
from goe.util.ora_query import OracleQuery
from goe.util.password_tools import PasswordTools
from goe.offload.offload_constants import (
    BACKEND_DISTRO_CDH,
    BACKEND_DISTRO_EMR,
    BACKEND_DISTRO_HDP,
    BACKEND_DISTRO_MAPR,
)


###############################################################################
# EXCEPTIONS
###############################################################################
class SystemEnvironmentException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

DEFAULT_DEV_ROOT = "~/dev/offload"
DEFAULT_OFFLOAD_ROOT = "~/offload"

OFFLOAD_VERSION_FILE = "version"
OFFLOAD_CHANGESET_FILE = "setup/sql/create_offload_package_spec.sql"

HADOOP_TYPE_UNKNOWN = "Unknown"

HADOOP_DB_TYPE_IMPALA = "Impala"
HADOOP_DB_TYPE_HIVE = "Hive"
HADOOP_DB_TYPE_UNKNOWN = "Unknown"

# Represents environment config that cannot be discovered for whatever reason
ITEM_UNDISCOVERED = "Undiscovered"

# Regex to parse 'changset' out of OFFLOAD_CHANGESET_FILE
RE_GC_VERSION = re.compile("gc_version\s+constant\s+varchar2.*:=\s*'.*\((\S+)\)'", re.I)


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class SystemEnvironment(object):
    """SystemEnvironment: Extract and present relevant pieces
    from (GOE) system environment

    parent class
    """

    def __init__(self, root_dir=None):
        """CONSTRUCTOR"""
        self._root_dir = root_dir

        self._linux = LinuxCmd()
        self._oracle = None

        self._host = None
        self._os_release = None
        self._os_issue = None
        self._oracle_version = None
        self._hadoop_distro = None
        self._goe_hadoop_distro = None
        self._hadoop_version = None
        self._hadoop_db_type = None
        self._hadoop_db_version = None
        self._goe_major_version = None

        logger.debug(
            "SystemEnvironment() object successfully initialized for: %s"
            % self._root_dir
        )

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _oracle_connect(self):
        """Connect to ORACLE with default user/password/dsn"""

        db_user = os.environ.get("ORA_ADM_USER")
        db_passwd = os.environ.get("ORA_ADM_PASS")
        db_dsn = os.environ.get("ORA_CONN")

        if not db_user or not db_passwd or not db_dsn:
            logger.warn(
                "Unable to instantiate ORACLE query executor. Please, enable ORA_ADM_ environment variables"
            )

        if os.environ.get("PASSWORD_KEY_FILE"):
            pass_tool = PasswordTools()
            db_passwd = pass_tool.b64decrypt(db_passwd)

        return OracleQuery(db_user, db_passwd, db_dsn)

    def _get_host(self):
        """Get host name"""
        return socket.gethostname()

    def _get_os_release(self):
        """Get OS release"""

        if not self._linux.execute("uname -sr"):
            logger.warn("Unable to extract OS release")
            return ITEM_UNDISCOVERED

        return self._linux.stdout.strip()

    def _get_os_issue(self):
        """Get OS issue"""

        if not self._linux.execute("head -1 /etc/issue"):
            logger.warn("Unable to extract OS 'issue'")
            return ITEM_UNDISCOVERED

        return self._linux.stdout.strip()

    def _get_oracle_version(self):
        """Get ORACLE Version and Edition"""
        if not self.oracle:
            logger.warn("Unable to discover ORACLE version")
            return ITEM_UNDISCOVERED

        sql = "select banner from v$version where banner like '%Oracle Database%'"
        result = self.oracle.execute(
            sql, cursor_fn=lambda c: c.fetchall(), as_dict=True
        )
        return result[0]["BANNER"].strip()

    def _get_hadoop_distro(self):
        """Get HADOOP distribution"""

        if not self._linux.execute("hadoop version"):
            logger.warn("Unable to extract Hadoop distro")
            return ITEM_UNDISCOVERED

        hadoop = self._linux.stdout.strip()

        if b"hortonworks" in hadoop:
            return BACKEND_DISTRO_HDP
        elif b"cdh" in hadoop:
            return BACKEND_DISTRO_CDH
        elif b"mapr" in hadoop:
            return BACKEND_DISTRO_MAPR
        else:
            logger.warn("Unable to parse out hadoop distro out of: %s" % hadoop)
            return HADOOP_TYPE_UNKNOWN

    def _get_goe_hadoop_distro(self):
        """Get HADOOP distribution with GOE overrides"""
        if "BACKEND_DISTRIBUTION" in os.environ:
            return os.environ["BACKEND_DISTRIBUTION"]
        else:
            return self._get_hadoop_distro()

    def _get_hadoop_version(self):
        """Get HADOOP version"""

        if not self._linux.execute("hadoop version | head -1"):
            logger.warn("Unable to extract Hadoop version")
            return ITEM_UNDISCOVERED

        return self._linux.stdout.replace("Hadoop ", "").strip()

    def _get_hadoop_db_type(self):
        """Get HADOOP db type (hive, impala) currently in use"""

        db_port = os.environ.get("HIVE_SERVER_PORT")

        if "21050" == db_port:
            return HADOOP_DB_TYPE_IMPALA
        elif "10000" == db_port:
            return HADOOP_DB_TYPE_HIVE
        else:
            logger.warn("Unable to recognize db port: %s" % db_port)
            return HADOOP_DB_TYPE_UNKNOWN

    def _get_hadoop_db_version(self, db_type):
        """Get Hive/Impala version"""

        def get_hive_version():
            """Get HIVE version"""
            if not self._linux.execute("hive --version 2>/dev/null | head -1"):
                logger.warn("Unable to extract HIVE version")
                return ITEM_UNDISCOVERED
            return self._linux.stdout.replace("Hive ", "").strip()

        def get_impala_version():
            """Get IMPALA version"""
            if not self._linux.execute("impala-shell --version 2>/dev/null | head -1"):
                logger.warn("Unable to extract IMPALA version")
                return ITEM_UNDISCOVERED
            return self._linux.stdout.replace("Impala Shell ", "").strip()

        if HADOOP_DB_TYPE_IMPALA == db_type:
            return get_impala_version()
        elif HADOOP_DB_TYPE_HIVE == db_type:
            return get_hive_version()
        else:
            logger.warn("Unable to recognize db type: %s" % db_type)
            return ITEM_UNDISCOVERED

    def _get_goe_major_version(self):
        """Get major version from 'version' file"""
        version_file = None
        version = ITEM_UNDISCOVERED

        if self._root_dir:
            version_file = os.path.join(self._root_dir, OFFLOAD_VERSION_FILE)

        if not self._root_dir:
            logger.warn("Root directory NOT specified. Unable to extract version")
        elif not os.path.exists(version_file):
            logger.warn("Version file: %s does NOT exist" % version_file)
        else:
            logger.debug("Reading version file: %s" % version_file)
            with open(version_file) as f:
                version = f.read().strip()

        logger.debug("Found GOE version: %s" % version)
        return version

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def oracle(self):
        if not self._oracle:
            self._oracle = self._oracle_connect()
        return self._oracle

    @property
    def host(self):
        if not self._host:
            self._host = self._get_host()
        return self._host

    @property
    def os_release(self):
        if not self._os_release:
            self._os_release = self._get_os_release()
        return self._os_release

    @property
    def os_issue(self):
        if not self._os_issue:
            self._os_issue = self._get_os_issue()
        return self._os_issue

    @property
    def oracle_version(self):
        if not self._oracle_version:
            self._oracle_version = self._get_oracle_version()
        return self._oracle_version

    @property
    def hadoop_distro(self):
        if not self._hadoop_distro:
            self._hadoop_distro = self._get_hadoop_distro()
        return self._hadoop_distro

    @property
    def goe_hadoop_distro(self):
        if not self._goe_hadoop_distro:
            self._goe_hadoop_distro = self._get_goe_hadoop_distro()
        return self._goe_hadoop_distro

    @property
    def hadoop_version(self):
        if not self._hadoop_version:
            self._hadoop_version = self._get_hadoop_version()
        return self._hadoop_version

    @property
    def hadoop_db_type(self):
        if not self._hadoop_db_type:
            self._hadoop_db_type = self._get_hadoop_db_type()
        return self._hadoop_db_type

    @property
    def hadoop_db_version(self):
        if not self._hadoop_db_version:
            self._hadoop_db_version = self._get_hadoop_db_version(self.hadoop_db_type)
        return self._hadoop_db_version

    @property
    def goe_major_version(self):
        if not self._goe_major_version:
            self._goe_major_version = self._get_goe_major_version()
        return self._goe_major_version

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def report(self, environment_id=None):
        """Report system environment"""
        environment_id = "[%s]" % environment_id if environment_id else ""
        report_body = "***** SYSTEM ENVIRONMENT %s *****\n\n" % environment_id

        report_body += "\tHost:\n\t\t%s\n" % self.host
        report_body += "\tOS:\n\t\tRelease: %s\n\t\tIssue: %s\n" % (
            self.os_release,
            self.os_issue,
        )
        report_body += "\tORACLE:\n\t\tVersion: %s\n" % self.oracle_version
        distro_override = (
            " (environment override as: %s)" % self.goe_hadoop_distro
            if self.goe_hadoop_distro != self.hadoop_distro
            else ""
        )
        report_body += "\tHadoop:\n\t\tDistro: %s%s\n\t\tVersion: %s\n" % (
            self.hadoop_distro,
            distro_override,
            self.hadoop_version,
        )
        report_body += "\tHadoop Db:\n\t\tType: %s\n\t\tVersion: %s\n" % (
            self.hadoop_db_type,
            self.hadoop_db_version,
        )

        return report_body


class DevSystemEnvironment(SystemEnvironment):
    """DevSystemEnvironment: Represents "Development" environment, i.e. ~/dev/offload"""

    def __init__(self, offload_dev_root=None):
        """CONSTRUCTOR"""
        if not offload_dev_root:
            offload_dev_root = DEFAULT_DEV_ROOT
        self._offload_dev_root = os.path.abspath(os.path.expanduser(offload_dev_root))

        super(DevSystemEnvironment, self).__init__(self._offload_dev_root)

        self._hg_changeset = None
        self._hg_user = None
        self._hg_date = None
        self._hg_summary = None

        logger.debug(
            "DevSystemEnvironment() object successfully initialized with dev root: %s"
            % self._offload_dev_root
        )

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _parse_hg_tip(self):
        """Parse HG tip
        and return changeset, user, date, summary
        """
        changeset = ITEM_UNDISCOVERED
        user = ITEM_UNDISCOVERED
        date = ITEM_UNDISCOVERED
        summary = ITEM_UNDISCOVERED

        # First, change working directory to 'offload dev root'
        if not os.path.isdir(self._offload_dev_root):
            logger.warn("Dev root: %s does not seem to exist" % self._offload_dev_root)
            return (
                ITEM_UNDISCOVERED,
                ITEM_UNDISCOVERED,
                ITEM_UNDISCOVERED,
                ITEM_UNDISCOVERED,
            )

        os.chdir(self._offload_dev_root)
        if not self._linux.execute("hg tip 2>/dev/null"):
            logger.warn("Unable to access HG tip")
            return (
                ITEM_UNDISCOVERED,
                ITEM_UNDISCOVERED,
                ITEM_UNDISCOVERED,
                ITEM_UNDISCOVERED,
            )

        tip = self._linux.stdout.strip()

        # Extract relevant information from hg tip
        for line in tip.split("\n"):
            if "changeset:" in line:
                changeset = line.replace("changeset:", "").strip()
            elif "user:" in line:
                user = line.replace("user:", "").strip()
            elif "date:" in line:
                date = line.replace("date:", "").strip()
            elif "summary:" in line:
                summary = line.replace("summary:", "").strip()

        return changeset, user, date, summary

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def hg_changeset(self):
        if not self._hg_changeset:
            (
                self._hg_changeset,
                self._hg_user,
                self._hg_date,
                self._hg_summary,
            ) = self._parse_hg_tip()
        return self._hg_changeset

    @property
    def hg_user(self):
        if not self._hg_user:
            (
                self._hg_changeset,
                self._hg_user,
                self._hg_date,
                self._hg_summary,
            ) = self._parse_hg_tip()
        return self._hg_user

    @property
    def hg_date(self):
        if not self._hg_date:
            (
                self._hg_changeset,
                self._hg_user,
                self._hg_date,
                self._hg_summary,
            ) = self._parse_hg_tip()
        return self._hg_date

    @property
    def hg_summary(self):
        if not self._hg_summary:
            (
                self._hg_changeset,
                self._hg_user,
                self._hg_date,
                self._hg_summary,
            ) = self._parse_hg_tip()
        return self._hg_summary

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def report(self):
        """Report 'dev' system environment"""
        report_body = super(DevSystemEnvironment, self).report(self._offload_dev_root)
        report_body += (
            "\tGOE:\n\t\tMajor Version: %s\n\t\tChangeset: %s\n\t\tBy: %s\n\t\tAt: %s\n\t\tSummary: %s\n"
            % (
                self.goe_major_version,
                self.hg_changeset,
                self.hg_user,
                self.hg_date,
                self.hg_summary,
            )
        )

        return report_body


class ProdSystemEnvironment(SystemEnvironment):
    """ProdSystemEnvironment: Represents "Production" environment, i.e. ~/offload"""

    def __init__(self, offload_root=None):
        """CONSTRUCTOR"""
        if not offload_root:
            offload_root = DEFAULT_OFFLOAD_ROOT
        self._offload_root = os.path.abspath(os.path.expanduser(offload_root))

        super(ProdSystemEnvironment, self).__init__(self._offload_root)

        self._changeset_file = os.path.join(self._offload_root, OFFLOAD_CHANGESET_FILE)

        self._hg_changeset = None
        self._hg_date = None

        logger.debug(
            "ProdSystemEnvironment() object successfully initialized with root: %s"
            % self._offload_root
        )

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _get_hg_changeset(self):
        """Get 'hg changeset' from special Adrian's sql file"""
        changeset = ITEM_UNDISCOVERED

        if not os.path.isfile(self._changeset_file):
            logger.warn(
                "Unable to extract 'changeset' as 'changeset file': %s does not exist"
                % self._changeset_file
            )
        else:
            logger.debug("Extracing changeset from file: %s" % self._changeset_file)
            with open(self._changeset_file) as c_file:
                for line in c_file:
                    match = RE_GC_VERSION.search(line)
                    if match:
                        changeset = match.group(1)
                        break  # We found what we want - no need to scan further

        logger.debug("Extracted changeset: %s" % changeset)
        return changeset

    def _get_hg_date(self):
        """Get 'hg date' by looking at 'last updated' timestamp of special Adrian's sql file"""
        hg_date = ITEM_UNDISCOVERED

        if not os.path.isfile(self._changeset_file):
            logger.warn(
                "Unable to extract 'changeset date' as 'changeset file': %s does not exist"
                % self._changeset_file
            )
        else:
            logger.debug(
                "Extracing changeset date from file: %s" % self._changeset_file
            )
            hg_date_unix = os.path.getmtime(self._changeset_file)
            # Format to mimic what hg command returns
            hg_date = datetime.datetime.fromtimestamp(hg_date_unix).strftime(
                "%a %b %d %H:%M:%s %Y +0000"
            )

        logger.debug("Extracted changeset date: %s" % hg_date)
        return hg_date

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def hg_changeset(self):
        if not self._hg_changeset:
            self._hg_changeset = self._get_hg_changeset()
        return self._hg_changeset

    @property
    def hg_date(self):
        if not self._hg_date:
            self._hg_date = self._get_hg_date()
        return self._hg_date

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def report(self):
        """Report 'prod' system environment"""
        report_body = super(ProdSystemEnvironment, self).report(self._offload_root)
        report_body += (
            "\tGOE:\n\t\tMajor Version: %s\n\t\tChangeset: %s\n\t\tAt: %s\n"
            % (self.goe_major_version, self.hg_changeset, self.hg_date)
        )

        return report_body


###########################################################################
# STANDALONE FUNCTIONS
###########################################################################


def system_environment(env_type, offload_root=None):
    """Return specific SystemEnvironment object based on 'env_type'"""
    env_type = env_type.lower()
    if env_type not in ("dev", "prod"):
        raise SystemEnvironmentException(
            "Environment type must be one of: ['dev', 'prod']"
        )

    if "dev" == env_type:
        return DevSystemEnvironment(offload_root)
    else:
        return ProdSystemEnvironment(offload_root)


def goe_hadoop_distro():
    """Return hadoop distribution (with optional GOE overrides)"""
    return SystemEnvironment().goe_hadoop_distro


if __name__ == "__main__":
    import sys

    from goe.util.misc_functions import set_goelib_logging

    def usage(prog_name):
        print("%s: [dev|prod] [<root>] [debug level]" % prog_name)
        sys.exit(1)

    def main():
        prog_name = sys.argv[0]

        if len(sys.argv) < 2:
            usage(prog_name)

        env_type = sys.argv[1]
        env_root = sys.argv[2] if len(sys.argv) >= 3 else None

        log_level = sys.argv[3].upper() if len(sys.argv) > 3 else "CRITICAL"
        set_goelib_logging(log_level)

        environment = system_environment(env_type, env_root)
        print(environment.report())

    main()
