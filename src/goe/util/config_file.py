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

""" config_file: GOE extensions for configparser
"""

import logging
import os
import os.path
import io

from configparser import SafeConfigParser

from goe.util.misc_functions import end_by


###############################################################################
# EXCEPTIONS
###############################################################################
class ConfigException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

DEFAULT_REMOTE_CONFIG_OSVAR = "REMOTE_OFFLOAD_CONF"

# (remote configuration) Section types
SECTION_TYPE_S3 = "s3"
SECTION_TYPE_HDFS = "hdfs"
SECTION_TYPE_GENERAL = "general"

###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class GOEConfig(SafeConfigParser):
    """configparser.SafeConfigParser() configuration file with GOE extensions"""

    def __init__(self, config_file, with_environment=False):
        """CONSTRUCTOR"""
        assert config_file

        self._config_file = config_file

        # Invoke parent's constructor
        super(GOEConfig, self).__init__(
            os.environ if with_environment else {}, inline_comment_prefixes="#"
        )

        self._config_file_exists = False
        if os.path.isfile(self._config_file):
            with open(self._config_file) as cfg_file:
                cfg_txt = os.path.expandvars(cfg_file.read())
            self.readfp(io.StringIO(cfg_txt))
            self._config_file_exists = True

        logger.debug(
            "Initialized GOEConfig() object with config: %s. Exists: %s"
            % (self._config_file, self._config_file_exists)
        )

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def config_file_exists(self):
        """Return True if config file exists, False otherwise"""
        return self._config_file_exists

    @property
    def config_file(self):
        """Return config file name"""
        return self._config_file

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def extract_options(self, section, required_options, error_if_not_set=False):
        """Extract and validate required options"""
        assert section and required_options

        options = {}

        for option in required_options:
            option_value = self.get(section, option)
            if not option_value and error_if_not_set:
                raise ConfigException(
                    "Please, define %s.%s configuration parameter" % (section, option)
                )
            else:
                options[option] = option_value

        return options

    def get(self, section, option, missing_replacement=None, **kwargs):
        """SafeConfigParser.get() override:

        If section or option do NOT exist,
            return 'missing_replacement' rather than throw exceptions
        Otherwise, behave exactly like SafeConfigParser.get()
        """

        if not self.has_section(section):
            logger.debug(
                "Config Section: %s does NOT exist. get() returns 'replacement': %s"
                % (section, missing_replacement)
            )
            return missing_replacement

        if not self.has_option(section, option):
            logger.debug(
                "Config Option: %s.%s does NOT exist. get() returns 'replacement': %s"
                % (section, option, missing_replacement)
            )
            return missing_replacement

        val = super(GOEConfig, self).get(section, option, **kwargs)
        logger.debug("Config Option: %s.%s exists. Return: %s" % (section, option, val))
        return val

    def getint(self, section, option, missing_replacement=None):
        """SafeConfigParser.getint() override:

        If section or option do NOT exist,
            return 'missing_replacement' rather than throw exceptions
        Otherwise, behave exactly like SafeConfigParser.getint()
        """

        if not self.has_section(section):
            logger.debug(
                "Config Section: %s does NOT exist. getint() returns 'replacement': %s"
                % (section, missing_replacement)
            )
            return missing_replacement

        if not self.has_option(section, option):
            logger.debug(
                "Config Option: %s.%s does NOT exist. getint() returns 'replacement': %s"
                % (section, option, missing_replacement)
            )
            return missing_replacement

        val = super(GOEConfig, self).get(section, option)
        if not val.isdigit():
            raise ConfigException(
                "Config Option: %s.%s is not a number" % (section, missing_replacement)
            )

        logger.debug("Config Option: %s.%s exists. Return: %s" % (section, option, val))
        return int(val)

    def set(self, section, option, value):
        """SaveConfigParser.set() override:

        If section does not exists, create it
        Otherwise, behave exactly like SafeConfigParser.set()

        Note, even if config file does not exist, it is still possible
        to set values over 'empty' configuration
        """

        if not self.has_section(section):
            logger.debug("Config Section: %s does NOT exist. Adding" % section)
            self.add_section(section)

        logger.debug("Config Set %s.%s to: %s" % (section, option, value))
        super(GOEConfig, self).set(section, option, value)


class GOERemoteConfig(GOEConfig):
    """'Remote (a.k.a. backup) configuration' file

    Usually, pointed out by $REMOTE_OFFLOAD_CONF and located at: ~/offload/conf/remote-offload.conf
    """

    ###########################################################################
    # CLASS CONSTANTS
    ###########################################################################

    def __init__(self, config_file=None):
        """CONSTRUCTOR"""

        if not config_file:
            config_file = GOERemoteConfig.default_config()

        # Invoke parent's constructor
        super(GOERemoteConfig, self).__init__(config_file)

        logger.debug(
            "Initialized GOERemoteConfig() object with config: %s. Exists: %s"
            % (self.config_file, self.config_file_exists)
        )

    ###########################################################################
    # STATIC METHODS
    ###########################################################################

    @classmethod
    def default_config(cls):
        """Returns 'default' config file"""
        if DEFAULT_REMOTE_CONFIG_OSVAR not in os.environ:
            raise ConfigException(
                "%s environment variable is NOT set" % DEFAULT_REMOTE_CONFIG_OSVAR
            )
        return os.environ[DEFAULT_REMOTE_CONFIG_OSVAR]

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def section_type(self, section):
        """Determine section type: s3, hdfs etc"""
        assert section

        if section not in self.sections():
            raise ConfigException("Section: %s does NOT exist" % section)

        s_type = SECTION_TYPE_GENERAL

        if all([self.get(section, _) for _ in ("s3_bucket", "s3_prefix")]):
            s_type = SECTION_TYPE_S3
        elif all(
            [self.get(section, _) for _ in ("hdfs_root", "hdfs_url", "webhdfs_url")]
        ):
            s_type = SECTION_TYPE_HDFS

        return s_type

    def base_url(self, section):
        """Return base url for a 'section' based on its type"""
        assert section

        s_type = self.section_type(section)
        url = None

        if SECTION_TYPE_S3 == s_type:
            bucket = self.get(section, "s3_bucket")
            prefix = self.get(section, "s3_prefix")

            url = "s3://%s%s" % (end_by(bucket, "/"), end_by(prefix.lstrip("/"), "/"))
        elif SECTION_TYPE_HDFS == s_type:
            host = self.get(section, "hdfs_url")
            root = self.get(section, "hdfs_root")

            url = "%s%s" % (end_by(host, "/"), end_by(root.lstrip("/"), "/"))
        else:
            raise ConfigException(
                "Base URL for section type: %s is NOT supported" % s_type
            )

        return url

    def base_root(self, section):
        """Return section filesystem "root" (i.e. /tmp) without hosts, ports, s3/hdfs prefixes etc"""
        assert section

        s_type = self.section_type(section)

        if SECTION_TYPE_S3 == s_type:
            return end_by(self.get(section, "s3_prefix", ""), "/")
        elif SECTION_TYPE_HDFS == s_type:
            return end_by(self.get(section, "hdfs_root", ""), "/")
        else:
            raise ConfigException(
                "Base ROOT for section type: %s is NOT supported" % s_type
            )

    def db_url(self, section, db_name):
        """Return 'db url', i.e.: sh.db/ for specific section"""
        assert section and db_name

        return "%s%s/" % (db_name.strip("/"), self.get(section, "db_path_suffix", ""))

    def table_url(self, section, db_table):
        """Return 'db table url', i.e.: sh.db/sales for specific section"""
        assert section and db_table

        if 1 != db_table.count("."):
            raise ConfigException("Invalid db.table specification: %s" % db_table)

        db, table = (_.strip() for _ in db_table.split("."))
        return "%s%s/" % (self.db_url(section, db), table.strip("/"))

    def file_url(self, section, db_table, key_name):
        """Return 'file url' for specific section, db_table and key

        i.e. hdfs, sh.times, offload_bucket_id=0/324b311ba26ded23.0.parq ->
        hdfs://localhost:8020/user/goe/offload/sh.db/times/offload_bucket_id=0/324b311ba26ded23.0.parq
        """
        return "%s%s%s" % (
            self.base_url(section),
            self.table_url(section, db_table),
            key_name,
        )


if __name__ == "__main__":
    import sys

    from goe.util.misc_functions import set_goelib_logging

    def usage(prog_name):
        print("usage: %s section operation [parameters] [log_level]" % prog_name)
        sys.exit(1)

    def main():
        prog_name = sys.argv[0]

        if len(sys.argv) < 3:
            usage(prog_name)

        log_level = sys.argv[-1:][0].upper()
        if log_level not in ("DEBUG", "INFO", "WARNING", "CRITICAL", "ERROR"):
            log_level = "CRITICAL"
        set_goelib_logging(log_level)

        section, operation = sys.argv[1:3]
        args = [_ for _ in sys.argv[3:] if _.upper() != log_level]

        set_goelib_logging(log_level)

        cfg = GOERemoteConfig()
        print(getattr(cfg, operation)(section, *args))

    main()
