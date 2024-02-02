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

""" 'Linux' shell command (executiion) abstractions
"""

import getpass
import logging
import os
import subprocess


###############################################################################
# EXCEPTIONS
###############################################################################
class LinuxCmdException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################
SSH_DEFAULTS = {"BatchMode": "yes", "ConnectTimeout": "3", "LogLevel": "ERROR"}


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


class LinuxCmd(object):
    """LinuxCmd: Linux command abstraction"""

    def __init__(self):
        """CONSTRUCTOR"""

        self._cmd = None  # (last) Linux command
        self._environment = None  # ... environment (extra os vars to use)
        self._user = None  # ... user
        self._host = None  # ... host
        self._stdout = None  # ... stdout
        self._stderr = None  # ... stderr
        self._returncode = None  # ... return code
        self._success = None  # ... True|False - whether execution was successful

        logger.debug("LinuxCmd() object successfully initialized")

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _set_vars(self, vars):
        """Add/set environmane variables for the command to run

        If 'vars' are not specified, return os.environ
        """

        if vars:
            logger.debug("Adding environment variables: '%s'" % vars)
            environ_copy = os.environ.copy()
            environ_copy.update(vars)
            return environ_copy
        else:
            logger.debug("Not requested to add environment variables")
            return os.environ

    def _sshize_by_host_user(self, cmd, user, host, environment):
        """Execute command as ssh by user@host"""
        logger.debug(
            "Ssh'izing command: %s to be executed by user: %s on host: %s"
            % (cmd, user, host)
        )

        if user == getpass.getuser() and host in ["localhost", b"localhost"]:
            logger.debug(
                "'user' and 'host' are local. Assuming: 'current user', 'current host'"
            )
            return cmd

        ssh_options = " ".join(
            "-o %s=%s" % (k, v) for k, v in list(SSH_DEFAULTS.items())
        )
        logger.debug("Adding SSH options: %s" % ssh_options)

        ssh_environment = ""
        if environment:
            ssh_environment = (
                ";".join("%s=%s" % (k, v) for k, v in list(environment.items())) + ";"
            )
            logger.debug("Adding SSH environment: %s" % ssh_environment)

        ssh_cmd = "ssh -t %s %s@%s '%s %s'" % (
            ssh_options,
            user,
            host,
            ssh_environment,
            cmd,
        )
        logger.debug("Ssh cmd: %s" % ssh_cmd)

        return ssh_cmd

    def _determine_user_and_host(self, user, host):
        """Determine actual 'user' and 'host'
        by accepting 'defaults' when required

        Returns: ActualUser, ActualHost
        """

        # Neither 'user' nor 'host' is supplied - it's a 'local' command
        if not user and not host:
            logger.debug(
                "Both: 'user' and 'host' are empty. Assuming: 'current user', 'current host'"
            )
            return None, None

        # Either 'user' or 'host' is supplied - it's a 'remote' command
        if not user:
            user = getpass.getuser()
            logger.debug("Extracting current user: %s from the environment" % user)

        if not host:
            host = "localhost"
            logger.debug("Setting 'host' to 'localhost'")

        return user, host

    def _execute(self, cmd):
        """Run 'linux' command: 'cmd', based on:

        self._environment: Export environment variables: {var: value, ...} before execution
        self._user:        Execute command as a different user (or 'current' user if None)
        self._host:        Execute command on a remote host (or 'localhost' if None)

        Sets:
            self._success
            self._stdout
            self._stderr
            self._returncode

        Returns: True if successful execution, False otherwise
        """

        run_cmd = False
        success, stdout, stderr, returncode = None, None, None, None

        if self._host:
            logger.debug(
                "Preparing to run remote linux command: %s on %s@%s"
                % (cmd, self._user, self._host)
            )
        else:
            logger.debug("Preparing to run local linux command: %s" % cmd)

        # Decomposing UNIX pipes is too complex to process (especially for $ENVVARS etc)
        # Let's just run the entire thing as shell
        if "|" in cmd:
            logger.debug("Detected Linux 'pipe'. Prepending 'pipe fail' code")
            run_cmd = "set -e; set -o pipefail; %s" % cmd
        else:
            run_cmd = cmd

        # If 'host' is supplied, sshize the command by it
        if self._host:
            run_cmd = self._sshize_by_host_user(
                run_cmd, self._user, self._host, self._environment
            )

        # Ok, let's run the command
        logger.debug("Running UNIX command: %s" % run_cmd)
        try:
            process = subprocess.Popen(
                run_cmd,
                shell=True,
                env=self._set_vars(self._environment),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            stdout, stderr = process.communicate()
            stdout, stderr = stdout.strip(), stderr.strip()
            if not stderr:
                stderr = None
            returncode = process.returncode

            if 0 == returncode:
                success = True
            else:
                success = False
                if not stdout:
                    stdout = None
        except OSError as e:
            logger.warning(
                "Command: %s failed to execute. Exception: %s" % (run_cmd, e)
            )
            success = False
            returncode = e.errno
            stdout = None
            stderr = e

        logger.debug(
            "Command: %s execution. Success: %s Stdout: %s Stderr: %s"
            % (run_cmd, success, stdout, stderr)
        )

        self._success = success
        self._stdout = stdout
        self._stderr = stderr
        self._returncode = returncode

        return success

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    @property
    def cmd(self):
        return self._cmd

    @property
    def environment(self):
        return self._cmd

    @property
    def user(self):
        return self._user

    @property
    def host(self):
        return self._host

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr

    @property
    def returncode(self):
        return self._returncode

    @property
    def success(self):
        return self._success

    ###########################################################################
    # STATIC METHODS
    ###########################################################################

    @staticmethod
    def add_to_env(var, value):
        """Add (a.k.a. 'prepend') 'value' to environment variable
        (rather than 'replace')
        """

        if var in os.environ:
            return value + ":" + os.environ[var]
        else:
            return value

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def execute(self, cmd, user=None, environment=None, host=None):
        """Execute linux command: 'cmd' and store results

        'user': Execute command as different user
                or 'current' user if None
        'environment': Export environment variables: {var: value, ...}
                       before execution
        'host': Host to execute the command on
                or 'localhost' if not supplied

        Returns: True if successful status, False otherwise
        """
        if not cmd:
            raise LinuxCmdException("Command is NOT supplied in LinuxCmd.execute()")

        self._cmd = cmd
        self._environment = environment

        # Determine 'actual' user and host to run the command as/on
        self._user, self._host = self._determine_user_and_host(user, host)

        self._success = self._execute(cmd)

        return self._success
