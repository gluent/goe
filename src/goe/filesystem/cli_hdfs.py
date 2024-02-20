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

""" CliHdfs: Command line hdfs implementation of GOEDfs
"""

from datetime import datetime
from getpass import getuser
import logging
from os.path import basename, exists as file_exists
import re
import subprocess
from subprocess import PIPE, STDOUT

from google.api_core import retry

from goe.config.orchestration_defaults import backend_distribution_default
from goe.filesystem.goe_dfs import (
    GOEDfs,
    GOEDfsDeleteNotComplete,
    GOEDfsException,
    gen_fs_uri,
    DFS_RETRY_TIMEOUT,
    DFS_TYPE_DIRECTORY,
    DFS_TYPE_FILE,
    GOE_DFS_SSH,
    OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER,
    OFFLOAD_FS_SCHEME_INHERIT,
)
from goe.offload.offload_constants import BACKEND_DISTRO_MAPR
from goe.util.misc_functions import is_number


###############################################################################
# EXCEPTIONS
###############################################################################

###############################################################################
# CONSTANTS
###############################################################################

###############################################################################
# LOGGING
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# CliHdfs
###############################################################################


class CliHdfs(GOEDfs):
    """A GOE wrapper over ssh calls to 'hdfs dfs'
    dry_run: Do not make changes. stat() continues to function though
    do_not_connect: Do not even connect, used for unit testing.
    """

    def __init__(
        self,
        hdfs_host,
        ssh_user,
        tmp_dir="/tmp",
        dry_run=False,
        messages=None,
        do_not_connect=False,
        db_path_suffix=None,
        hdfs_data=None,
    ):
        logger.info("Client setup: (%s, %s, %s)" % (hdfs_host, ssh_user, tmp_dir))

        super(CliHdfs, self).__init__(
            messages, dry_run=dry_run, do_not_connect=do_not_connect
        )

        self._db_path_suffix = db_path_suffix
        self._hdfs_data = hdfs_data
        self._hdfs_host = hdfs_host
        self._ssh_user = ssh_user
        self._tmp_dir = tmp_dir
        self.dfs_mechanism = GOE_DFS_SSH
        self.backend_dfs = "HDFS"

    def __str__(self):
        return "%s@%s" % (self._ssh_user, self._hdfs_host)

    def _ssh(self, ssh_user, host):
        if ssh_user == getuser() and host == "localhost":
            return []
        return ["ssh", "-tq", ssh_user + "@" + host]

    def _can_short_circuit_ssh(self, ssh_user, host):
        return self._ssh(ssh_user, host) == []

    def _normalize_hdfs_path(self, ssh_user, host, dfs_path):
        return (
            dfs_path
            if self._can_short_circuit_ssh(ssh_user, host)
            else "'" + dfs_path.strip("'") + "'"
        )

    def _scp_from(self, ssh_user, host, from_path, to_path):
        if ssh_user == getuser() and host == "localhost":
            return ["scp", from_path, to_path]
        return ["scp", "%s@%s:%s" % (ssh_user, host, from_path), to_path]

    def _scp_to(self, ssh_user, host, from_path, to_path):
        if ssh_user == getuser() and host == "localhost":
            return ["scp", from_path, to_path]
        return ["scp", from_path, "%s@%s:%s" % (ssh_user, host, to_path)]

    def _temp_file(self, base_file="goe_temp"):
        return "%s/%s.%s" % (
            self._tmp_dir,
            base_file,
            datetime.now().strftime("%y%j%H%M%S%f"),
        )

    def _check_returncode(self, returncode, output):
        self.debug("Return code: %s" % returncode)
        if returncode:
            self.log("Output: %s" % output)
            raise GOEDfsException(
                'Required shell command failed with return code "%s"\n%s'
                % (returncode, b"\n".join(output))
            )

    def _run_cmd(self, cmd, force_run=False):
        """Return exit code and stdout for a given cmd.
        stdout is bytes and is returned as such, it is up to the calling code to decide if that's correct or not.
        """
        logger.info("cmd: " + " ".join(cmd))

        if self._dry_run and (not force_run or self._do_not_connect):
            return 0, []

        with subprocess.Popen(cmd, stdout=PIPE, stderr=STDOUT) as proc:
            output = [_ for _ in proc.stdout if not self._ignorable_stderr(_)]
            # TODO nj@2017-08-14 Python docs say wait() with stdout=PIPE is a bad idea
            cmd_returncode = proc.wait()
            logger.debug("output: %s" % b"\n".join(output))
            logger.debug("returncode: %s" % cmd_returncode)
        return cmd_returncode, output

    def _valid_ls_line(self, ls_line):
        """Used to validate lines from "hdfs dfs -ls", known to ignore:
          "Found x items" line
          Logger output, e.g. "17/06/02 02:34:10 DEBUG ipc.ProtobufRpcEngine: Call: getListing took 3ms"
        We can trust ls output to be textual and convert to string from bytes if required
        """
        use_line = ls_line.decode() if isinstance(ls_line, bytes) else ls_line
        return (
            True
            if use_line and use_line.find("/") >= 0 and use_line[0] in ("-", "d")
            else False
        )

    def _ignorable_stderr(self, stderr_line):
        """Used to identify ignorable lines in stderr (which ends up in stdout anyway):
        "Pseudo-terminal will not be allocated..."
        """
        return (
            True
            if stderr_line and b"Pseudo-terminal will not be allocated" in stderr_line
            else False
        )

    @staticmethod
    def dfs_cmd(as_string=False):
        """Return distribution appropriate 'hdfs dfs' command"""
        dfs = ["hdfs", "dfs"]
        if backend_distribution_default() == BACKEND_DISTRO_MAPR:
            dfs = ["hadoop", "fs"]

        if as_string:
            dfs = " ".join(dfs)

        return dfs

    @property
    def client(self):
        logger.warn("Attempting to access 'client' in ssh based CliHdfs")
        return None

    def mkdir(self, dfs_path):
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("mkdir(%s)" % dfs_path)
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        cmd = (
            self._ssh(self._ssh_user, self._hdfs_host)
            + CliHdfs.dfs_cmd()
            + ["-mkdir", "-p", dfs_path]
        )
        returncode, output = self._run_cmd(cmd)
        self._check_returncode(returncode, output)

    @retry.Retry(
        predicate=retry.if_exception_type(GOEDfsDeleteNotComplete),
        deadline=DFS_RETRY_TIMEOUT,
    )
    def delete(self, dfs_path, recursive=False):
        """Delete a file or directory and contents."""
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("delete(%s, %s)" % (dfs_path, recursive))
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        if recursive:
            cmd = (
                self._ssh(self._ssh_user, self._hdfs_host)
                + CliHdfs.dfs_cmd()
                + ["-rm", "-f", "-r", dfs_path]
            )
        else:
            cmd = (
                self._ssh(self._ssh_user, self._hdfs_host)
                + CliHdfs.dfs_cmd()
                + ["-rm", "-f", dfs_path]
            )
        returncode, output = self._run_cmd(cmd)
        self._check_returncode(returncode, output)

        # If cloud DFS then check the files are no longer visible
        if not self._dry_run:
            scheme, _, _ = self._uri_component_split(dfs_path)
            self._post_cloud_delete_wait(scheme)
            if scheme in OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER and self.stat(dfs_path):
                self.debug("%s delete incomplete, retrying" % scheme)
                raise GOEDfsDeleteNotComplete
        return True

    def rename(self, hdfs_src_path, hdfs_dst_path):
        assert hdfs_src_path and hdfs_dst_path
        logger.info("rename(%s, %s)" % (hdfs_src_path, hdfs_dst_path))
        if not self._dry_run:
            cmd = (
                self._ssh(self._ssh_user, self._hdfs_host)
                + CliHdfs.dfs_cmd()
                + ["-mv", hdfs_src_path, hdfs_dst_path]
            )
            returncode, output = self._run_cmd(cmd)
        else:
            returncode, output = 0, []
        self._check_returncode(returncode, output)

    def rmdir(self, dfs_path, recursive=False):
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("rmdir(%s, %s)" % (dfs_path, recursive))
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        if recursive:
            return self.delete(dfs_path, recursive)
        else:
            cmd = (
                self._ssh(self._ssh_user, self._hdfs_host)
                + CliHdfs.dfs_cmd()
                + ["-rmdir", dfs_path]
            )
            returncode, output = self._run_cmd(cmd)
            self._check_returncode(returncode, output)
            return True

    def chmod(self, dfs_path, mode):
        # mode in format rwx or g+w
        assert (
            dfs_path
            and mode
            and (re.match(r"^\d+$", mode) or re.match(r"^[ugo]+[+\-][rwx]+$", mode))
        )
        assert isinstance(dfs_path, str)
        logger.info("chmod(%s, %s)" % (dfs_path, mode))
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        cmd = (
            self._ssh(self._ssh_user, self._hdfs_host)
            + CliHdfs.dfs_cmd()
            + ["-chmod", mode, dfs_path]
        )
        returncode, output = self._run_cmd(cmd)
        self._check_returncode(returncode, output)

    def chgrp(self, dfs_path, group):
        assert dfs_path and group
        assert isinstance(dfs_path, str)
        logger.info("chgrp(%s, %s)" % (dfs_path, group))
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        cmd = (
            self._ssh(self._ssh_user, self._hdfs_host)
            + CliHdfs.dfs_cmd()
            + ["-chgrp", group, dfs_path]
        )
        returncode, output = self._run_cmd(cmd)
        self._check_returncode(returncode, output)

    # NOTE: this approach uses string processing that relies on '-ls' output being formatted such that
    #       the absolute path of the entry appears at the end of each output line, and a forward slash
    #       will never appear anywhere else in a given line.
    def list_dir(self, dfs_path):
        """Returns a list of directory contents as strings (not bytes)"""
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("list_dir(%s)" % dfs_path)
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        cmd = (
            self._ssh(self._ssh_user, self._hdfs_host)
            + CliHdfs.dfs_cmd()
            + ["-ls", dfs_path]
        )
        returncode, output = self._run_cmd(cmd, force_run=True)
        self._check_returncode(returncode, output)
        return [
            _[_.find(b"/") :].strip().decode() for _ in output if self._valid_ls_line(_)
        ]

    def stat(self, dfs_path):
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("stat(%s)" % dfs_path)
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        cmd = (
            self._ssh(self._ssh_user, self._hdfs_host)
            + CliHdfs.dfs_cmd()
            + ["-ls", "-d", dfs_path]
        )
        returncode, output = self._run_cmd(cmd, force_run=True)
        if returncode == 0:
            tidy_output = [_.strip().decode() for _ in output if self._valid_ls_line(_)]
            if not tidy_output:
                # we ran a successful command but have no files to report
                self.debug("Stat command: %s" % cmd)
                self.debug("Stat raw text: %s" % output)
                return None
            tokens = tidy_output[0].split()
            if tokens:
                # only deriving file permissions and file type because it is unknown how reliable 'ls' format
                # is across versions/provider
                # further attributes can be derived in the future as needed
                # converting rwx style permissions to octal string to be consistent with webhdfs
                file_type = (
                    DFS_TYPE_DIRECTORY
                    if tokens[0] and tokens[0][0] == "d"
                    else DFS_TYPE_FILE
                )
                file_length = (
                    int(tokens[4]) if tokens[4] and is_number(tokens[4]) else None
                )
                return {
                    "permission": self.convert_rwx_perms_to_oct_str(tokens[0]),
                    "type": file_type,
                    "length": file_length,
                }
        else:
            # Log the message, in many cases this will be a positive error, such as confirming a file doesn't
            # exist. However in some cases it could contain useful debug information, hence logging as debug
            self.debug("Stat command: %s" % cmd)
            self.debug("Stat raw text: %s" % output)

    def container_exists(self, scheme, container):
        assert scheme and container
        if scheme in OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER:
            return bool(self.stat(self.gen_uri(scheme, container, "")))
        return False

    def copy_from_local(self, local_path, dfs_path, overwrite=False):
        """Two phase SCP to Hadoop node and then copy from local there."""
        assert local_path
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("copy_from_local(%s, %s)" % (local_path, dfs_path))
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        # Works in 2 steps, first scp file to Hadoop node and then invoke copyFromLocal
        base_file = basename(local_path)
        hadoop_temp_file = self._temp_file(base_file)

        scp_cmd = self._scp_to(
            self._ssh_user, self._hdfs_host, local_path, hadoop_temp_file
        )
        self.debug("SCP command: %s" % " ".join(scp_cmd))
        returncode, output = self._run_cmd(scp_cmd)
        self._check_returncode(returncode, output)

        try:
            cfl_opts = (["-f"] if overwrite else []) + [hadoop_temp_file, dfs_path]
            cfl_cmd = (
                self._ssh(self._ssh_user, self._hdfs_host)
                + CliHdfs.dfs_cmd()
                + ["-copyFromLocal"]
                + cfl_opts
            )
            self.debug("SSH command: %s" % " ".join(cfl_cmd))
            returncode, output = self._run_cmd(cfl_cmd)
            self._check_returncode(returncode, output)
        finally:
            # Even if copyFromLocal fails we still want to rm the temp file - no data left lying around
            rm_cmd = self._ssh(self._ssh_user, self._hdfs_host) + [
                "rm",
                "-f",
                hadoop_temp_file,
            ]
            returncode, output = self._run_cmd(rm_cmd)
            self._check_returncode(returncode, output)

    def copy_to_local(self, dfs_path, local_path, overwrite=False):
        """Two phase copy to local on Hadoop node and then SCP to current node."""
        if self._can_short_circuit_ssh(self._ssh_user, self._hdfs_host):
            # Same user/host so no need for second phase
            hadoop_temp_file = local_path
        else:
            base_file = basename(local_path)
            hadoop_temp_file = self._temp_file(base_file)

        if file_exists(local_path) and not overwrite:
            raise GOEDfsException(
                "Cannot copy file over existing file: %s" % local_path
            )

        try:
            # Phase 1: copy to local storage on Hadoop node
            cfl_opts = [dfs_path, hadoop_temp_file]
            cfl_cmd = (
                self._ssh(self._ssh_user, self._hdfs_host)
                + CliHdfs.dfs_cmd()
                + ["-copyToLocal"]
                + cfl_opts
            )
            self.debug("SSH command: %s" % " ".join(cfl_cmd))
            returncode, output = self._run_cmd(cfl_cmd)
            self._check_returncode(returncode, output)

            # Phase 2: copy from Hadoop node to local node
            if not self._can_short_circuit_ssh(self._ssh_user, self._hdfs_host):
                scp_cmd = self._scp_from(
                    self._ssh_user, self._hdfs_host, hadoop_temp_file, local_path
                )
                self.debug("SCP command: %s" % " ".join(scp_cmd))
                returncode, output = self._run_cmd(scp_cmd)
                self._check_returncode(returncode, output)
        finally:
            if not self._can_short_circuit_ssh(self._ssh_user, self._hdfs_host):
                # Even if copyToLocal fails we still want to rm the temp file - no data left lying around
                rm_cmd = self._ssh(self._ssh_user, self._hdfs_host) + [
                    "rm",
                    "-f",
                    hadoop_temp_file,
                ]
                returncode, output = self._run_cmd(rm_cmd)
                self._check_returncode(returncode, output)

    def gen_uri(
        self,
        scheme,
        container,
        path_prefix,
        backend_db=None,
        table_name=None,
        container_override=None,
    ):
        if not scheme or scheme == OFFLOAD_FS_SCHEME_INHERIT:
            # Fall back to hdfs_data for backward compatibility
            uri = gen_fs_uri(
                self._hdfs_data,
                self._db_path_suffix,
                backend_db=backend_db,
                table_name=table_name,
            )
        else:
            prefix = self._hdfs_data if path_prefix is None else path_prefix
            use_container = (
                container if scheme in OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER else None
            )
            uri = gen_fs_uri(
                prefix,
                self._db_path_suffix,
                scheme=scheme,
                container=container_override or use_container,
                backend_db=backend_db,
                table_name=table_name,
            )
        return uri

    def read(self, dfs_path, as_str=False):
        """Return contents of a file as a bytes object because we don't know anything about the contents"""
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("read(%s)" % dfs_path)
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        cmd = (
            self._ssh(self._ssh_user, self._hdfs_host)
            + CliHdfs.dfs_cmd()
            + ["-cat", dfs_path]
        )
        returncode, output = self._run_cmd(cmd, force_run=True)
        self._check_returncode(returncode, output)
        content = b"".join(output) if output else b""
        if as_str:
            content = content.decode()
        return content

    def write(self, dfs_path, data, overwrite=False):
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("write(%s, data, %s)" % (dfs_path, overwrite))
        dfs_path = self._normalize_hdfs_path(self._ssh_user, self._hdfs_host, dfs_path)

        local_temp_file = self._temp_file()
        write_mode = "wb" if isinstance(data, bytes) else "w"
        with open(local_temp_file, write_mode) as fh:
            fh.write(data)
        try:
            self.copy_from_local(local_temp_file, dfs_path, overwrite=overwrite)
        finally:
            # even if copy_from_local fails we still want to rm the temp file - no data left lying around
            rm_cmd = ["rm", "-f", local_temp_file]
            returncode, output = self._run_cmd(rm_cmd)
            self._check_returncode(returncode, output)
