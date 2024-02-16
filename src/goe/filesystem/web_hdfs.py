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

""" WebHdfs: WebHDFS/HTTPFS implementation of GOEDfs
"""

import logging
import os
from os.path import exists as file_exists
import re
from requests import Session
from requests_kerberos import HTTPKerberosAuth, OPTIONAL
from requests_kerberos.exceptions import MutualAuthenticationError
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning

from google.api_core import retry
from hdfs.ext.kerberos import KerberosClient
from hdfs.util import HdfsError

from goe.filesystem.goe_dfs import (
    GOEDfs,
    GOEDfsDeleteNotComplete,
    GOEDfsException,
    gen_fs_uri,
    DFS_RETRY_TIMEOUT,
    GOE_DFS_WEBHDFS,
    OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER,
    OFFLOAD_FS_SCHEME_INHERIT,
)


###############################################################################
# EXCEPTIONS
###############################################################################

###############################################################################
# CONSTANTS
###############################################################################

###############################################################################
# GLOBAL FUNCTIONS
###############################################################################


def get_hdfs_session(verify=None, user=None):
    """Construct 'HDFS session' object"""
    disable_warnings(InsecureRequestWarning)
    session = Session()
    session.verify = verify

    if user:
        if not session.params:
            session.params = {}
        session.params["user.name"] = user

    return session


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


###############################################################################
# GOEWebHdfsClient
###############################################################################


class GOEWebHdfsClient(KerberosClient):
    """A new client subclass for handling HTTPS connections as well as Kerberos & insecure.
    We have removed "cert" parameter. See link below for "cert" usage.
    See http://hdfscli.readthedocs.org/en/latest/advanced.html#custom-client-support for idea

    :param url: URL to namenode.
    :param verify: True: Check the host's cert against known certs, False: Don't check the cert. 'Path': Check the cert against 'Path'.
    :param user: User for insecure connection
    :param \\*\\*kwargs: Keyword arguments passed to the default `KerberosClient` constructor. Use this for mutual_auth & max_concurrency
      max_concurrency defaults to 1 and that appears a suitable default. KerberosClient() queues/retries if required.
    """

    def __init__(self, url, verify=None, user=None, **kwargs):
        super(GOEWebHdfsClient, self).__init__(
            url, session=get_hdfs_session(verify, user), **kwargs
        )

    def __del__(self):
        if hasattr(self, "_session") and self._session:
            try:
                self._session.close()
            except:
                pass


###############################################################################
# GOEWebHdfsClient
###############################################################################


class WebHdfs(GOEDfs):
    """A GOE wrapper over HDFSCli WebHDFS library
    http://hdfscli.readthedocs.org/en/latest/api.html
    verify_ssl_cert: Any of True, False, str(path to cert/bundle) for verify_ssl_cert to enable SSL. None to disable SSL
    dry_run: Do not make changes. stat() continues to function though
    do_not_connect: Do not even connect, used for unit testing.
    """

    def __init__(
        self,
        hdfs_namenode,
        webhdfs_port,
        hdfs_user=None,
        use_kerberos=False,
        verify_ssl_cert=None,
        dry_run=False,
        messages=None,
        do_not_connect=False,
        db_path_suffix=None,
        hdfs_data=None,
        **kwargs,
    ):
        assert hdfs_namenode
        assert webhdfs_port
        assert hdfs_user or use_kerberos or verify_ssl_cert is not None

        logger.info(
            "Client setup: (%s, %s, %s, %s, %s)"
            % (hdfs_namenode, webhdfs_port, hdfs_user, use_kerberos, verify_ssl_cert)
        )

        super(WebHdfs, self).__init__(
            messages, dry_run=dry_run, do_not_connect=do_not_connect
        )

        self._db_path_suffix = db_path_suffix
        self._hdfs_data = hdfs_data
        self._hdfs = self._hdfs_client(
            hdfs_namenode,
            webhdfs_port,
            hdfs_user,
            use_kerberos,
            verify_ssl_cert,
            **kwargs,
        )
        self.dfs_mechanism = GOE_DFS_WEBHDFS
        self.backend_dfs = "HDFS"

    def _hdfs_client(
        self,
        hdfs_namenode,
        webhdfs_port,
        hdfs_user,
        use_kerberos,
        verify_ssl_cert=None,
        **kwargs,
    ):
        """Returns a GOEWebHdfsClient client after first finding an active name node in the csv list of namenodes
        Works on the assumption that kerberos/ssl requirements for WebHdfs also apply to the REST API used to
        identify namenode status.
        """

        def http_protocol(verify_ssl_cert):
            return "https" if verify_ssl_cert is not None else "http"

        if self._do_not_connect:
            return None

        if use_kerberos:
            auth_user = None
            get_auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
        else:
            auth_user = hdfs_user
            get_auth = None

        namenodes = [_.strip() for _ in hdfs_namenode.split(",")]
        if len(namenodes) > 1:
            self.debug(
                "Multiple WebHDFS namenodes detected, determining active namenode to use"
            )
            session = get_hdfs_session(verify=verify_ssl_cert, user=auth_user)
            for namenode in namenodes:
                self.debug("Checking namenode: {name_node}".format(name_node=namenode))
                namenode_url = "{protocol}://{name_node}:{port}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus".format(
                    protocol=http_protocol(verify_ssl_cert),
                    name_node=namenode,
                    port=webhdfs_port,
                )
                try:
                    response = session.get(namenode_url, auth=get_auth)
                    if response.status_code == 200:
                        try:
                            beans = response.json()["beans"]
                            if beans:
                                if beans[0]["State"].lower() == "active":
                                    self.debug(
                                        "Namenode {name_node}, status: active".format(
                                            name_node=namenode
                                        )
                                    )
                                    self._url = "%s://%s:%s" % (
                                        http_protocol(verify_ssl_cert),
                                        namenode,
                                        webhdfs_port,
                                    )
                                    break
                            else:
                                self.notice(
                                    "WebHDFS active namenode check failed for {name_node} with empty [beans]".format(
                                        name_node=namenode
                                    )
                                )
                                self.debug(
                                    "Namenode {name_node} response: {json}".format(
                                        name_node=namenode, json=response.json()
                                    )
                                )
                        except KeyError:
                            self.notice(
                                "WebHDFS active namenode check failed for {name_node} with unknown response".format(
                                    name_node=namenode
                                )
                            )
                            self.debug(
                                "Namenode {name_node} status unknown: KeyError: {json}".format(
                                    name_node=namenode, json=response.json()
                                )
                            )
                    else:
                        self.debug(
                            "Namenode {name_node} status code: {code}".format(
                                name_node=namenode, code=response.status_code
                            )
                        )
                except (ConnectionError, MutualAuthenticationError) as exc:
                    self.notice(
                        "WebHDFS active namenode check failed for {name_node} with connection error".format(
                            name_node=namenode
                        )
                    )
                    self.debug(
                        "Namenode {name_node} connection error: {message}".format(
                            name_node=namenode, message=str(exc)
                        )
                    )
            if getattr(self, "_url", None) is None:
                self.warning(
                    "Multiple WebHDFS namenodes detected, unable to determine active node, defaulting to {name_node}".format(
                        name_node=namenodes[0]
                    )
                )
                self._url = "%s://%s:%s" % (
                    http_protocol(verify_ssl_cert),
                    namenodes[0],
                    webhdfs_port,
                )
        else:
            self._url = "%s://%s:%s" % (
                http_protocol(verify_ssl_cert),
                hdfs_namenode,
                webhdfs_port,
            )

        self.debug("Client url: %s" % self._url)
        self.debug("Verify SSL: %s" % verify_ssl_cert)
        return GOEWebHdfsClient(
            self._url, verify=verify_ssl_cert, user=auth_user, **kwargs
        )

    def __str__(self):
        return self._url

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    @property
    def client(self):
        return self._hdfs

    def mkdir(self, dfs_path):
        """No return code because WebHDFS has no meaningful return code"""
        logger.info("mkdir(%s)" % dfs_path)
        # not implemented mode parameter as prefer explicit chmod call
        if not self._dry_run:
            self._hdfs.makedirs(dfs_path)

    @retry.Retry(
        predicate=retry.if_exception_type(GOEDfsDeleteNotComplete),
        deadline=DFS_RETRY_TIMEOUT,
    )
    def delete(self, dfs_path, recursive=False):
        """Delete a file or directory and contents."""
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("delete(%s, %s)" % (dfs_path, recursive))
        if not self._dry_run:
            res = self._hdfs.delete(dfs_path, recursive)

            # If cloud DFS then check the files are no longer visible
            scheme, _, _ = self._uri_component_split(dfs_path)
            self._post_cloud_delete_wait(scheme)
            if scheme in OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER and self.stat(dfs_path):
                self.debug("%s delete incomplete, retrying" % scheme)
                raise GOEDfsDeleteNotComplete

            # True and False are both success therefore not reacting to res == False
            logger.debug("delete() returning: %s" % res)
            return res

    def rename(self, hdfs_src_path, hdfs_dst_path):
        logger.info("rename(%s, %s)" % (hdfs_src_path, hdfs_dst_path))
        assert hdfs_src_path and hdfs_dst_path
        if not self._dry_run:
            self._hdfs.rename(hdfs_src_path, hdfs_dst_path)

    def rmdir(self, dfs_path, recursive=False):
        logger.info("rmdir(%s, %s)" % (dfs_path, recursive))
        assert dfs_path
        assert isinstance(dfs_path, str)
        if not self._dry_run:
            return self.delete(dfs_path, recursive)

    def regex_mode_delta(self, mode):
        # return tuple of 3 parts of e.g. 'g+rw' : ('g', '+', 'rw')
        m = re.match(r"([ugo]+)([+\-])([rwx]+)", mode)
        if not m or len(m.groups()) != 3:
            raise GOEDfsException('Invalid chmod delta mode "%s"', mode)
        return m.groups()

    def octstr_from_human_perm_delta(self, mode):
        # only supports rwx keys. That should be fine for GOE usage
        perm_values = {"r": 4, "w": 2, "x": 1}
        ugo_values = {"u": 0, "g": 0, "o": 0}
        mode_groups = self.regex_mode_delta(mode)
        int_perm = sum([perm_values[perm] for perm in set(list(mode_groups[2]))])
        if mode_groups[1] == "-":
            ugo_values = {"u": 7, "g": 7, "o": 7}
            int_perm = 7 - int_perm
        for ugo in list(mode_groups[0]):
            ugo_values[ugo] = int_perm
        return "%s%s%s" % (ugo_values["u"], ugo_values["g"], ugo_values["o"])

    def apply_mode_delta(self, octstr_mode, octstr_delta, mode_op):
        assert mode_op in ["+", "-"]
        # e.g. apply_mode_delta('711', '020', '+') == '731'
        if mode_op == "+":
            new_mode = oct(int(octstr_mode, 8) | int(octstr_delta, 8))[2:]
        else:
            new_mode = oct(int(octstr_mode, 8) & int(octstr_delta, 8))[2:]
        return new_mode

    def chmod(self, dfs_path, mode):
        """chmod a file with mode in format 755 or g+w"""
        logger.info("chmod(%s, %s)" % (dfs_path, mode))

        assert (
            dfs_path
            and mode
            and (re.match(r"^\d+$", mode) or re.match(r"^[ugo]+[+\-][rwx]+$", mode))
        )
        assert isinstance(dfs_path, str)

        if self.stat(dfs_path):
            perms = self.get_perms(dfs_path)
            if re.match(r"^\d+$", mode):
                # replace existing perms with mode
                new_mode = mode
            elif re.match(r"^[ugo]+[+\-][rwx]+$", mode):
                # combine existing perms with mode
                mode_delta = self.octstr_from_human_perm_delta(mode)
                # combine existing perms with delta, e.g. 755 + 020 = 775
                new_mode = self.apply_mode_delta(
                    perms, mode_delta, self.regex_mode_delta(mode)[1]
                )
                logger.debug("new_mode: %s" % new_mode)
            else:
                raise GOEDfsException('Invalid chmod mode "%s"', mode)

            if not self._dry_run:
                if str(perms) != str(new_mode):
                    self._hdfs.set_permission(dfs_path, new_mode)
                else:
                    logger.debug(
                        "NOOP as mode matches new mode: %s == %s"
                        % (str(perms), str(new_mode))
                    )

    def chgrp(self, dfs_path, group):
        logger.info("chgrp(%s, %s)" % (dfs_path, group))
        assert dfs_path and group
        assert isinstance(dfs_path, str)
        if not self._dry_run:
            self._hdfs.set_owner(dfs_path, group=group)

    def stat(self, dfs_path):
        logger.info("stat(%s)" % dfs_path)
        try:
            if self._hdfs:
                return self._hdfs.status(dfs_path, strict=True)
        except HdfsError as exc:
            if "File does not exist:" in str(exc):
                return None
            raise
        return None

    def container_exists(self, scheme, container):
        raise NotImplementedError("container_exists() not implemented for WebHdfs")

    def copy_from_local(self, local_path, dfs_path, overwrite=False):
        logger.info("copy_from_local(%s, %s)" % (local_path, dfs_path))
        if not self._dry_run:
            try:
                res = self._hdfs.upload(dfs_path, local_path, overwrite=overwrite)
                self.debug("copy_from_local() returned: %s" % res)
            except HdfsError as exc:
                if "already exists" in str(exc) and not overwrite:
                    raise GOEDfsException(
                        "Cannot copy file over existing file: %s" % dfs_path
                    )
                else:
                    raise

    def copy_to_local(self, dfs_path, local_path, overwrite=False):
        logger.info("copy_to_local(%s, %s)" % (dfs_path, local_path))
        if not self._dry_run:
            if file_exists(local_path) and not overwrite:
                raise GOEDfsException(
                    "Cannot copy file over existing file: %s" % local_path
                )
            res = self._hdfs.download(dfs_path, local_path, overwrite=overwrite)
            self.debug("copy_to_local() returned: %s" % res)

    def write(self, dfs_path, data, overwrite=False):
        logger.info("write(%s, data, %s)" % (dfs_path, overwrite))
        if not self._dry_run:
            try:
                self._hdfs.write(dfs_path, data, overwrite)
            except HdfsError as exc:
                if "already exists" in str(exc) and not overwrite:
                    raise GOEDfsException(
                        "Cannot copy file over existing file: %s" % dfs_path
                    )
                else:
                    raise

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
        logger.info("read(%s)" % dfs_path)
        if not self._dry_run:
            with self._hdfs.read(dfs_path) as reader:
                content = reader.read()
                if as_str:
                    content = content.decode()
                return content

    def list_dir(self, dfs_path):
        logger.debug("list_dir(%s)" % dfs_path)
        if self._hdfs:
            return [os.path.join(dfs_path, _) for _ in self._hdfs.list(dfs_path)]
        else:
            return None
