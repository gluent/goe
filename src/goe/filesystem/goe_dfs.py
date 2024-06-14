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

""" GOEDfs: Backend storage (e.g. HDFS) interaction library.
"""

from abc import ABCMeta, abstractmethod, abstractproperty
import logging
import os
import time
from urllib.parse import urlparse

from google.api_core import retry, exceptions as google_exceptions

from goe.offload.offload_messages import VERBOSE, VVERBOSE


###############################################################################
# EXCEPTIONS
###############################################################################


class GOEDfsException(Exception):
    pass


class GOEDfsDeleteNotComplete(Exception):
    pass


class GOEDfsFilesNotVisible(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

DFS_TYPE_DIRECTORY = "DIRECTORY"
DFS_TYPE_FILE = "FILE"

# Retry timeout in seconds
DFS_RETRY_TIMEOUT = 120

GOE_DFS_AZURE = "AZURE"
GOE_DFS_GCS = "GCS"
GOE_DFS_S3 = "S3"
GOE_DFS_SSH = "SSH"
GOE_DFS_WEBHDFS = "WEBHDFS"

# Generic Azure FS scheme used for Snowflake integration
OFFLOAD_FS_SCHEME_AZURE = "azure"
# Generic FS scheme indicating the scheme is derived from Hadoop database and not included explicitly in URIs
OFFLOAD_FS_SCHEME_INHERIT = "inherit"
OFFLOAD_FS_SCHEME_HDFS = "hdfs"
OFFLOAD_FS_SCHEME_S3A = "s3a"
OFFLOAD_FS_SCHEME_MAPRFS = "maprfs"
OFFLOAD_FS_SCHEME_S3 = "s3"
OFFLOAD_FS_SCHEME_GS = "gs"
# Azure Blob Storage
OFFLOAD_FS_SCHEME_WASB = "wasb"
OFFLOAD_FS_SCHEME_WASBS = "wasbs"
# Azure Data Lake Storage Gen1
OFFLOAD_FS_SCHEME_ADL = "adl"
# Azure Data Lake Storage Gen2
OFFLOAD_FS_SCHEME_ABFS = "abfs"
OFFLOAD_FS_SCHEME_ABFSS = "abfss"
VALID_OFFLOAD_FS_SCHEMES = [
    OFFLOAD_FS_SCHEME_INHERIT,
    OFFLOAD_FS_SCHEME_GS,
    OFFLOAD_FS_SCHEME_HDFS,
    OFFLOAD_FS_SCHEME_S3A,
    OFFLOAD_FS_SCHEME_MAPRFS,
    OFFLOAD_FS_SCHEME_S3,
    OFFLOAD_FS_SCHEME_WASB,
    OFFLOAD_FS_SCHEME_WASBS,
    OFFLOAD_FS_SCHEME_ADL,
    OFFLOAD_FS_SCHEME_ABFS,
    OFFLOAD_FS_SCHEME_ABFSS,
]
OFFLOAD_FS_SCHEMES_REQUIRING_ACCOUNT = [OFFLOAD_FS_SCHEME_AZURE]
OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER = [
    OFFLOAD_FS_SCHEME_GS,
    OFFLOAD_FS_SCHEME_S3A,
    OFFLOAD_FS_SCHEME_S3,
    OFFLOAD_FS_SCHEME_WASB,
    OFFLOAD_FS_SCHEME_WASBS,
    OFFLOAD_FS_SCHEME_ADL,
    OFFLOAD_FS_SCHEME_ABFS,
    OFFLOAD_FS_SCHEME_ABFSS,
]
OFFLOAD_NON_HDFS_FS_SCHEMES = [
    _
    for _ in VALID_OFFLOAD_FS_SCHEMES
    if _ not in (OFFLOAD_FS_SCHEME_INHERIT, OFFLOAD_FS_SCHEME_HDFS)
]
OFFLOAD_WEBHDFS_COMPATIBLE_FS_SCHEMES = [
    _
    for _ in VALID_OFFLOAD_FS_SCHEMES
    if _ in (OFFLOAD_FS_SCHEME_INHERIT, OFFLOAD_FS_SCHEME_HDFS)
]
AZURE_OFFLOAD_FS_SCHEMES = [
    OFFLOAD_FS_SCHEME_WASB,
    OFFLOAD_FS_SCHEME_WASBS,
    OFFLOAD_FS_SCHEME_ADL,
    OFFLOAD_FS_SCHEME_ABFS,
    OFFLOAD_FS_SCHEME_ABFSS,
]

UNSUPPORTED_URI_SCHEME_EXCEPTION_TEXT = "Unsupported URI scheme"

# Not using os.sep because that is the path separator of the edge node - not the DFS
URI_SEP = "/"

# Delay after deleting files from cloud storage. We have a retry but also give it chance.
POST_CLOUD_DELETE_WAIT_SECONDS = 0.2


###############################################################################
# STANDALONE FUNCTIONS
###############################################################################


def uri_component_split(uri):
    """Decompose a filesystem URI of format: "scheme://container/path"
    Return a tuple of (scheme, container, path)
    """
    assert uri
    parsed = urlparse(uri, allow_fragments=False)
    scheme = parsed.scheme.lower()
    if scheme and scheme not in VALID_OFFLOAD_FS_SCHEMES:
        raise GOEDfsException(
            "%s: %s" % (UNSUPPORTED_URI_SCHEME_EXCEPTION_TEXT, scheme)
        )
    else:
        container = parsed.netloc
        path = parsed.path
    return scheme, container, path


def get_scheme_from_location_uri(dfs_path):
    """get the scheme from a uri. only return values we understand how to deal with"""
    scheme = dfs_path.split(":")[0]
    if not scheme or scheme[0] == "/":
        return OFFLOAD_FS_SCHEME_HDFS
    if scheme in VALID_OFFLOAD_FS_SCHEMES:
        return scheme
    raise NotImplementedError("Location scheme support %s not implemented" % scheme)


def gen_fs_uri(
    path_prefix: str,
    db_path_suffix=None,
    scheme=None,
    container=None,
    backend_db=None,
    table_name=None,
) -> str:
    """Generates a file URI
    Note this is not getting an existing URI for an existing db or table, it generates a new one based on inputs
    The return value can be at any of 3 levels:
      1) The parent directory of offload databases
      2) The location for CREATE DATABASE commands
      3) The location for CREATE TABLE commands

    A URI looks something like:
        scheme://container/pre/fix/backend_db/table_name/
    Azure URIs have an extra component but this function would expect the account to be defined within container:
        scheme://container@account.with.domain/pre/fix/backend_db/table_name/

    path_prefix: Optional path
    db_path_suffix: Optional suffix for the backend_db part of URI. Used in Hadoop for .db
    container: Bucket/container
    backend_db: Optional extra path component for db/schema name
    table_name: Optional extra path component for table name
    """
    assert path_prefix is not None
    if table_name:
        assert backend_db

    scheme_str = ("%s://" % scheme) if scheme else ""
    container_str = container or ""

    if backend_db and table_name:
        final_path = os.path.join(
            path_prefix.strip(URI_SEP), backend_db + (db_path_suffix or ""), table_name
        )
    elif backend_db:
        final_path = os.path.join(
            path_prefix.strip(URI_SEP), backend_db + (db_path_suffix or "")
        )
    else:
        final_path = path_prefix.strip(URI_SEP)

    return scheme_str + container_str + URI_SEP + final_path


def gen_load_uri_from_options(
    offload_options, hadoop_db=None, table_name=None, scheme_override=None
) -> str:
    """Returns a Hadoop file URI based on options config."""
    uri = gen_fs_uri(
        offload_options.hdfs_load,
        offload_options.hdfs_db_path_suffix,
        backend_db=hadoop_db,
        table_name=table_name,
    )
    return uri


###############################################################################
# LOGGING
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# GOEDfs
###############################################################################


class GOEDfs(object, metaclass=ABCMeta):
    def __init__(self, messages, dry_run=False, do_not_connect=False):
        self._messages = messages
        self._dry_run = dry_run
        self._do_not_connect = do_not_connect
        if dry_run:
            logger.info("* Dry run *")

    @abstractproperty
    def client(self):
        pass

    @abstractmethod
    def delete(self, dfs_path, recursive=False):
        """Delete a file or directory and contents."""

    @abstractmethod
    def chmod(self, dfs_path, mode):
        pass

    @abstractmethod
    def chgrp(self, dfs_path, group):
        pass

    @abstractmethod
    def container_exists(self, scheme, container):
        pass

    @abstractmethod
    def copy_from_local(self, local_path, dfs_path, overwrite=False):
        pass

    @abstractmethod
    def copy_to_local(self, dfs_path, local_path, overwrite=False):
        pass

    @abstractmethod
    def gen_uri(
        self,
        scheme,
        container,
        path_prefix,
        backend_db=None,
        table_name=None,
        container_override=None,
    ):
        """Generate a URI for the DFS in use, mostly a wrapper for gen_fs_uri() but some DFSs have own specifics.
        Some implementations may format the container, container_override provides a way around that.
        """

    @abstractmethod
    def mkdir(self, dfs_path):
        """Create an empty directory, some file systems (such as object stores) will not support this and
        will raise NotImplementedError()
        """

    @abstractmethod
    def read(self, dfs_path, as_str=False):
        """Return the contents of a remote file as bytes unless as_str=True"""

    @abstractmethod
    def rename(self, hdfs_src_path, hdfs_dst_path):
        pass

    @abstractmethod
    def rmdir(self, dfs_path, recursive=False):
        pass

    @abstractmethod
    def stat(self, dfs_path):
        """Returns a dictionary containing assorted attributes for a file.
        For historical reasons the dict is based on that used by WebHDFS but
        other keys may work their way in over time.
        Current expected keys are:
        {'type': 'FILE' or 'DIRECTORY',
         'length': size-of-file-in-bytes,
         'permission': Octal permissions e.g. '755' or '640'
        }
        """

    @abstractmethod
    def write(self, dfs_path: str, data, overwrite=False):
        """Write some data to a remote file"""

    @abstractmethod
    def list_dir(self, dfs_path):
        """Return a list of file/directory names within dfs_path"""

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _post_cloud_delete_wait(self, scheme):
        """For cloud storage sleep for a specific period (POST_CLOUD_DELETE_WAIT_SECONDS) after a delete"""
        if scheme in OFFLOAD_FS_SCHEMES_REQUIRING_CONTAINER:
            self.debug("Post cloud delete sleep: %s" % POST_CLOUD_DELETE_WAIT_SECONDS)
            time.sleep(POST_CLOUD_DELETE_WAIT_SECONDS)

    @staticmethod
    def _uri_component_split(dfs_path):
        """Wrapper for global uri_component_split() but also removes leading
        slash from path before returning values.
        """
        scheme, container, path = uri_component_split(dfs_path)
        if path and path.startswith(URI_SEP):
            path = path.lstrip(URI_SEP)
        return scheme, container, path

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def log(self, msg):
        if self._messages:
            self._messages.log(msg, detail=VERBOSE)
        logger.info(msg)

    def debug(self, msg):
        if self._messages:
            self._messages.log(msg, detail=VVERBOSE)
        logger.debug(msg)

    def notice(self, msg):
        if self._messages:
            self._messages.notice(msg)
        logger.debug(msg)

    def warning(self, msg):
        if self._messages:
            self._messages.warning(msg)
        logger.warning(msg)

    def get_perms(self, dfs_path):
        """Return permissions of a file or path in numeric form, e.g. "755".
        Numeric format is the default for webhdfs therefore no complexity.
        """
        logger.info("get_perms(%s)" % dfs_path)
        stats = self.stat(dfs_path)
        logger.debug(
            "get_perms() returning: %s" % stats["permission"] if stats else stats
        )
        return stats["permission"] if stats else stats

    def convert_rwx_perms_to_oct_str(self, perm_string):
        """Take in incoming string of the format "-rwxr-x---" and return as a string of format "750" """
        logger.info("convert_rwx_perms_to_oct_str(%s)" % perm_string)
        if not perm_string:
            return None
        assert len(perm_string) >= 10, "Unexpected permission format %s" % perm_string
        # Only supports rwx keys. That should be fine for GOE usage
        perm_values = {"r": 4, "w": 2, "x": 1}
        oct_str = ""
        # Remove directory part and any trailing characters (e.g. "+")
        perm_string = perm_string[1:10]
        logger.debug("Trimmed string to %s" % perm_string)
        # Sum up the octal value of each set of 3 perm characters
        for i in range(3):
            oct_str += str(
                sum(
                    [
                        perm_values.get(char, 0)
                        for char in list(perm_string[i * 3 : i * 3 + 3])
                    ]
                )
            )
        logger.debug("Returning %s" % oct_str)
        return oct_str

    def command_contains_injection_concerns(self, command_string):
        concerning_chars = [";", "|", ">"]
        return any(_ for _ in concerning_chars if _ in command_string)

    @retry.Retry(
        predicate=retry.if_exception_type(
            GOEDfsFilesNotVisible, google_exceptions.ServiceUnavailable
        ),
        deadline=DFS_RETRY_TIMEOUT,
    )
    def list_dir_and_wait_for_contents(self, dfs_path):
        """Same as list_dir but wait for positive results.
        Useful when target DFS suffers from eventual consistency.
        """
        files = self.list_dir(dfs_path)
        if not files:
            self.debug("Files in %s not yet visible, waiting" % dfs_path)
            raise GOEDfsFilesNotVisible
        return files
