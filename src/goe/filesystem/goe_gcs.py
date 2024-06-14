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

""" GOEGcs: GCS implementation of GOEDfs
"""

import logging
from os.path import basename, exists as file_exists
from requests import ConnectionError

from google.api_core import retry, exceptions as google_exceptions
from google.cloud import storage

from goe.filesystem.goe_dfs import (
    GOEDfs,
    GOEDfsDeleteNotComplete,
    GOEDfsException,
    gen_fs_uri,
    DFS_RETRY_TIMEOUT,
    DFS_TYPE_DIRECTORY,
    DFS_TYPE_FILE,
    GOE_DFS_GCS,
    URI_SEP,
)

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
# GOEGcs
###############################################################################


class GOEGcs(GOEDfs):
    """A GOE wrapper over Google Cloud Storage API.
    https://googleapis.dev/python/storage/latest/client.html
    dry_run: Do not make changes.
    do_not_connect: Do not even connect, used for unit testing.
    """

    def __init__(
        self, messages, dry_run=False, do_not_connect=False, db_path_suffix=None
    ):
        assert messages

        logger.info("Client setup: GOEGcs")

        super(GOEGcs, self).__init__(
            messages, dry_run=dry_run, do_not_connect=do_not_connect
        )

        if do_not_connect:
            self._client = None
        else:
            self._client = storage.Client()
        self._db_path_suffix = db_path_suffix
        self.dfs_mechanism = GOE_DFS_GCS
        self.backend_dfs = "GCS"

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def chgrp(self, dfs_path, group):
        raise NotImplementedError("chgrp() not implemented for GOEGcs")

    def chmod(self, dfs_path, mode):
        raise NotImplementedError("chmod() not implemented for GOEGcs")

    def client(self):
        return self._client

    def container_exists(self, scheme, container):
        assert container
        try:
            return bool(self._client and self._client.get_bucket(container))
        except google_exceptions.NotFound:
            return False

    def copy_from_local(self, local_path, dfs_path, overwrite=False):
        assert local_path
        assert dfs_path
        assert isinstance(local_path, str)
        assert isinstance(dfs_path, str)
        logger.info("copy_from_local(%s, %s)" % (local_path, dfs_path))
        scheme, container, path = self._uri_component_split(dfs_path)
        target_path = (path + basename(local_path)) if path.endswith(URI_SEP) else path
        self.debug(
            "Copying to target scheme/container/path: %s"
            % str([scheme, container, target_path])
        )
        if not self._dry_run:
            bucket = self._client.get_bucket(container)
            blob = bucket.blob(target_path)
            if blob.exists() and not overwrite:
                raise GOEDfsException(
                    "Cannot copy file over existing file: %s" % target_path
                )
            blob.upload_from_filename(local_path)

    def copy_to_local(self, dfs_path, local_path, overwrite=False):
        assert dfs_path
        assert local_path
        assert isinstance(dfs_path, str)
        assert isinstance(local_path, str)
        logger.info("copy_to_local(%s, %s)" % (dfs_path, local_path))
        scheme, container, path = self._uri_component_split(dfs_path)
        self.debug(
            "Copying from scheme/container/path: %s" % str([scheme, container, path])
        )
        if not self._dry_run:
            bucket = self._client.get_bucket(container)
            blob = bucket.blob(path)
            if file_exists(local_path) and not overwrite:
                raise GOEDfsException(
                    "Cannot copy file over existing file: %s" % local_path
                )
            with open(local_path, "wb") as file_handle:
                blob.download_to_file(file_handle)

    @retry.Retry(
        predicate=retry.if_exception_type(
            GOEDfsDeleteNotComplete,
            ConnectionError,
            google_exceptions.ServiceUnavailable,
        ),
        deadline=DFS_RETRY_TIMEOUT,
    )
    def delete(self, dfs_path, recursive=False):
        """Delete a file or directory and contents"""

        def pragmatic_delete(blob):
            """Run a delete but accept the fact that the blob may have disappeared in the meantime"""
            try:
                blob.delete()
            except google_exceptions.NotFound:
                self.debug(
                    "delete(%s) returned 404 (NotFound), accepting as deleted"
                    % blob.name
                )

        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("delete(%s, %s)" % (dfs_path, recursive))
        scheme, container, path = self._uri_component_split(dfs_path)
        self.debug("Delete scheme/container/path: %s" % str([scheme, container, path]))
        if self._dry_run:
            return None
        bucket = self._client.get_bucket(container)
        if not recursive or (self.stat(dfs_path) or {}).get("type") == DFS_TYPE_FILE:
            # Just delete the single blob
            blob = bucket.blob(path)
            if not blob.exists():
                return None
            self.debug("delete(%s)" % blob.name)
            pragmatic_delete(blob)
            self._post_cloud_delete_wait(scheme)
            if blob.exists():
                self.debug("GCS delete incomplete, retrying")
                raise GOEDfsDeleteNotComplete
        else:
            self.debug("Recursive delete using directory prefix: %s" % path)
            if not path.endswith(URI_SEP):
                path += URI_SEP
            # No batching (self._client.batch()) here because that circumvents the NotFound catch in pragmatic_delete()
            found_files = False
            for blob in self._client.list_blobs(bucket, prefix=path):
                found_files = True
                if blob.exists():
                    self.debug("delete(%s)" % blob.name)
                    pragmatic_delete(blob)
            if found_files:
                self._post_cloud_delete_wait(scheme)
                blobs_pending_delete = [
                    _ for _ in self._client.list_blobs(bucket, prefix=path)
                ]
                if blobs_pending_delete:
                    self.debug("GCS delete incomplete, retrying")
                    raise GOEDfsDeleteNotComplete
        # If we get this far then it worked
        return True

    def gen_uri(
        self,
        scheme,
        container,
        path_prefix,
        backend_db=None,
        table_name=None,
        container_override=None,
    ):
        uri = gen_fs_uri(
            path_prefix,
            self._db_path_suffix,
            scheme=scheme,
            container=container_override or container,
            backend_db=backend_db,
            table_name=table_name,
        )
        return uri

    @retry.Retry(
        predicate=retry.if_exception_type(google_exceptions.GatewayTimeout),
        deadline=DFS_RETRY_TIMEOUT,
    )
    def list_dir(self, dfs_path):
        """Return a list of file/directory names within dfs_path"""
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("list_dir(%s)" % dfs_path)
        scheme, container, path = self._uri_component_split(dfs_path)
        self.debug(
            "Listing contents of scheme/container/path: %s"
            % str([scheme, container, path])
        )
        if path and not path.endswith(URI_SEP):
            path += URI_SEP
        if self._client:
            bucket = self._client.get_bucket(container)
            blobs = self._client.list_blobs(bucket, prefix=path, delimiter=URI_SEP)
            # We need to iterate over all blobs in order to populate blobs.prefixes (directories).
            # Also exclude the directory we are checking contents of.
            blob_names = [_.name for _ in blobs if _.name != path]
            blob_names.extend(blobs.prefixes)
            # Tag the scheme & bucket back on the front of listing results
            return [self.gen_uri(scheme, container, _) for _ in blob_names]

    def mkdir(self, dfs_path):
        """No mkdir on GCS"""
        pass

    @retry.Retry(
        predicate=retry.if_exception_type(google_exceptions.GatewayTimeout),
        deadline=DFS_RETRY_TIMEOUT,
    )
    def read(self, dfs_path, as_str=False):
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("read(%s)" % dfs_path)
        scheme, container, path = self._uri_component_split(dfs_path)
        self.debug(
            "Downloading contents of scheme/container/path: %s"
            % str([scheme, container, path])
        )
        if self._dry_run:
            return None
        bucket = self._client.get_bucket(container)
        blob = bucket.blob(path)
        if not blob.exists():
            raise GOEDfsException("Cannot download non-existent file: %s" % path)
        if as_str:
            return blob.download_as_text()
        else:
            return blob.download_as_bytes()

    def rename(self, hdfs_src_path, hdfs_dst_path):
        raise NotImplementedError("rename() not implemented for GOEGcs")

    def rmdir(self, dfs_path, recursive=False):
        return self.delete(dfs_path, recursive=recursive)

    @retry.Retry(
        predicate=retry.if_exception_type(google_exceptions.GatewayTimeout),
        deadline=DFS_RETRY_TIMEOUT,
    )
    def stat(self, dfs_path):
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("stat(%s)" % dfs_path)

        scheme, container, path = self._uri_component_split(dfs_path)

        if not self._client:
            return None

        self.debug(
            "Checking status of scheme/container/path: %s"
            % str([scheme, container, path])
        )

        bucket = self._client.get_bucket(container)
        # No delimiter parameter because we want to be able to stat directories
        blobs = list(self._client.list_blobs(bucket, prefix=path))
        # We need to ensure we only match the requested path, not
        # any other files that happen to start with the same characters
        matched_files = [_ for _ in blobs if _.name == path]
        matched_files_by_dir = [
            _ for _ in blobs if _.name.startswith(path.rstrip(URI_SEP) + URI_SEP)
        ]

        if len(matched_files) > 1:
            self.debug("Multiple file matches: %s" % str(matched_files))
            raise GOEDfsException("Path matches multiple files: %s" % dfs_path)
        elif matched_files:
            return {
                "length": matched_files[0].size,
                "permission": None,
                "type": DFS_TYPE_FILE,
            }
        elif matched_files_by_dir:
            # We found file entries prefixed with dfs_path as a dir, so we know it is a directory
            return {"length": 0, "permission": None, "type": DFS_TYPE_DIRECTORY}
        else:
            return None

    def write(self, dfs_path: str, data, overwrite=False):
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("write(%s)" % dfs_path)
        scheme, container, path = self._uri_component_split(dfs_path)
        self.debug(
            "Writing contents of scheme/container/path: %s"
            % str([scheme, container, path])
        )
        if self._dry_run:
            return None
        bucket = self._client.get_bucket(container)
        blob = bucket.blob(path)
        if blob.exists() and not overwrite:
            raise GOEDfsException("Cannot write to existing file: %s" % path)
        blob.upload_from_string(data)
