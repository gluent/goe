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

""" GOEAzure: Azure implementation of GOEDfs
"""

import logging
from os.path import basename, exists as file_exists

from azure.common import AzureMissingResourceHttpError
from azure.core.exceptions import HttpResponseError
from azure.storage.blob import BlobServiceClient
from google.api_core import retry

from goe.filesystem.goe_dfs import (
    GOEDfs,
    GOEDfsDeleteNotComplete,
    GOEDfsException,
    gen_fs_uri,
    uri_component_split,
    OFFLOAD_FS_SCHEME_ABFS,
    OFFLOAD_FS_SCHEME_ABFSS,
    DFS_RETRY_TIMEOUT,
    DFS_TYPE_DIRECTORY,
    DFS_TYPE_FILE,
    GOE_DFS_AZURE,
    URI_SEP,
)


###############################################################################
# EXCEPTIONS
###############################################################################


class GOEAzureTryAgainException(Exception):
    pass


###############################################################################
# CONSTANTS
###############################################################################

###############################################################################
# GLOBAL FUNCTIONS
###############################################################################


def azure_fq_container_name(
    azure_account_name, azure_account_domain, container, account_in_path=False
):
    """Returns a fully qualified Azure container name based on options, e.g. container@account.domain.
    account_in_path is an override giving the format: account.domain/container
    """
    assert azure_account_name
    assert azure_account_domain
    domain = azure_account_domain
    if not domain.startswith("."):
        domain = "." + domain
    fq_account = azure_account_name + domain
    if account_in_path:
        container = fq_account + URI_SEP + container
    else:
        container = container + "@" + fq_account
    return container


###############################################################################
# LOGGING
###############################################################################

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


###############################################################################
# GOEAzure
###############################################################################


class GOEAzure(GOEDfs):
    """A GOE wrapper over Azure Cloud Storage API.
    https://docs.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob?view=azure-python-previous
    This code is specific to v2.x of the module which we are tied to because of Python 2.7 and the Snowflake client.
    When we upgrade to Python 3 we'll likely end up with a newer version of the Snowflake client and a newer version
    of the Azure client which will require wholesale changes to this module because the top level Azure methods
    are different.
    dry_run: Do not make changes.
    do_not_connect: Do not even connect, used for unit testing.
    """

    def __init__(
        self,
        azure_account_name,
        azure_account_key,
        azure_account_domain,
        messages,
        dry_run=False,
        do_not_connect=False,
        db_path_suffix=None,
    ):
        assert azure_account_name
        assert azure_account_key
        assert messages

        logger.info("Client setup: GOEAzure")

        super(GOEAzure, self).__init__(
            messages, dry_run=dry_run, do_not_connect=do_not_connect
        )

        if azure_account_domain and not azure_account_domain.startswith("."):
            azure_account_domain = "." + azure_account_domain

        if do_not_connect:
            self._client = None
        else:
            azure_url = "https://{}{}".format(azure_account_name, azure_account_domain)
            self._client = BlobServiceClient(
                account_url=azure_url, credential=azure_account_key
            )
        self._azure_account_name = azure_account_name
        self._azure_account_domain = azure_account_domain
        self._db_path_suffix = db_path_suffix
        self.dfs_mechanism = GOE_DFS_AZURE
        self.backend_dfs = "Azure"

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _blob_exists(self, container, path):
        blobs = self._list_blob_names(container, path)
        return bool([_ for _ in blobs if _ == path])

    def _container_exists(self, container):
        buckets = self._client.list_containers(name_starts_with=container)
        return bool([_.name for _ in buckets if _.name == container])

    def _list_blobs(self, container, prefix, recursive=False):
        container_client = self._client.get_container_client(container)
        if recursive:
            blobs = [_ for _ in container_client.list_blobs(name_starts_with=prefix)]
        else:
            blobs = [
                _
                for _ in container_client.walk_blobs(
                    name_starts_with=prefix, delimiter=URI_SEP
                )
            ]
        return blobs

    def _list_blob_names(self, container, prefix, recursive=False):
        return [
            _.name for _ in self._list_blobs(container, prefix, recursive=recursive)
        ]

    @staticmethod
    def _uri_component_split(dfs_path):
        """Override of interface _uri_component_split() removing any account/domain from container name"""
        scheme, container, path = uri_component_split(dfs_path)
        if path and path.startswith(URI_SEP):
            path = path.lstrip(URI_SEP)
        if container and "@" in container:
            container = container.split("@")[0]
        return scheme, container, path

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def chgrp(self, dfs_path, group):
        raise NotImplementedError("chgrp() not implemented for GOEAzure")

    def chmod(self, dfs_path, mode):
        raise NotImplementedError("chmod() not implemented for GOEAzure")

    def client(self):
        return self._client

    def container_exists(self, scheme, container):
        assert container
        return bool(self._client and self._container_exists(container))

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
            if not self._container_exists(container):
                raise GOEDfsException("Container does not exist: %s" % container)
            if self._blob_exists(container, target_path) and not overwrite:
                raise GOEDfsException(
                    "Cannot copy file over existing file: %s" % target_path
                )
            container_client = self._client.get_container_client(container)
            with open(local_path, "rb") as data:
                container_client.upload_blob(
                    name=target_path, data=data, overwrite=overwrite
                )

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
            if file_exists(local_path) and not overwrite:
                raise GOEDfsException(
                    "Cannot copy file over existing file: %s" % local_path
                )
            blob_client = self._client.get_blob_client(container, blob=path)
            with open(local_path, "wb") as file:
                data = blob_client.download_blob()
                file.write(data.readall())

    @retry.Retry(
        predicate=retry.if_exception_type(
            GOEDfsDeleteNotComplete, GOEAzureTryAgainException
        ),
        deadline=DFS_RETRY_TIMEOUT,
    )
    def delete(self, dfs_path, recursive=False):
        """Delete a file or directory and contents"""

        def pragmatic_delete(container, path):
            """Run a delete but accept the fact that the blob may have disappeared in the meantime"""
            try:
                container_client = self._client.get_container_client(container)
                container_client.delete_blob(path)
            except AzureMissingResourceHttpError:
                self.debug(
                    "delete_blob(%s, %s) returned 404 (AzureMissingResourceHttpError), accepting as deleted"
                    % (container, path)
                )

        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("delete(%s, %s)" % (dfs_path, recursive))
        scheme, container, path = self._uri_component_split(dfs_path)
        self.debug("Delete scheme/container/path: %s" % str([scheme, container, path]))
        if self._dry_run:
            return None
        if not recursive or (self.stat(dfs_path) or {}).get("type") == DFS_TYPE_FILE:
            # Just delete the single blob
            if not self._blob_exists(container, path):
                return None
            self.debug("delete_blob(%s, %s)" % (container, path))
            pragmatic_delete(container, path)
            self._post_cloud_delete_wait(scheme)
            if self._blob_exists(container, path):
                self.debug("Azure delete incomplete, retrying")
                raise GOEDfsDeleteNotComplete
        else:
            self.debug("Recursive delete using directory prefix: %s" % path)
            if not path.endswith(URI_SEP):
                path += URI_SEP
            found_files = False
            try:
                blobs_pending_delete = self._list_blob_names(
                    container, path, recursive=True
                )
            except HttpResponseError as exc:
                if "try again after some time" in str(exc):
                    self.debug(
                        "Azure list_blobs timeout, retrying operation:\n{}".format(
                            str(exc)
                        )
                    )
                    raise GOEAzureTryAgainException
                else:
                    raise

            if scheme in [OFFLOAD_FS_SCHEME_ABFS, OFFLOAD_FS_SCHEME_ABFSS]:
                # Reversing the order of the paths so deepest entries are first reduced the number of
                # round trips cleaning up non-empty directories for ABFS.
                blobs_pending_delete = list(reversed(blobs_pending_delete))

            # For a recursive delete we need the directory itself in the list
            if path not in blobs_pending_delete:
                blobs_pending_delete.append(path)
            if path.rstrip(URI_SEP) not in blobs_pending_delete:
                blobs_pending_delete.append(path.rstrip(URI_SEP))
            for blob_name in blobs_pending_delete:
                if self._blob_exists(container, blob_name):
                    found_files = True
                    self.debug("delete_blob(%s)" % blob_name)
                    pragmatic_delete(container, blob_name)
                elif scheme in [OFFLOAD_FS_SCHEME_ABFS, OFFLOAD_FS_SCHEME_ABFSS]:
                    if (
                        self.stat(self.gen_uri(scheme, container, blob_name)) or {}
                    ).get("type") == DFS_TYPE_DIRECTORY:
                        # On ABFS directories don't disappear when they empty (like prefixes do).
                        # We need to explicitly delete them.
                        found_files = True
                        try:
                            pragmatic_delete(container, blob_name.rstrip("/"))
                        except HttpResponseError as exc:
                            if "DirectoryIsNotEmpty" not in str(exc):
                                raise
                self._post_cloud_delete_wait(scheme)
            if found_files:
                blobs_pending_delete = self._list_blob_names(
                    container, path, recursive=True
                )
                if blobs_pending_delete:
                    self.debug("Azure delete incomplete, retrying")
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
        if container_override:
            fq_container = container_override
        else:
            fq_container = azure_fq_container_name(
                self._azure_account_name, self._azure_account_domain, container
            )
        uri = gen_fs_uri(
            path_prefix,
            self._db_path_suffix,
            scheme=scheme,
            container=fq_container,
            backend_db=backend_db,
            table_name=table_name,
        )
        return uri

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
            if not self._container_exists(container):
                raise GOEDfsException("Container does not exist: %s" % container)
            blob_names = self._list_blob_names(container, path)
            # Exclude the directory we are checking contents of
            blob_names = [_ for _ in blob_names if _ != path]
            # Tag the scheme & bucket back on the front of listing results
            return [self.gen_uri(scheme, container, _) for _ in blob_names]

    def mkdir(self, dfs_path):
        """No mkdir on Azure block storage"""
        pass

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
        if not self._blob_exists(container, path):
            raise GOEDfsException("Cannot download non-existent file: %s" % path)
        blob_client = self._client.get_blob_client(container, blob=path)
        data = blob_client.download_blob()
        content = data.readall()
        if as_str:
            content = content.decode()
        return content

    def rename(self, hdfs_src_path, hdfs_dst_path):
        raise NotImplementedError("rename() not implemented for GOEAzure")

    def rmdir(self, dfs_path, recursive=False):
        return self.delete(dfs_path, recursive=recursive)

    @retry.Retry(
        predicate=retry.if_exception_type(GOEAzureTryAgainException),
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

        if not self._container_exists(container):
            raise GOEDfsException("Container does not exist: %s" % container)

        # We don't want a trailing '/' which would send us down a path level
        path = path.rstrip(URI_SEP)

        try:
            blobs = self._list_blobs(container, path)
        except HttpResponseError as exc:
            if "OperationTimedOut" in str(exc):
                self.debug(
                    "Azure list_blobs timeout, retrying operation:\n{}".format(str(exc))
                )
                raise GOEAzureTryAgainException
            else:
                raise
        matched_files = [_ for _ in blobs if _.name == path]
        matched_dirs = [_ for _ in blobs if _.name == path.rstrip(URI_SEP) + URI_SEP]
        if matched_dirs:
            return {"length": 0, "permission": None, "type": DFS_TYPE_DIRECTORY}
        elif len(matched_files) > 1:
            self.debug("Multiple file matches: %s" % str(matched_files))
            raise GOEDfsException("Path matches multiple files: %s" % dfs_path)
        elif matched_files:
            return {
                "length": matched_files[0].size,
                "permission": None,
                "type": DFS_TYPE_FILE,
            }
        else:
            return None

    def write(self, dfs_path, data, overwrite=False):
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
        if not self._container_exists(container):
            raise GOEDfsException("Container does not exist: %s" % container)

        if self._blob_exists(container, path) and not overwrite:
            raise GOEDfsException("Cannot write to existing file: %s" % path)
        container_client = self._client.get_container_client(container)
        container_client.upload_blob(name=path, data=data, overwrite=overwrite)
