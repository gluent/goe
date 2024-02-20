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

""" GOES3: S3 implementation of GOEDfs
"""

import logging
from os.path import basename, exists as file_exists

import boto3
from botocore.exceptions import ClientError
from google.api_core import retry

from goe.filesystem.goe_dfs import (
    GOEDfs,
    GOEDfsDeleteNotComplete,
    GOEDfsException,
    gen_fs_uri,
    DFS_RETRY_TIMEOUT,
    DFS_TYPE_DIRECTORY,
    DFS_TYPE_FILE,
    GOE_DFS_S3,
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


class GOES3(GOEDfs):
    """A GOE wrapper over Amazon S3 Storage API (Boto3).
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
    dry_run: Do not make changes.
    do_not_connect: Do not even connect, used for unit testing.
    """

    def __init__(
        self, messages, dry_run=False, do_not_connect=False, db_path_suffix=None
    ):
        assert messages

        logger.info("Client setup: GOES3")

        super(GOES3, self).__init__(
            messages, dry_run=dry_run, do_not_connect=do_not_connect
        )

        if do_not_connect:
            self._client = None
        else:
            self._client = boto3.resource("s3")
        self._db_path_suffix = db_path_suffix
        self.dfs_mechanism = GOE_DFS_S3
        self.backend_dfs = "S3"

    ###########################################################################
    # PRIVATE METHODS
    ###########################################################################

    def _blob_exists(self, container, path):
        s3_object = self._client.Object(container, path)
        try:
            s3_object.get()
            return True
        except self._client.meta.client.exceptions.NoSuchKey:
            return False

    def _list_by_prefix(self, container, path, recursive=False):
        """Return a dict of stat() dicts for objects in a bucket matching a prefix
        I wanted to use a Bucket resource to get this list but Delimiter parameter in code
        below was resulting in no matches, had to resort to client and pagination:
            s3_bucket = self._client.Bucket(container)
            for blob in s3_bucket.objects.filter(Prefix=path, Delimiter=URI_SEP):
                blob_type = DFS_TYPE_DIRECTORY if blob.key.endswith(URI_SEP) else DFS_TYPE_FILE
                blobs[blob.key] = {'length': blob.size, 'permission': None, 'type': blob_type}
        """

        def get_paginator(recursive):
            if recursive:
                return paginator.paginate(Bucket=container, Prefix=path)
            else:
                return paginator.paginate(
                    Bucket=container, Prefix=path, Delimiter=URI_SEP
                )

        logger.info("_list_by_prefix(%s, %s)" % (container, path))
        matched_entries = {}
        if self._client:
            s3_client = self._client.meta.client
            paginator = s3_client.get_paginator("list_objects_v2")
            for resp in get_paginator(recursive):
                for subdir in resp.get("CommonPrefixes") or []:
                    matched_entries[subdir["Prefix"]] = {
                        "length": 0,
                        "permission": None,
                        "type": DFS_TYPE_DIRECTORY,
                    }
                for subobj in resp.get("Contents") or []:
                    matched_entries[subobj["Key"]] = {
                        "length": subobj["Size"],
                        "permission": None,
                        "type": DFS_TYPE_FILE,
                    }

        return matched_entries

    ###########################################################################
    # PUBLIC METHODS
    ###########################################################################

    def chgrp(self, dfs_path, group):
        raise NotImplementedError("chgrp() not implemented for GOES3")

    def chmod(self, dfs_path, mode):
        raise NotImplementedError("chmod() not implemented for GOES3")

    def client(self):
        return self._client

    def container_exists(self, scheme, container):
        assert container
        if not self._client:
            return False
        try:
            response = self._client.meta.client.head_bucket(Bucket=container)
            return True
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code in [403, 404]:
                return False
            else:
                raise

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
            if self._blob_exists(container, target_path) and not overwrite:
                raise GOEDfsException(
                    "Cannot copy file over existing file: %s" % target_path
                )
            s3_bucket = self._client.Bucket(container)
            s3_bucket.upload_file(Filename=local_path, Key=target_path)

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
            s3_bucket = self._client.Bucket(container)
            s3_bucket.download_file(Key=path, Filename=local_path)

    @retry.Retry(
        predicate=retry.if_exception_type(GOEDfsDeleteNotComplete),
        deadline=DFS_RETRY_TIMEOUT,
    )
    def delete(self, dfs_path, recursive=False):
        """Delete a file or directory and contents"""
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("delete(%s, %s)" % (dfs_path, recursive))
        scheme, container, path = self._uri_component_split(dfs_path)
        self.debug("Delete scheme/container/path: %s" % str([scheme, container, path]))
        if not recursive or (self.stat(dfs_path) or {}).get("type") == DFS_TYPE_FILE:
            # Delete the single blob
            if self._dry_run:
                return None
            self._client.Object(container, path).delete()
            self._post_cloud_delete_wait(scheme)
            if self.stat(dfs_path):
                self.debug("S3 delete incomplete, retrying")
                raise GOEDfsDeleteNotComplete
        else:
            self.debug("Recursive delete using directory prefix: %s" % path)
            if not path.endswith(URI_SEP):
                path += URI_SEP
            objects = self._list_by_prefix(container, path, recursive=True)
            if self._dry_run or not objects:
                return None
            delete_request = [{"Key": _} for _ in list(objects.keys())]
            s3_bucket = self._client.Bucket(container)
            delete_response = s3_bucket.delete_objects(
                Delete={"Objects": delete_request}
            )
            if delete_response and delete_response.get("Errors"):
                # There were failures
                self.debug(
                    "Object delete errors: %s" % str(delete_response.get("Errors"))
                )
                problem_keys = [_["Key"] for _ in delete_response.get("Errors")]
                raise GOEDfsException(
                    "Errors deleting following objects from S3: %s" % str(problem_keys)
                )
            self._post_cloud_delete_wait(scheme)
            if self._list_by_prefix(container, path, recursive=True):
                self.debug("S3 delete incomplete, retrying")
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

    def list_dir(self, dfs_path):
        """Return a list of file/directory names within dfs_path"""
        assert dfs_path
        assert isinstance(dfs_path, str)
        logger.info("list_dir(%s)" % dfs_path)
        scheme, container, path = self._uri_component_split(dfs_path)
        if path and not path.endswith(URI_SEP):
            path += URI_SEP
        self.debug(
            "Listing contents of scheme/container/path: %s"
            % str([scheme, container, path])
        )
        objects = self._list_by_prefix(container, path)
        if objects:
            self.debug("Matched objects: %s" % len(objects))
            # We need to prefix the scheme & bucket back on the front of results
            return [self.gen_uri(scheme, container, _) for _ in list(objects.keys())]
        else:
            return None

    def mkdir(self, dfs_path):
        """No mkdir on S3"""
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
        try:
            s3_object = self._client.Object(container, path)
            s3_response = s3_object.get()
            if s3_response and s3_response.get("Body"):
                content = s3_response["Body"].read()
                if as_str:
                    content = content.decode()
                return content
        except self._client.meta.client.exceptions.NoSuchKey:
            raise GOEDfsException("Cannot download non-existent file: %s" % path)

    def rename(self, hdfs_src_path, hdfs_dst_path):
        raise NotImplementedError("rename() not implemented for GOES3")

    def rmdir(self, dfs_path, recursive=False):
        return self.delete(dfs_path, recursive=recursive)

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

        # We don't want a trailing '/' which would send us down a path level
        path = path.rstrip(URI_SEP)

        matches = self._list_by_prefix(container, path)
        if path in matches:
            return matches[path]
        elif path + URI_SEP in matches:
            return matches[path + URI_SEP]
        else:
            self.debug("Could not find path in %s matched files" % len(matches))
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
        if self._blob_exists(container, path) and not overwrite:
            raise GOEDfsException("Cannot write over existing file: %s" % path)
        s3_bucket = self._client.Bucket(container)
        s3_bucket.put_object(Key=path, Body=data)
