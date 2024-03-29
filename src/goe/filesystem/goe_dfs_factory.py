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

from goe.filesystem.goe_dfs import (
    OFFLOAD_FS_SCHEME_GS,
    OFFLOAD_FS_SCHEME_S3,
    OFFLOAD_FS_SCHEME_S3A,
    AZURE_OFFLOAD_FS_SCHEMES,
)
from goe.offload.offload_constants import HADOOP_BASED_BACKEND_DISTRIBUTIONS


def get_dfs_from_options(
    offload_options, messages=None, force_ssh=False, dry_run=None, do_not_connect=False
):
    """Helper function to get an appropriate GOEDfs object based on offload options."""
    if dry_run is None:
        dry_run = bool(not offload_options.execute)

    if offload_options.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
        if (
            offload_options.webhdfs_host
            and offload_options.webhdfs_port
            and not force_ssh
        ):
            from goe.filesystem.web_hdfs import WebHdfs

            return WebHdfs(
                offload_options.webhdfs_host,
                offload_options.webhdfs_port,
                offload_options.hadoop_ssh_user,
                True if offload_options.kerberos_service else False,
                offload_options.webhdfs_verify_ssl,
                dry_run=dry_run,
                messages=messages,
                do_not_connect=do_not_connect,
                db_path_suffix=offload_options.hdfs_db_path_suffix,
                hdfs_data=offload_options.hdfs_data,
            )
        else:
            from goe.filesystem.cli_hdfs import CliHdfs

            return CliHdfs(
                offload_options.hdfs_host,
                offload_options.hadoop_ssh_user,
                dry_run=dry_run,
                messages=messages,
                do_not_connect=do_not_connect,
                db_path_suffix=offload_options.hdfs_db_path_suffix,
                hdfs_data=offload_options.hdfs_data,
            )
    elif offload_options.offload_fs_scheme == OFFLOAD_FS_SCHEME_GS:
        from goe.filesystem.goe_gcs import GOEGcs

        return GOEGcs(
            messages,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            db_path_suffix=offload_options.hdfs_db_path_suffix,
        )
    elif offload_options.offload_fs_scheme in (
        OFFLOAD_FS_SCHEME_S3,
        OFFLOAD_FS_SCHEME_S3A,
    ):
        from goe.filesystem.goe_s3 import GOES3

        return GOES3(
            messages,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            db_path_suffix=offload_options.hdfs_db_path_suffix,
        )
    elif offload_options.offload_fs_scheme in AZURE_OFFLOAD_FS_SCHEMES:
        from goe.filesystem.goe_azure import GOEAzure

        return GOEAzure(
            offload_options.offload_fs_azure_account_name,
            offload_options.offload_fs_azure_account_key,
            offload_options.offload_fs_azure_account_domain,
            messages,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            db_path_suffix=offload_options.hdfs_db_path_suffix,
        )
    else:
        if offload_options.offload_fs_scheme:
            raise NotImplementedError(
                "Backend system/scheme has not been implemented: %s/%s"
                % (offload_options.target, offload_options.offload_fs_scheme)
            )
        else:
            raise NotImplementedError(
                "Backend system has not been implemented: %s" % offload_options.target
            )
