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

from typing import TYPE_CHECKING

from goe.filesystem.goe_dfs import (
    OFFLOAD_FS_SCHEME_GS,
    OFFLOAD_FS_SCHEME_S3,
    OFFLOAD_FS_SCHEME_S3A,
    AZURE_OFFLOAD_FS_SCHEMES,
)
from goe.offload.offload_constants import HADOOP_BASED_BACKEND_DISTRIBUTIONS

if TYPE_CHECKING:
    from goe.config.orchestration_config import OrchestrationConfig
    from goe.filesystem.goe_dfs import GOEDfs


def get_dfs_from_options(
    config: "OrchestrationConfig",
    messages=None,
    force_ssh=False,
    dry_run=True,
    do_not_connect=False,
) -> "GOEDfs":
    """Helper function to get an appropriate GOEDfs object based on offload options."""
    if config.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
        if config.webhdfs_host and config.webhdfs_port and not force_ssh:
            from goe.filesystem.web_hdfs import WebHdfs

            return WebHdfs(
                config.webhdfs_host,
                config.webhdfs_port,
                config.hadoop_ssh_user,
                True if config.kerberos_service else False,
                config.webhdfs_verify_ssl,
                dry_run=dry_run,
                messages=messages,
                do_not_connect=do_not_connect,
                db_path_suffix=config.hdfs_db_path_suffix,
                hdfs_data=config.hdfs_data,
            )
        else:
            from goe.filesystem.cli_hdfs import CliHdfs

            return CliHdfs(
                config.hdfs_host,
                config.hadoop_ssh_user,
                dry_run=dry_run,
                messages=messages,
                do_not_connect=do_not_connect,
                db_path_suffix=config.hdfs_db_path_suffix,
                hdfs_data=config.hdfs_data,
            )
    elif config.offload_fs_scheme == OFFLOAD_FS_SCHEME_GS:
        from goe.filesystem.goe_gcs import GOEGcs

        return GOEGcs(
            messages,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            db_path_suffix=config.hdfs_db_path_suffix,
        )
    elif config.offload_fs_scheme in (
        OFFLOAD_FS_SCHEME_S3,
        OFFLOAD_FS_SCHEME_S3A,
    ):
        from goe.filesystem.goe_s3 import GOES3

        return GOES3(
            messages,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            db_path_suffix=config.hdfs_db_path_suffix,
        )
    elif config.offload_fs_scheme in AZURE_OFFLOAD_FS_SCHEMES:
        from goe.filesystem.goe_azure import GOEAzure

        return GOEAzure(
            config.offload_fs_azure_account_name,
            config.offload_fs_azure_account_key,
            config.offload_fs_azure_account_domain,
            messages,
            dry_run=dry_run,
            do_not_connect=do_not_connect,
            db_path_suffix=config.hdfs_db_path_suffix,
        )
    else:
        if config.offload_fs_scheme:
            raise NotImplementedError(
                "Backend system/scheme has not been implemented: %s/%s"
                % (config.target, config.offload_fs_scheme)
            )
        else:
            raise NotImplementedError(
                "Backend system has not been implemented: %s" % config.target
            )
