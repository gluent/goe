#! /usr/bin/env python3
"""
LICENSE_TEXT
"""

from gluentlib.filesystem.gluent_dfs import OFFLOAD_FS_SCHEME_GS, OFFLOAD_FS_SCHEME_S3, OFFLOAD_FS_SCHEME_S3A,\
    AZURE_OFFLOAD_FS_SCHEMES
from gluentlib.offload.offload_constants import HADOOP_BASED_BACKEND_DISTRIBUTIONS


def get_dfs_from_options(offload_options, messages=None, force_ssh=False, dry_run=None):
    """ Helper function to get an appropriate GluentDfs object based on offload options.
    """
    if dry_run is None:
        dry_run = bool(not offload_options.execute)

    if offload_options.backend_distribution in HADOOP_BASED_BACKEND_DISTRIBUTIONS:
        if offload_options.webhdfs_host and offload_options.webhdfs_port and not force_ssh:
            from gluentlib.filesystem.web_hdfs import WebHdfs
            return WebHdfs(offload_options.webhdfs_host,
                           offload_options.webhdfs_port,
                           offload_options.hadoop_ssh_user,
                           True if offload_options.kerberos_service else False,
                           offload_options.webhdfs_verify_ssl,
                           dry_run=dry_run,
                           messages=messages,
                           db_path_suffix=offload_options.hdfs_db_path_suffix,
                           hdfs_data=offload_options.hdfs_data)
        else:
            from gluentlib.filesystem.cli_hdfs import CliHdfs
            return CliHdfs(offload_options.hdfs_host,
                           offload_options.hadoop_ssh_user,
                           dry_run=dry_run,
                           messages=messages,
                           db_path_suffix=offload_options.hdfs_db_path_suffix,
                           hdfs_data=offload_options.hdfs_data)
    elif offload_options.offload_fs_scheme == OFFLOAD_FS_SCHEME_GS:
        from gluentlib.filesystem.gluent_gcs import GluentGcs
        return GluentGcs(messages, dry_run=dry_run,
                         db_path_suffix=offload_options.hdfs_db_path_suffix)
    elif offload_options.offload_fs_scheme in (OFFLOAD_FS_SCHEME_S3, OFFLOAD_FS_SCHEME_S3A):
        from gluentlib.filesystem.gluent_s3 import GluentS3
        return GluentS3(messages, dry_run=dry_run,
                        db_path_suffix=offload_options.hdfs_db_path_suffix)
    elif offload_options.offload_fs_scheme in AZURE_OFFLOAD_FS_SCHEMES:
        from gluentlib.filesystem.gluent_azure import GluentAzure
        return GluentAzure(offload_options.offload_fs_azure_account_name,
                           offload_options.offload_fs_azure_account_key,
                           offload_options.offload_fs_azure_account_domain,
                           messages,
                           dry_run=dry_run,
                           db_path_suffix=offload_options.hdfs_db_path_suffix)
    else:
        if offload_options.offload_fs_scheme:
            raise NotImplementedError('Backend system/scheme has not been implemented: %s/%s'
                                      % (offload_options.target, offload_options.offload_fs_scheme))
        else:
            raise NotImplementedError('Backend system has not been implemented: %s' % offload_options.target)
