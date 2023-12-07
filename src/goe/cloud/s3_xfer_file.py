#! /usr/bin/env python3
""" s3_xfer_file: 'Pluggable' file implementation, used in xfer_xfer.

    This one is for S3.

    LICENSE_TEXT
"""

import logging
import socket

from botocore.vendored.requests.packages.urllib3.exceptions import \
    ReadTimeoutError
from botocore.exceptions import IncompleteReadError, ClientError

from boto3.s3.transfer import TransferConfig

from goe.cloud.xfer_file import XferFile, XferFileException
from goe.cloud.offload_logic import DST_S3, DST_HDFS


###############################################################################
# CONSTANTS
###############################################################################

S3_RETRIABLE_ERRORS = (
    socket.timeout, 
    ReadTimeoutError,
    IncompleteReadError
)

# Supported extra options for S3
# (mapping between config file and boto parameters)
SUPPORTED_S3_OPTIONS = {
    'server_side_encryption': 'ServerSideEncryption',
    'sse_customer_algorithm': 'SSECustomerAlgorithm',
    'sse_customer_key': 'SSECustomerKey',
    'sse_customer_key_md5': 'SSECustomerKeyMD5',
    'sse_kms_key_id': 'SSEKMSKeyId',
    'storage_class': 'StorageClass',
}
SUPPORTED_S3_OPTION_VALUES = {
    'ServerSideEncryption': ('AES256', 'aws:kms'),
    'SSECustomerAlgorithm': '*', # '*' == Any value allowed
    'SSECustomerKey': '*',
    'SSECustomerKeyMD5': '*',
    'SSEKMSKeyId': '*',
    'StorageClass': ('STANDARD', 'STANDARD_IA', 'REDUCED_REDUNDANCY'),
}

# Supported S3 transfer config options (aka: boto3.s3.transfer.TransferConfig)
SUPPORTED_S3_TRANSFER_CONFIG_OPTIONS = list(TransferConfig().__dict__.keys())

# (hdfs) Metadata, recorded with every S3 file
S3_METADATA_KEYS = {
    'owner': str,
    'group': str,
    'permission': str,
    'blocksize': int,
    'replication': int,
    'checksum': lambda x: x # Struct
}


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


class S3XferFile(XferFile):
    """ Represents S3 "transfer" file
    """
    def __init__(self, client, section, cfg, bucket, prefix):
        assert bucket and prefix

        super(S3XferFile, self).__init__(client, section, cfg)

        self.bucket = bucket.strip('/')
        self.prefix = prefix.strip('/')

        logger.debug("Constructed S3XferFile object: %s" % self)


    def __str__(self):
        return "[%s] s3://%s/%s" % (self._section, self.bucket, self.prefix)


    # -- Private methods

    def _get_stats(self):
        def cast_to_proper_type(key, value):
            if value is not None and key in S3_METADATA_KEYS:
                return S3_METADATA_KEYS[key](value)
            else:
                return value

        try:
            kwargs = self._get_extra_s3_options(('SSECustomerAlgorithm', 'SSECustomerKey'))

            response = self._client.head_object(Bucket=self.bucket, Key=self.prefix, **kwargs)
        except ClientError as e:
            logger.debug("Exception: %s on HEAD response from %s. Assuming, object does not exist" % \
                (e, self))
            return {}

        if not response:
            logger.debug("No HEAD response from %s. Assuming, object does not exist" % self)
            return {}
        if response.get('DeleteMarker', False):
            logger.debug("Object: %s is deleted. Treating as though it does not exist" % self)
            return {}

        # S3 metadata
        metadata = response.get('Metadata', {})
        stats = {k: cast_to_proper_type(k, metadata.get(k, None)) for k in metadata if k in S3_METADATA_KEYS}

        # S3 stats
        stats['length'] = int(response['ContentLength'])

        logger.debug("S3 file: %s stats: %s" % (self, stats))
        return stats


    # -- Private methods

    def _s3_file_metadata(self, stats):
        s3_metadata = {k: str(stats[k]) for k in S3_METADATA_KEYS if k in stats}

        logger.debug("S3 file metadata: %s" % s3_metadata)
        return s3_metadata


    def _get_extra_s3_options(self, restrict_options=None):
        """ Get extra S3 options (aka: ExtraArgs)
            if they are defined in configuration file (encryption etc)
        """
        options = {
            SUPPORTED_S3_OPTIONS[k]: self._cfg.get(self._section, k)
            for k in self._cfg.options(self._section)
            if k in SUPPORTED_S3_OPTIONS
        }

        # Only return options that are supported by callers
        if restrict_options:
            options = {k: v for k, v in list(options.items()) if k in restrict_options}

        for opt in options:
            if ('*' != SUPPORTED_S3_OPTION_VALUES[opt]) and (options[opt] not in SUPPORTED_S3_OPTION_VALUES[opt]):
                raise XferFileException("Value: %s is invalid for option: %s" % (options[opt], opt))

        logger.debug("Extra S3 options: %s" % options)
        return options


    def _get_boto_transfer_config(self):
        """ Get boto3.s3.transfer.TransferConfig options
            if they are defined in configuration file
        """
        extra_options = {
            k: int(self._cfg.get(self._section, k))
            for k in self._cfg.options(self._section)
            if k in SUPPORTED_S3_TRANSFER_CONFIG_OPTIONS
        }
        logger.debug("Extra S3 transfer options: %s" % extra_options)

        options = TransferConfig(**extra_options)
        logger.debug("S3 transfer options: %s" % options.__dict__)
        return options


    # -- Properties

    @property
    def filetype(self):
        return DST_S3


    # -- Public routines

    def copy_s3(self, dst_s3):
        args = {
            'Bucket':   dst_s3.bucket,
            'Key':      dst_s3.prefix,
            'Metadata': self._s3_file_metadata(self.stats),
            'CopySource': {
                'Bucket': self.bucket,
                'Key':    self.prefix 
            }
        }
        args.update(dst_s3._get_extra_s3_options())
        self._client.copy_object(**args)


    def upload_hdfs(self, src_hdfs):
        kwargs = self._get_extra_s3_options()
        kwargs['Metadata'] = self._s3_file_metadata(src_hdfs.stats)

        with src_hdfs.client.read(src_hdfs.hdfs_path) as reader:
            args = {
                'Fileobj':  reader,
                'Bucket':   self.bucket,
                'Key':      self.prefix,
                'ExtraArgs':kwargs
            }
            config = self._get_boto_transfer_config()
            if config:
                args['Config'] = config

            self._client.upload_fileobj(**args)


    def download_hdfs(self, dst_hdfs):
        kwargs = self._get_extra_s3_options(("SSECustomerAlgorithm", "SSECustomerKey", "SSECustomerKeyMD5"))
        with dst_hdfs.client.write(
            dst_hdfs.hdfs_path,
            overwrite=True,
            permission=self.stats['permission'],
            blocksize=self.stats['blocksize'],
            replication=self.stats['replication']
        ) as writer:
            args = {
                'Fileobj':  writer,
                'Bucket':   self.bucket,
                'Key':      self.prefix,
                'ExtraArgs':kwargs
            }
            config = self._get_boto_transfer_config()
            if config:
                args['Config'] = config

            self._client.download_fileobj(**args)


    def copy(self, other, force=False):
        logger.debug("Initiating S3 copy: %s -> %s" % (self, other))

        if DST_HDFS == other.filetype:
            # s3 -> hdfs
            self.download_hdfs(other)
        elif DST_S3 == other.filetype:
            self.copy_s3(other)
        else:
            raise NotImplementedError("Copy from S3 to: %s is not supported yet" % other.filetype)

        logger.debug("Completed S3 copy: %s -> %s" % (self, other))
