#! /usr/bin/env python3
""" hdfs_xfer_file: 'Pluggable' file implementation, used in xfer_xfer.

    This one is for HDFS.

    LICENSE_TEXT
"""

import logging

import hdfs

from gluentlib.util.misc_functions import begin_with
from gluentlib.util.parallel_exec import STATUS_NOOP

from gluentlib.cloud.xfer_file import XferFile, XferFileException, DST_MATCH, DST_NONE, DST_PARTIAL, DST_INVALID
from gluentlib.cloud.offload_logic import DST_S3, DST_HDFS


###############################################################################
# CONSTANTS
###############################################################################

# Some filesystems (i.e. maprfs:) do not expose checksums
UNKNOWN_CHECKSUM = {"checksum": "Unknown"}


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler()) # Disabling logging by default


class HdfsXferFile(XferFile):
    """ Represents HDFS "transfer" file
    """
    def __init__(self, client, section, cfg, hdfs_path):
        assert hdfs_path

        super(HdfsXferFile, self).__init__(client, section, cfg)

        self.hdfs_path = begin_with(hdfs_path, '/')
        self.hdfs_type = cfg.get(section, 'hdfs_url').split(':')[0] # hdfs:// or maprfs://

        logger.debug("Constructed HdfsXferFile object: %s" % self)


    def __str__(self):
        return "[%s] %s:%s" % (self._section, self.hdfs_type, self.hdfs_path)


    # -- Private functions

    def _get_checksum(self):
        checksum = UNKNOWN_CHECKSUM

        try:
            checksum = self._client.checksum(self.hdfs_path) 
        except hdfs.util.HdfsError:
            logger.warn("Unable to extract checksum from: %s" % self)

        return checksum
        

    def _get_stats(self):
        def format_checksum(checksum_dict):
            """ Stable format of checksum dict-to-string
            """
            return "{%s}" % ", ".join("%s: %s" % (p, checksum_dict[p]) for p in sorted(checksum_dict.keys()))

        # Making all stats keys lowercase to be compatible with all platforms
        stats = self._client.status(self.hdfs_path, strict=False)
        if not stats:
            logger.debug("Exception on HEAD response from %s. Assuming, object does not exist" % self)
            return {}
        else:
            stats = {k.lower(): v for k, v in list(stats.items())}

        stats["checksum"] = format_checksum(self._get_checksum())

        logger.debug("HDFS file: %s stats: %s" % (self, stats))
        return stats


    def _compare(self, dst):
        """ Determine the state of DST file
            (i.e. "copied successfully", "invalid" or eligible for "copy continue")

            Returns:
                DST_MATCH:   DST file matches SRC file - copy successful
                DST_INVALID: DST file is invalid (and needs to be re-copied)
                DST_PARTIAL: DST file was partially copied - can resume
                DST_NONE:    DST file does NOT exist
        """
        if not dst.exists:
            logger.debug("Destination file: %s does NOT exist" % dst)
            return DST_NONE

        if dst.stats['length'] > self.stats['length']:
            logger.warn("Destination file: %s is INVALID. Length: %d > source length: %d" % \
                (dst, dst.stats['length'], self.stats['length']))
            return DST_INVALID

        if dst.stats['length'] < self.stats['length']:
            # Previous transfer likely failed - assuming file is "restartable"
            # It would be a lot better to validate that DST partial file is valid,
            # but there seems to be no way to do it
            # Also important: it may take time for DST file to "stabilize"
            logger.warn("Destination file: %s is PARTIALLY COPIED. Length: %d < source length: %d" % \
                (dst, dst.stats['length'], self.stats['length']))
            return DST_PARTIAL

        src_checksum = self.checksum
        dst_checksum = dst.checksum
        logger.debug("Source file: %s checksum: %s" % (self, src_checksum))
        logger.debug("Destination file: :%s checksum: %s" % (dst, dst_checksum))

        if UNKNOWN_CHECKSUM == src_checksum:
            logger.warn("Source file: %s does not expose checksum" % self)
        elif UNKNOWN_CHECKSUM == dst_checksum:
            logger.warn("Destination file: %s does not expose checksum" % dst)
        elif src_checksum['bytes'] != dst_checksum['bytes']:
            logger.warn("Destination file: %s is INVALID. Checksum does NOT match source after copying from: %s" % \
                (dst, self))
            return DST_INVALID

        logger.debug("Copy: %s -> %s is VALID" % (self, dst))
        return DST_MATCH


    # -- Properties

    @property
    def filetype(self):
        return DST_HDFS


    @property
    def checksum(self):
        return self._get_checksum()


    # -- Public functions

    def delete(self):
        logger.debug("Deleting %s" % self)
        self._client.delete(self.hdfs_path)
        self.refresh()


    def copy_hdfs(self, other, force):
        """ Copy file with:
                - resume from previous if destination file exists
                - retry if failure
                - validation after copy

            Returns True if successful (stops re-tries)
            or raises XferFileException if unsuccessful (on to the next re-try)
        """
        dst_status = DST_INVALID if force else self._compare(other)

        if DST_MATCH == dst_status:
            return STATUS_NOOP

        if DST_INVALID == dst_status:
            other.remove(dst_file)

        offset = other.stats['length'] if (other.exists and DST_PARTIAL == dst_status) else 0

        # Actual HDFS -> HDFS copy
        with self.client.read(self.hdfs_path, chunk_size=self.stats['blocksize'], offset=offset) as reader:
            if offset:
                other.client.write(other.hdfs_path, reader, append=True)
            else:
                other.client.write(other.hdfs_path, reader,
                    permission=self.stats['permission'], replication=self.stats['replication'], blocksize=self.stats['blocksize'])

        # Reload stats after copy
        other.refresh()

        # After copy validation - are we good ?
        if DST_MATCH != self._compare(other):
            raise HdfsHdfsXferException("Invalid transfer for: %s -> %s" % (self, other))


    def copy(self, other, force=False):
        logger.debug("Initiating HDFS copy: %s -> %s" % (self, other))

        if DST_HDFS == other.filetype:
            # hdfs(self) -> hdfs(other)
            self.copy_hdfs(other, force)
        elif DST_S3 == other.filetype:
            # hdfs -> s3
            other.upload_hdfs(self)
        else:
            raise NotImplementedError("Copy from HDFS to: %s is not supported yet" % other.filetype)

        logger.debug("Completed HDFS copy: %s -> %s" % (self, other))
