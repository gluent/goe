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

""" xfer_xfer: Source agnostic 'file transfer' object

    Uses 'pluggable' modules as "transfer destinations", i.e.:

    s3_xfer_file
    hdfs_xfer_file
"""

import logging
import threading

from goe.offload.offload_messages import OffloadMessagesMixin

from goe.util.parallel_exec import ParallelExecutor, STATUS_NOOP
from goe.util.misc_functions import end_by

from goe.cloud.hdfs_store import HdfsStore
from goe.cloud.s3_store import S3Store
from goe.cloud.offload_logic import DST_S3, DST_HDFS

from goe.cloud.xfer_file import XferFile
from goe.cloud.hdfs_xfer_file import HdfsXferFile
from goe.cloud.s3_xfer_file import S3XferFile


###############################################################################
# EXCEPTIONS
###############################################################################
class XferXferException(Exception):
    pass


###############################################################################
# LOGGING
###############################################################################
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())  # Disabling logging by default


class XferXfer(OffloadMessagesMixin, object):
    def __init__(self, cfg, messages=None):
        """CONSTRUCTOR"""

        self._cfg = cfg
        self._clients = {}  # Client objects (1 for each: thread + type)
        self._executor = ParallelExecutor()  # Job executor object

        super(XferXfer, self).__init__(messages, logger)

        logger.debug("XferXfer() object successfully initialized")

    ###########################################################################
    # PRIVATE ROUTINES
    ###########################################################################

    def _allocate_client(self, section):
        section_type = self._cfg.section_type(section)

        if DST_S3 == section_type:
            return S3Store.s3_client()
        elif DST_HDFS == section_type:
            return HdfsStore.hdfscli_connect(self._cfg, section)
        else:
            raise XferXferException("Client for: %s is not supported" % section_type)

    def _get_client(self, section):
        if section not in self._clients:
            self._clients[section] = {}

        current_thread = threading.current_thread()
        if current_thread not in self._clients[section]:
            self._clients[section][current_thread] = self._allocate_client(section)

        return self._clients[section][current_thread]

    def _build_xfer_file(self, section, filename):
        section_type = self._cfg.section_type(section)
        if DST_S3 == section_type:
            return S3XferFile(
                client=self._get_client(section),
                section=section,
                cfg=self._cfg,
                bucket=self._cfg.get(section, "s3_bucket"),
                prefix="%s%s"
                % (end_by(self._cfg.get(section, "s3_prefix"), "/"), filename),
            )
        elif DST_HDFS == section_type:
            return HdfsXferFile(
                client=self._get_client(section),
                section=section,
                cfg=self._cfg,
                hdfs_path="%s%s"
                % (end_by(self._cfg.get(section, "hdfs_root"), "/"), filename),
            )
        else:
            raise XferXferException("File type: %s is not supported" % section_type)

    ###########################################################################
    # PROPERTIES
    ###########################################################################

    # Passthrough executor states
    @property
    def submitted(self):
        return self._executor.submitted

    @property
    def executed(self):
        return self._executor.executed

    @property
    def successful(self):
        return self._executor.successful

    @property
    def failed(self):
        return self._executor.failed

    @property
    def noop(self):
        return self._executor.noop

    ###########################################################################
    # PUBLIC ROUTINES
    ###########################################################################

    def copy(self, src_section, src_key, dst_section, dst_key, job_id=1, force=False):
        """Destination agnostic "file copy" routine: src -> dst
        as in: src_section:src_key -> dst_section:dst_key
        """
        assert src_section and src_key and dst_section and dst_key
        src = self._build_xfer_file(src_section, src_key)
        dst = self._build_xfer_file(dst_section, dst_key)

        logger.debug(
            "[Job: %d] Starting single file copy from: %s to: %s" % (job_id, src, dst)
        )
        self.log_vverbose("Copying: %s -> %s" % (src, dst))

        # Run xfer
        if not src.exists:
            raise XferXferException(
                "[FILE COPY job: %d] FAILED: File: %s does NOT exist" % (job_id, src)
            )

        if (not force) and dst.equals(src):
            logger.debug("File: %s is the same as: %s. Skipping copy" % (src, dst))
            return STATUS_NOOP

        src.copy(dst)
        logger.debug(
            "[Job: %d] Finish single file copy from: %s to: %s. Success"
            % (job_id, src, dst)
        )

        return True

    def multicopy(self, copy_pairs, parallel=1, kill_on_first_error=False, force=False):
        """Destination agnostic "file copy" routine
        that accepts multiple "copy pairs" and copies them in parallel

        "copy pair": (source_obj, destination_obj)
        """

        def emit_job(job_id, file_item):
            src_section, src_key, dst_section, dst_key = file_item
            return [
                self.copy,
                src_section,
                src_key,
                dst_section,
                dst_key,
                job_id,
                force,
            ]

        assert copy_pairs

        logger.debug("Starting multiple file copy with: %s parallel slaves" % parallel)

        status = all(
            self._executor.execute_in_threads(
                parallel,
                (
                    emit_job(job_id, file_item)
                    for job_id, file_item in enumerate(copy_pairs)
                ),
                kill_on_first_error=kill_on_first_error,
            )
        )

        # Display final status
        logger.debug("OVERALL STATUS=%s" % status)

        return status

    def listcopy(
        self,
        src_section,
        dst_section,
        src_files,
        dst_remap=lambda x: x,
        parallel=1,
        kill_on_first_error=False,
        force=False,
    ):
        """Copy a list of files from source to destination

        src/dst _section: 'remote-offload' configuration file sections
        src_files: file list, starting from the 'section root'
        dst_remap: Remapping function to be applied to src files to make dst_name
        kill_on_first_error: Whether to exit immediately on 1st copy error
        force: Re-copy file even if it is 'the same'
        """
        assert src_section and dst_section and src_files

        logger.debug(
            "Starting list copy from: %s to: %s with: %s parallel slaves"
            % (src_section, dst_section, parallel)
        )

        copy_pairs = (
            (src_section, src_f, dst_section, dst_remap(src_f)) for src_f in src_files
        )
        self.multicopy(copy_pairs, parallel, kill_on_first_error, force)


if __name__ == "__main__":
    import sys

    from goe.util.misc_functions import set_goelib_logging, options_list_to_namespace
    from goe.offload.offload_messages import OffloadMessages
    from goe.util.config_file import GOERemoteConfig

    def usage(prog_name):
        print(
            "usage: %s hdfs_section s3_section <file_name> [option=value ...] ['log_level']"
            % prog_name
        )
        sys.exit(1)

    def main():
        if len(sys.argv) < 4:
            usage(sys.argv[0])

        log_level = sys.argv[-1:][0].upper()
        if log_level not in ("DEBUG", "INFO", "WARNING", "CRITICAL", "ERROR"):
            log_level = "CRITICAL"
        set_goelib_logging(log_level)

        src, dst, filename = sys.argv[1:4]
        args = [_ for _ in sys.argv[4:] if _.upper() != log_level]
        user_options = vars(options_list_to_namespace(args))
        messages = user_options.get("messages", False)
        if messages:
            messages = OffloadMessages(messages)

        cfg = GOERemoteConfig()

        xfer = XferXfer(cfg, messages=messages)
        xfer.listcopy(src, dst, [filename])

    main()
