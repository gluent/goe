# Copyright 2024 The GOE Authors. All rights reserved.
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

import fsspec
from gcsfs.core import GCS_MIN_BLOCK_SIZE


def is_gcs_path(path: str):
    return bool(path and path.startswith("gs://"))


def is_valid_path_for_logs(path: str):
    return bool(path and (path.startswith("/") or is_gcs_path(path)))


class GOELogFileHandle:
    name: str = None

    def __init__(self, path: str, mode="w"):
        self._fs = self._get_fs(path)
        self.name = path
        self._fh = self._open(path, mode=mode)

    def __enter__(self):
        return self._fh

    def __exit__(self, type, value, traceback):
        self.close()

    def _get_fs(self, path: str) -> fsspec.AbstractFileSystem:
        """Get fsspec filesystem for path."""
        if path.startswith("gs://"):
            """Do not pass in the token so that gcsfs will try and get the application
            default credentials from a number of sources. This will raise an exception
            if it cannot authenticate with GCS.
            https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem
            """
            fs = fsspec.filesystem("gs", block_size=GCS_MIN_BLOCK_SIZE)
        else:
            fs = fsspec.filesystem("file")
        return fs

    def _open(self, path: str, mode="w"):
        return self._fs.open(path, mode=mode)

    def close(self):
        if not self._fh.closed:
            return self._fh.close()

    def flush(self):
        # On GCSFileSystem _fh.flush() doesn't actually flush unless the buffer is beyond block size.
        # Plus we can't set the block size artifically low. In practice
        self._fh.flush()

    def write(self, *args, **kwargs):
        return self._fh.write(*args, **kwargs)
