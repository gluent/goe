""" TestGluentDfs: Unit test library to test API for all supported backend filesystems.
    This focuses on API calls that do not need to connect to the system.
    Because there is no connection we can fake any backend and test basic functionality.
"""
import logging
import os
from unittest import main

from goe.filesystem.gluent_dfs import (
    GluentDfsException,
    DFS_TYPE_DIRECTORY,
    DFS_TYPE_FILE,
)
from goe.filesystem.gluent_dfs_factory import get_dfs_from_options

from tests.unit.filesystem.test_gluent_dfs import TestGluentDfs
from tests.integration.test_functions import (
    build_current_options,
)
from tests.testlib.test_framework.test_functions import (
    get_test_messages,
)


###############################################################################
# LOGGING
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# TestCurrentDfs
###############################################################################


class TestCurrentDfs(TestGluentDfs):
    def __init__(self, *args, **kwargs):
        super(TestCurrentDfs, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = None
        self.some_file = None
        self.some_local_file = "/tmp/temp_file.txt"

    def setUp(self):
        config = build_current_options()
        messages = get_test_messages(config, "TestCurrentDfs")
        self.api = get_dfs_from_options(config, messages, dry_run=True)
        self.some_dir = self.api.gen_uri(
            config.offload_fs_scheme,
            config.offload_fs_container,
            config.offload_fs_prefix,
        )
        self.some_file = self.some_dir + "/a-file"

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestCurrentDfsExecuteMode
###############################################################################


class TestCurrentDfsExecuteMode(TestGluentDfs):
    """This class actually copies files around to prove the API does exactly as it is supposed to
    The API will be used to create a specific structure and then interrogate it ensuring inputs and
    outputs are as expected.
    Structure:
        configured container/prefix/
            unittests/
                file1.txt
                file2.txt (unicode contents)
                file3.txt (bytes content)
                dir_l1/
                    file4.txt
                    dir_l2/
                        file5.txt
    """

    def __init__(self, *args, **kwargs):
        super(TestCurrentDfsExecuteMode, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = None
        self.some_file = None
        self._local_dir = "/tmp"
        self._file1 = "dfs-unit-file1.txt"
        self._file2 = "dfs-unit-file2.txt"
        self._file3 = "dfs-unit-file3.txt"
        self._file4 = "dfs-unit-file4.txt"
        self._file5 = "dfs-unit-file5.txt"
        self._test_files = {
            self._file1: {"content": "file 1 contents", "subdir": None},
            self._file2: {"content": "file 2 contents", "subdir": None},
            self._file3: {"content": b"file 3 contents", "subdir": None},
            self._file4: {"content": "file 3 contents", "subdir": "dir_l1"},
            self._file5: {"content": "file 4 contents", "subdir": "dir_l1/dir_l2"},
        }
        self._remote_dir = None

    def _check_dir_has_x_entries(self, remote_path, expected_dir_entries):
        files = self.api.list_dir(remote_path)
        self.assertIsInstance(files, list)
        self.assertEqual(len(files), expected_dir_entries)

    def _create_local_files(self):
        def create_tmp_file(file_name, contents):
            with open(
                self._local_path(file_name),
                "wb" if isinstance(contents, bytes) else "w",
            ) as writer:
                writer.write(contents)

        for file_name in self._test_files:
            create_tmp_file(file_name, self._test_files[file_name]["content"])

    def _delete_local_files(self):
        for file_name in self._test_files:
            os.remove(self._local_path(file_name))

    def _delete_remote_files(self):
        for file_name in self._test_files:
            self.api.delete(self._remote_path(file_name))

    def _local_path(self, file_name):
        return self._local_dir + "/" + file_name

    def _remote_path(self, file_name=None, with_subdir=True):
        path = self._remote_dir
        if file_name:
            if with_subdir and self._test_files[file_name]["subdir"]:
                file_name = (
                    self._test_files[file_name]["subdir"].rstrip("/") + "/" + file_name
                )
            path = path.rstrip("/") + "/" + file_name
        return path

    def _try_mkdir(self, path):
        try:
            self.api.mkdir(path)
        except NotImplementedError:
            pass

    def setUp(self):
        config = build_current_options()
        messages = get_test_messages(config, "TestCurrentDfsExecuteMode")
        self.api = get_dfs_from_options(config, messages, dry_run=False)
        self._remote_dir = (
            self.api.gen_uri(
                config.offload_fs_scheme,
                config.offload_fs_container,
                config.offload_fs_prefix,
            ).rstrip("/")
            + "/"
            + "unittests"
            + "/"
        )

    def test_dfs_in_execute_mode(self):
        """Upload/download/delete files to fully prove the API, other unit tests skim test the API
        The API will be used to create a specific structure and then interrogate it ensuring inputs and
        outputs are as expected.
        Structure:
            configured container/prefix/
                unittests/
                    file1.txt
                    file2.txt
                    dir_level1/
                        file3.txt
                        dir_level2/
                            file4.txt
        """
        # Create test files on local storage and delete any existing unit test remote files
        self._create_local_files()
        self._delete_remote_files()
        # mkdir parent directory on DFSs that support mkdir
        self._try_mkdir(self._remote_path())

        # Before we start check list_dir find no entries
        files = self.api.list_dir(self._remote_path())
        self.assertFalse(
            bool(files), "These files should not be here: {}".format(files)
        )

        # Upload file1 using copy_from_local()
        self.api.copy_from_local(
            self._local_path(self._file1),
            self._remote_path(self._file1),
            overwrite=False,
        )
        stat = self.api.stat(self._remote_path(self._file1))
        self.assertEqual(stat["type"], DFS_TYPE_FILE)
        # File exists and therefore should fail
        with self.assertRaises(GluentDfsException):
            self.api.copy_from_local(
                self._local_path(self._file1),
                self._remote_path(self._file1),
                overwrite=False,
            )
        # Check we can overwrite
        self.api.copy_from_local(
            self._local_path(self._file1),
            self._remote_path(self._file1),
            overwrite=True,
        )
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file1), as_str=True)
        self.assertEqual(self._test_files[self._file1]["content"], content)

        # Upload file2 using write()
        self.api.write(
            self._remote_path(self._file2),
            self._test_files[self._file2]["content"],
            overwrite=False,
        )
        # File exists and therefore should fail
        with self.assertRaises(GluentDfsException):
            self.api.write(
                self._remote_path(self._file2),
                self._test_files[self._file2]["content"],
                overwrite=False,
            )
        # Check we can overwrite
        self.api.write(
            self._remote_path(self._file2),
            self._test_files[self._file2]["content"],
            overwrite=True,
        )
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file2), as_str=True)
        self.assertEqual(self._test_files[self._file2]["content"], content)

        # Upload file3 (bytes content) using both copy_from_local() and write()
        self.api.copy_from_local(
            self._local_path(self._file3),
            self._remote_path(self._file3),
            overwrite=False,
        )
        stat = self.api.stat(self._remote_path(self._file3))
        self.assertEqual(stat["type"], DFS_TYPE_FILE)
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file3))
        self.assertEqual(self._test_files[self._file3]["content"], content)
        # Overwrite with write()
        self.api.write(
            self._remote_path(self._file3),
            self._test_files[self._file3]["content"],
            overwrite=True,
        )
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file3))
        self.assertEqual(self._test_files[self._file3]["content"], content)

        # Check list_dir has 3 entries
        self._check_dir_has_x_entries(self._remote_path(), 3)

        # Test copy_to_local
        with self.assertRaises(GluentDfsException):
            self.api.copy_to_local(
                self._remote_path(self._file1),
                self._local_path(self._file1),
                overwrite=False,
            )
        self.api.copy_to_local(
            self._remote_path(self._file1),
            self._local_path(self._file1),
            overwrite=True,
        )

        # Upload file4 in a subdirectory
        self._try_mkdir(
            self._remote_path(
                self._test_files[self._file4]["subdir"], with_subdir=False
            )
        )
        self.api.copy_from_local(
            self._local_path(self._file4),
            self._remote_path(self._file4),
            overwrite=False,
        )
        stat = self.api.stat(self._remote_path(self._file4))
        self.assertEqual(stat["type"], DFS_TYPE_FILE)
        stat = self.api.stat(
            self._remote_path(
                self._test_files[self._file4]["subdir"], with_subdir=False
            )
        )
        self.assertEqual(stat["type"], DFS_TYPE_DIRECTORY)
        # Directory also visible with trailing slash
        stat = self.api.stat(
            self._remote_path(
                self._test_files[self._file4]["subdir"], with_subdir=False
            )
            + "/"
        )
        self.assertEqual(stat["type"], DFS_TYPE_DIRECTORY)
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file4), as_str=True)
        self.assertEqual(self._test_files[self._file4]["content"], content)

        # Check top level list_dir now has 4 entries
        self._check_dir_has_x_entries(self._remote_path(), 4)

        # Upload file4 in a subdirectory of a subdirectory
        self._try_mkdir(
            self._remote_path(
                self._test_files[self._file5]["subdir"], with_subdir=False
            )
        )
        self.api.copy_from_local(
            self._local_path(self._file5),
            self._remote_path(self._file5),
            overwrite=False,
        )
        stat = self.api.stat(self._remote_path(self._file5))
        self.assertEqual(stat["type"], DFS_TYPE_FILE)
        stat = self.api.stat(
            self._remote_path(
                self._test_files[self._file5]["subdir"], with_subdir=False
            )
        )
        self.assertEqual(stat["type"], DFS_TYPE_DIRECTORY)
        # Directory also visible with trailing slash
        stat = self.api.stat(
            self._remote_path(
                self._test_files[self._file5]["subdir"], with_subdir=False
            )
            + "/"
        )
        self.assertEqual(stat["type"], DFS_TYPE_DIRECTORY)
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file5), as_str=True)
        self.assertEqual(self._test_files[self._file5]["content"], content)

        # Check top level list_dir still has 4 entries
        self._check_dir_has_x_entries(self._remote_path(), 4)

        # Delete a file
        self.assertIn(
            self._remote_path(self._file2), self.api.list_dir(self._remote_path())
        )
        self.api.delete(self._remote_path(self._file2))
        self.assertNotIn(
            self._remote_path(self._file2), self.api.list_dir(self._remote_path())
        )
        # Deleting non-existent file is fine
        self.api.delete(self._remote_path(self._file2))

        # Check top level list_dir now has 3 entries
        self._check_dir_has_x_entries(self._remote_path(), 3)

        # Delete a directory and contents
        self.api.delete(
            self._remote_path(
                self._test_files[self._file4]["subdir"], with_subdir=False
            ),
            recursive=True,
        )
        self.assertIsNone(self.api.stat(self._remote_path(self._file4)))
        self.assertIsNone(self.api.stat(self._remote_path(self._file5)))
        # File 1 should still exist
        stat = self.api.stat(self._remote_path(self._file1))
        self.assertEqual(stat["type"], DFS_TYPE_FILE)

        # Tidy up test files
        self._delete_local_files()
        self._delete_remote_files()


if __name__ == "__main__":
    main()
