""" TestGluentDfs: Unit test library to test API for all supported backend filesystems.
    This focuses on API calls that do not need to connect to the system.
    Because there is no connection we can fake any backend and test basic functionality.
"""
import logging
import os
from unittest import TestCase, main

from tests.offload.unittest_functions import build_current_options

from gluentlib.filesystem.cli_hdfs import CliHdfs
from gluentlib.filesystem.gluent_azure import GluentAzure
from gluentlib.filesystem.gluent_gcs import GluentGcs
from gluentlib.filesystem.gluent_s3 import GluentS3
from gluentlib.filesystem.web_hdfs import WebHdfs
from gluentlib.filesystem.gluent_dfs import GluentDfsException, gen_fs_uri, uri_component_split, \
    DFS_TYPE_DIRECTORY, DFS_TYPE_FILE, UNSUPPORTED_URI_SCHEME_EXCEPTION_TEXT
from gluentlib.filesystem.gluent_dfs_factory import get_dfs_from_options
from gluentlib.offload.offload_messages import OffloadMessages

###############################################################################
# LOGGING
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# TestGluentDfs
###############################################################################

class TestGluentDfs(TestCase):
    """ An intermediate class that defines the individual tests that are
        inherited by the actual backend APIs.
    """

    def __init__(self, *args, **kwargs):
        super(TestGluentDfs, self).__init__(*args, **kwargs)
        self.api = None
        self.options = None
        self.some_dir = None
        self.some_file = None
        self.some_local_file = None

    def _test_chmod(self):
        try:
            self.api.chmod(self.some_dir, '755')
            self.api.chmod(self.some_dir, 'g+w')
        except NotImplementedError:
            pass

    def _test_chgrp(self):
        try:
            self.api.chgrp(self.some_dir, 'a-group')
        except NotImplementedError:
            pass

    def _test_container_exists(self):
        try:
            self.assertIn(self.api.container_exists('some-scheme', 'some-container'), (True, False))
            if self.options:
                self.assertTrue(self.api.container_exists(self.options.offload_fs_scheme, self.options.offload_fs_container))
                self.assertFalse(self.api.container_exists(self.options.offload_fs_scheme, 'this-is-defo-not-a-container'))
        except NotImplementedError:
            pass

    def _test_copy_from_local(self):
        self.api.copy_from_local(self.some_local_file, self.some_file)
        self.api.copy_from_local(self.some_local_file, self.some_file, overwrite=True)

    def _test_copy_to_local(self):
        self.api.copy_to_local(self.some_file, self.some_local_file)
        self.api.copy_to_local(self.some_file, self.some_local_file, overwrite=True)

    def _test_delete(self):
        self.api.delete(self.some_file)
        self.api.delete(self.some_dir, recursive=True)

    def _test_list_dir(self):
        listing = self.api.list_dir(self.some_dir)
        if listing:
            self.assertIsInstance(listing, list)

    def _test_list_snapshots(self):
        try:
            self.api.list_snapshots(self.some_dir)
        except NotImplementedError:
            pass

    def _test_list_snapshottable_dirs(self):
        try:
            self.api.list_snapshottable_dirs()
        except NotImplementedError:
            pass

    def _test_mkdir(self):
        self.api.mkdir(self.some_dir)

    def _test_read(self):
        self.api.read(self.some_file)

    def _test_rename(self):
        try:
            self.api.rename(self.some_file, self.some_file + '_2')
        except NotImplementedError:
            pass

    def _test_rmdir(self):
        self.api.rmdir(self.some_dir)

    def _test_stat(self):
        stat = self.api.stat(self.some_dir)
        if stat:
            self.assertIsInstance(stat, dict)
            self.assertIn('type', stat)
            self.assertEqual(stat['type'], DFS_TYPE_DIRECTORY)
            self.assertIn('length', stat)
        stat = self.api.stat(self.some_file)
        if stat:
            self.assertIsInstance(stat, dict)
            self.assertIn('type', stat)
            self.assertEqual(stat['type'], DFS_TYPE_FILE)
            self.assertIn('length', stat)

    def _test_gen_fs_uri(self):
        def check_gen_fs_uri(expected_uri, path_prefix, db_path_suffix=None, scheme=None, container=None,
                             backend_db=None, table_name=None):
            uri = gen_fs_uri(path_prefix, db_path_suffix=db_path_suffix, scheme=scheme, container=container,
                             backend_db=backend_db, table_name=table_name)
            self.assertIsInstance(uri, str)
            self.assertEqual(uri, expected_uri)

        check_gen_fs_uri('s3a://bucket/some-path', 'some-path', scheme='s3a', container='bucket')
        check_gen_fs_uri('gs://dev-bucket/some-path/dev-name/db_load/my-table', 'some-path/dev-name', scheme='gs',
                         container='dev-bucket', backend_db='db_load', table_name='my-table')
        check_gen_fs_uri('hdfs:///user/gluent/db_load.db/my-table', '/user/gluent', '.db', scheme='hdfs',
                         backend_db='db_load', table_name='my-table')
        check_gen_fs_uri('/user/gluent/db_load.db/my-table', '/user/gluent', '.db',
                         backend_db='db_load', table_name='my-table')
        check_gen_fs_uri('wasb://bucket@account.blob.blah/some-path', 'some-path', scheme='wasb',
                         container='bucket@account.blob.blah')

    def _test_uri_component_split(self):
        def check_uri_component_split(path, expected_scheme, expected_container, expected_path):
            parts = uri_component_split(path)
            self.assertIsInstance(parts, tuple)
            self.assertEqual(len(parts), 3)
            self.assertEqual(parts[0], expected_scheme)
            self.assertEqual(parts[1], expected_container)
            self.assertEqual(parts[2], expected_path)

        check_uri_component_split('s3a://bucket/some-path/some-file', 's3a', 'bucket', '/some-path/some-file')
        check_uri_component_split('gs://bucket/some-path/some-file', 'gs', 'bucket', '/some-path/some-file')
        check_uri_component_split('hdfs://host:123/some-path/some-file', 'hdfs', 'host:123', '/some-path/some-file')
        check_uri_component_split('hdfs://ha-nameservice/some-path/some-file', 'hdfs', 'ha-nameservice', '/some-path/some-file')
        check_uri_component_split('hdfs:///some-path/some-file', 'hdfs', '', '/some-path/some-file')
        check_uri_component_split('/some-path/some-file', '', '', '/some-path/some-file')
        try:
            # Expect exception
            check_uri_component_split('not-a-scheme://bucket/some-path/some-file', '', '', '')
        except GluentDfsException as exc:
            if UNSUPPORTED_URI_SCHEME_EXCEPTION_TEXT in str(exc):
                pass
            else:
                raise
        check_uri_component_split('://bucket/some-path/some-file', '', '', '://bucket/some-path/some-file')

    def _test_write(self):
        self.api.write(self.some_file, 'some-contents-for-a-file')
        self.api.write(self.some_file, 'some-contents-for-a-file', overwrite=True)

    def _run_all_tests(self):
        self._test_chmod()
        self._test_chgrp()
        self._test_container_exists()
        self._test_copy_from_local()
        self._test_copy_to_local()
        self._test_delete()
        self._test_list_dir()
        self._test_list_snapshottable_dirs()
        self._test_list_snapshots()
        self._test_mkdir()
        self._test_read()
        self._test_rename()
        self._test_rmdir()
        self._test_stat()
        self._test_gen_fs_uri()
        self._test_uri_component_split()
        self._test_write()

        self.options = None

###############################################################################
# TestWebHdfs
###############################################################################

class TestWebHdfs(TestGluentDfs):
    """ There's minimal point to these tests because in dry_run mode we don't
        actually call the underlying thirdparty module.
        So really we're just shaking down that there are no syntax errors or
        import problems.
    """

    def __init__(self, *args, **kwargs):
        super(TestWebHdfs, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = 'hdfs://some-path'
        self.some_file = 'hdfs://some-path/a-file'
        self.some_local_file = '/tmp/temp_file.txt'

    def setUp(self):
        messages = OffloadMessages()
        self.api = WebHdfs('a-host', 12345, 'a-user', dry_run=True,
                           messages=messages, do_not_connect=True)

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestCliHdfs
###############################################################################

class TestCliHdfs(TestGluentDfs):

    def __init__(self, *args, **kwargs):
        super(TestCliHdfs, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = 'hdfs://some-path'
        self.some_file = 'hdfs://some-path/a-file'
        self.some_local_file = '/tmp/temp_file.txt'

    def setUp(self):
        messages = OffloadMessages()
        self.api = CliHdfs('a-host', 'a-user', dry_run=True,
                           messages=messages, do_not_connect=True)

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestGluentGcs
###############################################################################

class TestGluentGcs(TestGluentDfs):

    def __init__(self, *args, **kwargs):
        super(TestGluentGcs, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = 'gs://a-bucket/some-path'
        self.some_file = 'gs://a-bucket/some-path/a-file'
        self.some_local_file = '/tmp/temp_file.txt'

    def setUp(self):
        messages = OffloadMessages()
        self.api = GluentGcs(messages, dry_run=True, do_not_connect=True)

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestGluentS3
###############################################################################

class TestGluentS3(TestGluentDfs):

    def __init__(self, *args, **kwargs):
        super(TestGluentS3, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = 's3://a-bucket/some-path'
        self.some_file = 's3://a-bucket/some-path/a-file'
        self.some_local_file = '/tmp/temp_file.txt'

    def setUp(self):
        messages = OffloadMessages()
        self.api = GluentS3(messages, dry_run=True, do_not_connect=True)

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestGluentAzure
###############################################################################

class TestGluentAzure(TestGluentDfs):

    def __init__(self, *args, **kwargs):
        super(TestGluentAzure, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = 'wasb://a-bucket/some-path'
        self.some_file = 'wasb://a-bucket/some-path/a-file'
        self.some_local_file = '/tmp/temp_file.txt'

    def setUp(self):
        messages = OffloadMessages()
        self.api = GluentAzure('an-account-name', 'an-account-key', 'a-domain',
                               messages, dry_run=True, do_not_connect=True)

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestCurrentDfs
###############################################################################

class TestCurrentDfs(TestGluentDfs):

    def __init__(self, *args, **kwargs):
        super(TestCurrentDfs, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = None
        self.some_file = None
        self.some_local_file = '/tmp/temp_file.txt'

    def setUp(self):
        messages = OffloadMessages()
        options = build_current_options()
        self.api = get_dfs_from_options(options, messages, dry_run=True)
        self.options = options
        self.some_dir = self.api.gen_uri(options.offload_fs_scheme, options.offload_fs_container,
                                         options.offload_fs_prefix)
        self.some_file = self.some_dir + '/a-file'

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestCurrentDfsExecuteMode
###############################################################################

class TestCurrentDfsExecuteMode(TestGluentDfs):
    """ This class actually copies files around to prove the API does exactly as it is supposed to
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
        self._local_dir = '/tmp'
        self._file1 = 'dfs-unit-file1.txt'
        self._file2 = 'dfs-unit-file2.txt'
        self._file3 = 'dfs-unit-file3.txt'
        self._file4 = 'dfs-unit-file4.txt'
        self._file5 = 'dfs-unit-file5.txt'
        self._test_files = {self._file1: {'content': 'file 1 contents', 'subdir': None},
                            self._file2: {'content': 'file 2 contents', 'subdir': None},
                            self._file3: {'content': b'file 3 contents', 'subdir': None},
                            self._file4: {'content': 'file 3 contents', 'subdir': 'dir_l1'},
                            self._file5: {'content': 'file 4 contents', 'subdir': 'dir_l1/dir_l2'}}
        self._remote_dir = None

    def _check_dir_has_x_entries(self, remote_path, expected_dir_entries):
        files = self.api.list_dir(remote_path)
        self.assertIsInstance(files, list)
        self.assertEqual(len(files), expected_dir_entries)

    def _create_local_files(self):
        def create_tmp_file(file_name, contents):
            with open(self._local_path(file_name), 'wb' if isinstance(contents, bytes) else 'w') as writer:
                writer.write(contents)
        for file_name in self._test_files:
            create_tmp_file(file_name, self._test_files[file_name]['content'])

    def _delete_local_files(self):
        for file_name in self._test_files:
            os.remove(self._local_path(file_name))

    def _delete_remote_files(self):
        for file_name in self._test_files:
            self.api.delete(self._remote_path(file_name))

    def _local_path(self, file_name):
        return self._local_dir + '/' + file_name

    def _remote_path(self, file_name=None, with_subdir=True):
        path = self._remote_dir
        if file_name:
            if with_subdir and self._test_files[file_name]['subdir']:
                file_name = self._test_files[file_name]['subdir'].rstrip('/') + '/' + file_name
            path = path.rstrip('/') + '/' + file_name
        return path

    def _try_mkdir(self, path):
        try:
            self.api.mkdir(path)
        except NotImplementedError:
            pass

    def setUp(self):
        messages = OffloadMessages()
        options = build_current_options()
        self.api = get_dfs_from_options(options, messages, dry_run=False)
        self._remote_dir = self.api.gen_uri(options.offload_fs_scheme, options.offload_fs_container,
                                            options.offload_fs_prefix).rstrip('/') + '/' + 'unittests' + '/'

    def test_dfs_in_execute_mode(self):
        """ Upload/download/delete files to fully prove the API, other unit tests skim test the API
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
        self.assertFalse(bool(files), 'These files should not be here: {}'.format(files))

        # Upload file1 using copy_from_local()
        self.api.copy_from_local(self._local_path(self._file1), self._remote_path(self._file1), overwrite=False)
        stat = self.api.stat(self._remote_path(self._file1))
        self.assertEqual(stat['type'], DFS_TYPE_FILE)
        # File exists and therefore should fail
        with self.assertRaises(GluentDfsException):
            self.api.copy_from_local(self._local_path(self._file1), self._remote_path(self._file1), overwrite=False)
        # Check we can overwrite
        self.api.copy_from_local(self._local_path(self._file1), self._remote_path(self._file1), overwrite=True)
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file1), as_str=True)
        self.assertEqual(self._test_files[self._file1]['content'], content)

        # Upload file2 using write()
        self.api.write(self._remote_path(self._file2), self._test_files[self._file2]['content'], overwrite=False)
        # File exists and therefore should fail
        with self.assertRaises(GluentDfsException):
            self.api.write(self._remote_path(self._file2), self._test_files[self._file2]['content'], overwrite=False)
        # Check we can overwrite
        self.api.write(self._remote_path(self._file2), self._test_files[self._file2]['content'], overwrite=True)
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file2), as_str=True)
        self.assertEqual(self._test_files[self._file2]['content'], content)

        # Upload file3 (bytes content) using both copy_from_local() and write()
        self.api.copy_from_local(self._local_path(self._file3), self._remote_path(self._file3), overwrite=False)
        stat = self.api.stat(self._remote_path(self._file3))
        self.assertEqual(stat['type'], DFS_TYPE_FILE)
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file3))
        self.assertEqual(self._test_files[self._file3]['content'], content)
        # Overwrite with write()
        self.api.write(self._remote_path(self._file3), self._test_files[self._file3]['content'], overwrite=True)
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file3))
        self.assertEqual(self._test_files[self._file3]['content'], content)

        # Check list_dir has 3 entries
        self._check_dir_has_x_entries(self._remote_path(), 3)

        # Test copy_to_local
        with self.assertRaises(GluentDfsException):
            self.api.copy_to_local(self._remote_path(self._file1), self._local_path(self._file1), overwrite=False)
        self.api.copy_to_local(self._remote_path(self._file1), self._local_path(self._file1), overwrite=True)

        # Upload file4 in a subdirectory
        self._try_mkdir(self._remote_path(self._test_files[self._file4]['subdir'], with_subdir=False))
        self.api.copy_from_local(self._local_path(self._file4), self._remote_path(self._file4), overwrite=False)
        stat = self.api.stat(self._remote_path(self._file4))
        self.assertEqual(stat['type'], DFS_TYPE_FILE)
        stat = self.api.stat(self._remote_path(self._test_files[self._file4]['subdir'], with_subdir=False))
        self.assertEqual(stat['type'], DFS_TYPE_DIRECTORY)
        # Directory also visible with trailing slash
        stat = self.api.stat(self._remote_path(self._test_files[self._file4]['subdir'], with_subdir=False) + '/')
        self.assertEqual(stat['type'], DFS_TYPE_DIRECTORY)
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file4), as_str=True)
        self.assertEqual(self._test_files[self._file4]['content'], content)

        # Check top level list_dir now has 4 entries
        self._check_dir_has_x_entries(self._remote_path(), 4)

        # Upload file4 in a subdirectory of a subdirectory
        self._try_mkdir(self._remote_path(self._test_files[self._file5]['subdir'], with_subdir=False))
        self.api.copy_from_local(self._local_path(self._file5), self._remote_path(self._file5), overwrite=False)
        stat = self.api.stat(self._remote_path(self._file5))
        self.assertEqual(stat['type'], DFS_TYPE_FILE)
        stat = self.api.stat(self._remote_path(self._test_files[self._file5]['subdir'], with_subdir=False))
        self.assertEqual(stat['type'], DFS_TYPE_DIRECTORY)
        # Directory also visible with trailing slash
        stat = self.api.stat(self._remote_path(self._test_files[self._file5]['subdir'], with_subdir=False) + '/')
        self.assertEqual(stat['type'], DFS_TYPE_DIRECTORY)
        # Check file contents are correct
        content = self.api.read(self._remote_path(self._file5), as_str=True)
        self.assertEqual(self._test_files[self._file5]['content'], content)

        # Check top level list_dir still has 4 entries
        self._check_dir_has_x_entries(self._remote_path(), 4)

        # Delete a file
        self.assertIn(self._remote_path(self._file2), self.api.list_dir(self._remote_path()))
        self.api.delete(self._remote_path(self._file2))
        self.assertNotIn(self._remote_path(self._file2), self.api.list_dir(self._remote_path()))
        # Deleting non-existent file is fine
        self.api.delete(self._remote_path(self._file2))

        # Check top level list_dir now has 3 entries
        self._check_dir_has_x_entries(self._remote_path(), 3)

        # Delete a directory and contents
        self.api.delete(self._remote_path(self._test_files[self._file4]['subdir'], with_subdir=False), recursive=True)
        self.assertIsNone(self.api.stat(self._remote_path(self._file4)))
        self.assertIsNone(self.api.stat(self._remote_path(self._file5)))
        # File 1 should still exist
        stat = self.api.stat(self._remote_path(self._file1))
        self.assertEqual(stat['type'], DFS_TYPE_FILE)

        # Tidy up test files
        self._delete_local_files()
        self._delete_remote_files()


if __name__ == '__main__':
    main()
