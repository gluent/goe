""" TestGOEDfs: Unit test library to test API for all supported backend filesystems.
    This focuses on API calls that do not need to connect to the system.
    Because there is no connection we can fake any backend and test basic functionality.
"""
import logging
from unittest import TestCase, main

from goe.filesystem.cli_hdfs import CliHdfs
from goe.filesystem.goe_azure import GOEAzure
from goe.filesystem.goe_gcs import GOEGcs
from goe.filesystem.goe_s3 import GOES3
from goe.filesystem.web_hdfs import WebHdfs
from goe.filesystem.goe_dfs import (
    GOEDfsException,
    gen_fs_uri,
    uri_component_split,
    DFS_TYPE_DIRECTORY,
    DFS_TYPE_FILE,
    UNSUPPORTED_URI_SCHEME_EXCEPTION_TEXT,
)
from goe.offload.offload_messages import OffloadMessages

###############################################################################
# LOGGING
###############################################################################

logger = logging.getLogger(__name__)
# Disabling logging by default
logger.addHandler(logging.NullHandler())


###############################################################################
# TestGOEDfs
###############################################################################


class TestGOEDfs(TestCase):
    """An intermediate class that defines the individual tests that are
    inherited by the actual backend APIs.
    """

    def __init__(self, *args, **kwargs):
        super(TestGOEDfs, self).__init__(*args, **kwargs)
        self.api = None
        self.options = None
        self.some_dir = None
        self.some_file = None
        self.some_local_file = None

    def _test_chmod(self):
        try:
            self.api.chmod(self.some_dir, "755")
            self.api.chmod(self.some_dir, "g+w")
        except NotImplementedError:
            pass

    def _test_chgrp(self):
        try:
            self.api.chgrp(self.some_dir, "a-group")
        except NotImplementedError:
            pass

    def _test_container_exists(self):
        try:
            self.assertIn(
                self.api.container_exists("some-scheme", "some-container"),
                (True, False),
            )
            if self.options:
                self.assertTrue(
                    self.api.container_exists(
                        self.options.offload_fs_scheme,
                        self.options.offload_fs_container,
                    )
                )
                self.assertFalse(
                    self.api.container_exists(
                        self.options.offload_fs_scheme, "this-is-defo-not-a-container"
                    )
                )
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

    def _test_mkdir(self):
        self.api.mkdir(self.some_dir)

    def _test_read(self):
        self.api.read(self.some_file)

    def _test_rename(self):
        try:
            self.api.rename(self.some_file, self.some_file + "_2")
        except NotImplementedError:
            pass

    def _test_rmdir(self):
        self.api.rmdir(self.some_dir)

    def _test_stat(self):
        stat = self.api.stat(self.some_dir)
        if stat:
            self.assertIsInstance(stat, dict)
            self.assertIn("type", stat)
            self.assertEqual(stat["type"], DFS_TYPE_DIRECTORY)
            self.assertIn("length", stat)
        stat = self.api.stat(self.some_file)
        if stat:
            self.assertIsInstance(stat, dict)
            self.assertIn("type", stat)
            self.assertEqual(stat["type"], DFS_TYPE_FILE)
            self.assertIn("length", stat)

    def _test_gen_fs_uri(self):
        def check_gen_fs_uri(
            expected_uri,
            path_prefix,
            db_path_suffix=None,
            scheme=None,
            container=None,
            backend_db=None,
            table_name=None,
        ):
            uri = gen_fs_uri(
                path_prefix,
                db_path_suffix=db_path_suffix,
                scheme=scheme,
                container=container,
                backend_db=backend_db,
                table_name=table_name,
            )
            self.assertIsInstance(uri, str)
            self.assertEqual(uri, expected_uri)

        check_gen_fs_uri(
            "s3a://bucket/some-path", "some-path", scheme="s3a", container="bucket"
        )
        check_gen_fs_uri(
            "gs://dev-bucket/some-path/dev-name/db_load/my-table",
            "some-path/dev-name",
            scheme="gs",
            container="dev-bucket",
            backend_db="db_load",
            table_name="my-table",
        )
        check_gen_fs_uri(
            "hdfs:///user/goe/db_load.db/my-table",
            "/user/goe",
            ".db",
            scheme="hdfs",
            backend_db="db_load",
            table_name="my-table",
        )
        check_gen_fs_uri(
            "/user/goe/db_load.db/my-table",
            "/user/goe",
            ".db",
            backend_db="db_load",
            table_name="my-table",
        )
        check_gen_fs_uri(
            "wasb://bucket@account.blob.blah/some-path",
            "some-path",
            scheme="wasb",
            container="bucket@account.blob.blah",
        )

    def _test_uri_component_split(self):
        def check_uri_component_split(
            path, expected_scheme, expected_container, expected_path
        ):
            parts = uri_component_split(path)
            self.assertIsInstance(parts, tuple)
            self.assertEqual(len(parts), 3)
            self.assertEqual(parts[0], expected_scheme)
            self.assertEqual(parts[1], expected_container)
            self.assertEqual(parts[2], expected_path)

        check_uri_component_split(
            "s3a://bucket/some-path/some-file", "s3a", "bucket", "/some-path/some-file"
        )
        check_uri_component_split(
            "gs://bucket/some-path/some-file", "gs", "bucket", "/some-path/some-file"
        )
        check_uri_component_split(
            "hdfs://host:123/some-path/some-file",
            "hdfs",
            "host:123",
            "/some-path/some-file",
        )
        check_uri_component_split(
            "hdfs://ha-nameservice/some-path/some-file",
            "hdfs",
            "ha-nameservice",
            "/some-path/some-file",
        )
        check_uri_component_split(
            "hdfs:///some-path/some-file", "hdfs", "", "/some-path/some-file"
        )
        check_uri_component_split(
            "/some-path/some-file", "", "", "/some-path/some-file"
        )
        try:
            # Expect exception
            check_uri_component_split(
                "not-a-scheme://bucket/some-path/some-file", "", "", ""
            )
        except GOEDfsException as exc:
            if UNSUPPORTED_URI_SCHEME_EXCEPTION_TEXT in str(exc):
                pass
            else:
                raise
        check_uri_component_split(
            "://bucket/some-path/some-file", "", "", "://bucket/some-path/some-file"
        )

    def _test_write(self):
        self.api.write(self.some_file, "some-contents-for-a-file")
        self.api.write(self.some_file, "some-contents-for-a-file", overwrite=True)

    def _run_all_tests(self):
        self._test_chmod()
        self._test_chgrp()
        self._test_container_exists()
        self._test_copy_from_local()
        self._test_copy_to_local()
        self._test_delete()
        self._test_list_dir()
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


class TestWebHdfs(TestGOEDfs):
    """There's minimal point to these tests because in dry_run mode we don't
    actually call the underlying thirdparty module.
    So really we're just shaking down that there are no syntax errors or
    import problems.
    """

    def __init__(self, *args, **kwargs):
        super(TestWebHdfs, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = "hdfs://some-path"
        self.some_file = "hdfs://some-path/a-file"
        self.some_local_file = "/tmp/temp_file.txt"

    def setUp(self):
        messages = OffloadMessages()
        self.api = WebHdfs(
            "a-host",
            12345,
            "a-user",
            dry_run=True,
            messages=messages,
            do_not_connect=True,
        )

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestCliHdfs
###############################################################################


class TestCliHdfs(TestGOEDfs):
    def __init__(self, *args, **kwargs):
        super(TestCliHdfs, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = "hdfs://some-path"
        self.some_file = "hdfs://some-path/a-file"
        self.some_local_file = "/tmp/temp_file.txt"

    def setUp(self):
        messages = OffloadMessages()
        self.api = CliHdfs(
            "a-host", "a-user", dry_run=True, messages=messages, do_not_connect=True
        )

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestGOEGcs
###############################################################################


class TestGOEGcs(TestGOEDfs):
    def __init__(self, *args, **kwargs):
        super(TestGOEGcs, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = "gs://a-bucket/some-path"
        self.some_file = "gs://a-bucket/some-path/a-file"
        self.some_local_file = "/tmp/temp_file.txt"

    def setUp(self):
        messages = OffloadMessages()
        self.api = GOEGcs(messages, dry_run=True, do_not_connect=True)

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestGOES3
###############################################################################


class TestGOES3(TestGOEDfs):
    def __init__(self, *args, **kwargs):
        super(TestGOES3, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = "s3://a-bucket/some-path"
        self.some_file = "s3://a-bucket/some-path/a-file"
        self.some_local_file = "/tmp/temp_file.txt"

    def setUp(self):
        messages = OffloadMessages()
        self.api = GOES3(messages, dry_run=True, do_not_connect=True)

    def test_all(self):
        self._run_all_tests()


###############################################################################
# TestGOEAzure
###############################################################################


class TestGOEAzure(TestGOEDfs):
    def __init__(self, *args, **kwargs):
        super(TestGOEAzure, self).__init__(*args, **kwargs)
        self.api = None
        self.some_dir = "wasb://a-bucket/some-path"
        self.some_file = "wasb://a-bucket/some-path/a-file"
        self.some_local_file = "/tmp/temp_file.txt"

    def setUp(self):
        messages = OffloadMessages()
        self.api = GOEAzure(
            "an-account-name",
            "an-account-key",
            "a-domain",
            messages,
            dry_run=True,
            do_not_connect=True,
        )

    def test_all(self):
        self._run_all_tests()


if __name__ == "__main__":
    main()
