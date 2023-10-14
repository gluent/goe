#! /usr/bin/env python3
""" Test module for gluentlib.persistence.hdfs_filepath_key_value_store
"""

from teamcity import is_running_under_teamcity
from teamcity.unittestpy import TeamcityTestRunner

import os
import unittest

from gluentlib.persistence.hdfs_filepath_key_value_store import HdfsFilepathKeyValueStore, HdfsFilepathKeyValueStoreEntry

class TestHdfsFilepathKeyValueStore(unittest.TestCase):

  def _check_expected_values_for_kv_store(self, kv_store, expected_values_dict):
    self.assertEqual(expected_values_dict, kv_store.get_all(list(expected_values_dict.keys())))
    for k, v in expected_values_dict.items():
      self.assertEqual(v, kv_store.get(k))

  def test_single_put(self):
    self._do_test_single_put()

  def _do_test_single_put(self, client_factory=None):
    client_factory = client_factory or get_mock_client_factory_for_basic_tests()
    # test initial conditions
    self._do_test_single_get(client_factory=client_factory)
    self._do_test_multi_get(client_factory=client_factory)

    foo_bar_kv_store = HdfsFilepathKeyValueStore('/foo/bar/', client_factory)
    foo_baz_kv_store = HdfsFilepathKeyValueStore('/foo/baz/', client_factory)

    self.assertIsNone(foo_bar_kv_store.get('baz')) # 'baz' is initially undefined
    foo_bar_kv_store.put('baz', 'bam')
    foo_bar_kv_store.put('foo', 'foo/bar foo-NEW')
    foo_bar_kv_store.put('bar', 'foo/bar bar-NEW')
    foo_bar_expected = {
      'baz': 'bam',
      'foo': 'foo/bar foo-NEW',
      'bar': 'foo/bar bar-NEW',
    }
    self._check_expected_values_for_kv_store(foo_bar_kv_store, foo_bar_expected)
    foo_bar_kv_store.purge_old_entries()
    self._check_expected_values_for_kv_store(foo_bar_kv_store, foo_bar_expected)

    self.assertIsNone(foo_baz_kv_store.get('baz')) # 'baz' is initially undefined
    foo_baz_kv_store.put('baz', 'bam')
    foo_baz_kv_store.put('foo', 'foo/baz foo-NEW')
    foo_baz_kv_store.put('bar', 'foo/baz bar-NEW')
    foo_baz_expected = {
      'baz': 'bam',
      'foo': 'foo/baz foo-NEW',
      'bar': 'foo/baz bar-NEW',
    }
    self._check_expected_values_for_kv_store(foo_baz_kv_store, foo_baz_expected)
    foo_baz_kv_store.purge_old_entries()
    self._check_expected_values_for_kv_store(foo_baz_kv_store, foo_baz_expected)

  def test_multi_put(self):
    self._do_test_multi_put()

  def _do_test_multi_put(self, client_factory=None):
    client_factory = client_factory or get_mock_client_factory_for_basic_tests()
    # test initial conditions
    self._do_test_single_get(client_factory=client_factory)
    self._do_test_multi_get(client_factory=client_factory)
 
    foo_bar_kv_store = HdfsFilepathKeyValueStore('/foo/bar/', client_factory)
    foo_baz_kv_store = HdfsFilepathKeyValueStore('/foo/baz/', client_factory)

    self.assertIsNone(foo_bar_kv_store.get('baz')) # 'baz' is initially undefined 
    foo_bar_kv_store.put_all({ 'baz': 'bam', 'foo': 'foo/bar foo-NEW', 'bar': 'foo/bar bar-NEW' })
    foo_bar_expected = {
      'baz': 'bam',
      'foo': 'foo/bar foo-NEW',
      'bar': 'foo/bar bar-NEW',
    }
    self._check_expected_values_for_kv_store(foo_bar_kv_store, foo_bar_expected)
    foo_bar_kv_store.purge_old_entries()
    self._check_expected_values_for_kv_store(foo_bar_kv_store, foo_bar_expected)

    self.assertIsNone(foo_baz_kv_store.get('baz')) # 'baz' is initially undefined
    foo_baz_kv_store.put_all({ 'baz': 'bam', 'foo': 'foo/baz foo-NEW', 'bar': 'foo/baz bar-NEW' })
    foo_baz_expected = {
      'baz': 'bam',
      'foo': 'foo/baz foo-NEW',
      'bar': 'foo/baz bar-NEW',
    }
    self._check_expected_values_for_kv_store(foo_baz_kv_store, foo_baz_expected)
    foo_baz_kv_store.purge_old_entries()
    self._check_expected_values_for_kv_store(foo_baz_kv_store, foo_baz_expected)

  def test_purge_old_entries(self):
    self._do_test_purge_old_entries()

  def _do_test_purge_old_entries(self, client_factory=None):
    client_factory = client_factory or get_mock_client_factory_for_basic_tests()
    # test initial conditions
    self._do_test_single_get(client_factory=client_factory)
    self._do_test_multi_get(client_factory=client_factory)

    foo_bar_kv_store = HdfsFilepathKeyValueStore('/foo/bar/', client_factory)
    foo_baz_kv_store = HdfsFilepathKeyValueStore('/foo/baz/', client_factory)
    foo_bar_kv_store.purge_old_entries()
    foo_baz_kv_store.purge_old_entries()

    # after purge, 'get' and 'get_all' should still exhibit the same behavior
    # (since we haven't added any new entries -- just deleted old ones)

    self._do_test_single_get(client_factory=client_factory)
    self._do_test_multi_get(client_factory=client_factory)

  def test_single_get(self):
    self._do_test_single_get()

  def _do_test_single_get(self, client_factory=None):
    client_factory = client_factory or get_mock_client_factory_for_basic_tests()
    foo_bar_kv_store = HdfsFilepathKeyValueStore('/foo/bar/', client_factory)
    foo_baz_kv_store = HdfsFilepathKeyValueStore('/foo/baz/', client_factory)

    self.assertEqual('foo-2', foo_bar_kv_store.get('foo'))
    self.assertEqual('bar-2', foo_bar_kv_store.get('bar'))

    self.assertEqual('foo-5', foo_baz_kv_store.get('foo'))
    self.assertEqual('bar-7', foo_baz_kv_store.get('bar'))

  def test_multi_get(self):
    self._do_test_multi_get()

  def _do_test_multi_get(self, client_factory=None):
    client_factory = client_factory or get_mock_client_factory_for_basic_tests()
    foo_bar_kv_store = HdfsFilepathKeyValueStore('/foo/bar/', client_factory)
    foo_baz_kv_store = HdfsFilepathKeyValueStore('/foo/baz/', client_factory)

    foo_bar_expected = {
      'foo': 'foo-2',
      'bar': 'bar-2',
    }
    self.assertEqual(foo_bar_expected, foo_bar_kv_store.get_all(['foo', 'bar']))

    foo_baz_expected = {
      'foo': 'foo-5',
      'bar': 'bar-7',
    }
    self.assertEqual(foo_baz_expected, foo_baz_kv_store.get_all(['foo', 'bar']))

  def test_initialize(self):
    client = get_mock_client_factory_for_basic_tests()()
    client_factory = lambda: client
    foo_bar_kv_store = HdfsFilepathKeyValueStore('/foo/bar/', client_factory)
    foo_baz_kv_store = HdfsFilepathKeyValueStore('/foo/baz/', client_factory)
 
    foo_bar_kv_store.initialize()
    self.assertTrue(client.was_mkdir_called_for_dirpath('/foo/bar/'))

    foo_baz_kv_store.initialize()
    self.assertTrue(client.was_mkdir_called_for_dirpath('/foo/baz/'))

class MockHdfsClient(object):
  def __init__(self, paths_to_listings):
    self._paths_to_listings = { os.path.normpath(path): sorted(map(os.path.normpath, listings)) for path, listings in paths_to_listings.items() }
    self._mkdir_dirpaths = []

  def check_path_exists_and_get_normalized(self, path):
    npath = os.path.normpath(path)
    if npath not in self._paths_to_listings:
      raise Exception("Not a directory: '{}'".format(npath))
    return npath

  def list_dir(self, path):
    npath = self.check_path_exists_and_get_normalized(path)
    return self._paths_to_listings[npath]

  def mkdir(self, dirpath):
    self._mkdir_dirpaths.append(dirpath)

  def was_mkdir_called_for_dirpath(self, dirpath):
    return dirpath in self._mkdir_dirpaths

  def write(self, path, data):
    base_dir, filename = os.path.split(path)
    base_dir_npath = self.check_path_exists_and_get_normalized(base_dir)
    existing_listings = set(self._paths_to_listings[base_dir_npath])
    # TODO: does this make sense? i.e. would our 'real' hdfs client silently
    #       overwrite an existing file, or puke?
    existing_listings.add(path)
    self._paths_to_listings[base_dir_npath] = sorted(list(existing_listings))
 
  def delete(self, path):
    base_dir, filename = os.path.split(path)
    base_dir_npath = self.check_path_exists_and_get_normalized(base_dir)
    existing_listings = set(self._paths_to_listings[base_dir_npath])
    existing_listings.remove(path)
    self._paths_to_listings[base_dir_npath] = sorted(list(existing_listings))

# path_to_listing_dict e.g.: [ '/foo/bar': [ {'seqno': 1, 'key_string': 'foo', 'value_string': 'bar' } ]]
def get_mock_hdfs_client(path_to_listing_dict_list):
  paths_to_listings = {}
  for path, listing_dict_list in path_to_listing_dict_list.items():
    listings = [HdfsFilepathKeyValueStoreEntry(**_).encode(path) for _ in listing_dict_list]
    paths_to_listings[path] = listings

  return MockHdfsClient(paths_to_listings)

def get_mock_client_factory_for_basic_tests():
  foo_bar_initial_listings = [
    { 's': 0, 'k': 'foo', 'v': 'foo-0' },
    { 's': 1, 'k': 'foo', 'v': 'foo-1' },
    { 's': 2, 'k': 'foo', 'v': 'foo-2' },
    { 's': 0, 'k': 'bar', 'v': 'bar-1' },
    { 's': 1, 'k': 'bar', 'v': 'bar-2' },
  ]
  foo_baz_initial_listings = [
    { 's': 3, 'k': 'foo', 'v': 'foo-3' },
    { 's': 4, 'k': 'foo', 'v': 'foo-4' },
    { 's': 5, 'k': 'foo', 'v': 'foo-5' },
    { 's': 2, 'k': 'bar', 'v': 'bar-6' },
    { 's': 3, 'k': 'bar', 'v': 'bar-7' },
  ]
  paths_to_listings = {
      '/foo/bar': foo_bar_initial_listings,
      '/foo/baz': foo_baz_initial_listings,
  }

  mock_client = get_mock_hdfs_client(paths_to_listings)
  return lambda: mock_client

if __name__ == '__main__':
    if is_running_under_teamcity():
        runner = TeamcityTestRunner()
    else:
        runner = unittest.TextTestRunner()
    unittest.main(testRunner=runner)
