"""
LICENSE_TEXT
"""
import urllib.request, urllib.parse, urllib.error
import urllib.parse

from .key_value_store import KeyValueStore

# NOTE: the hdfs filepath approach has some severe limitations, and is
#       probably only useful for test, dev, and alpha offerings. for
#       instance: the maximum length of an HDFS filepath is likely
#       somewhere in the neighborhood of 255 bytes (though this will
#       vary by deployment)
class HdfsFilepathKeyValueStore(KeyValueStore):
  def __init__(self, hdfs_data_directory, hdfs_client_factory, dry_run=False):
    self.hdfs_data_directory = hdfs_data_directory
    self.hdfs_client_factory = hdfs_client_factory
    self._dry_run = dry_run

  def initialize(self):
    if not self._dry_run:
      self.hdfs_client_factory().mkdir(self.hdfs_data_directory)

  def get(self, key_string):
    max_entry = self._get_max_seqno_entries([key_string])[key_string]
    return max_entry.value_string if max_entry else None

  def put(self, key_string, value_string, seqno=-1):
    if seqno < 0:
      max_entry = self._get_max_seqno_entries([key_string])[key_string]
      seqno = (1 + max_entry.seqno) if max_entry else 0
    entry = HdfsFilepathKeyValueStoreEntry(seqno, key_string, value_string)
    entry_path = entry.encode(self.hdfs_data_directory)
    if not self._dry_run:
      self.hdfs_client_factory().write(entry_path, data='.')

  def get_all(self, key_string_list):
    keys_and_entries = self._get_max_seqno_entries(key_string_list)
    return { k: entry.value_string if entry else None for k, entry in keys_and_entries.items() }

  def put_all(self, key_string_to_value_string_map):
    keys_and_entries = self._get_max_seqno_entries(list(key_string_to_value_string_map.keys()))
    keys_and_seqnos = { k: (1 + entry.seqno) if entry else 0 for k, entry in keys_and_entries.items() }
    for key_string, value_string in key_string_to_value_string_map.items():
      self.put(key_string, value_string, seqno=keys_and_seqnos[key_string])

  def _list_entries(self, entry_filter=lambda _: True):
    dir_listing = self.hdfs_client_factory().list_dir(self.hdfs_data_directory)
    return [ _ for _ in (HdfsFilepathKeyValueStoreEntry.decode(_) for _ in dir_listing) if entry_filter(_) ]

  def _extract_max_seqno_entries_from_entries_list(self, entries_list, keys_to_include=[]):
    max_entries_for_keys = { k: None for k in keys_to_include }
    for entry in entries_list:
      cur_max_for_key = max_entries_for_keys.get(entry.key_string, None)
      if not cur_max_for_key or entry.seqno > cur_max_for_key.seqno:
        max_entries_for_keys[entry.key_string] = entry
    return max_entries_for_keys

  def _get_max_seqno_entries(self, key_string_list=[]):
    key_string_set = set(key_string_list)
    entry_filter = lambda entry: entry.key_string in key_string_set if key_string_set else lambda _: True
    entries = self._list_entries(entry_filter=entry_filter)
    return self._extract_max_seqno_entries_from_entries_list(entries, key_string_list)

  def run_gc(self):
    self.purge_old_entries()

  def purge_old_entries(self):
    all_entries = self._list_entries()
    max_entries = self._extract_max_seqno_entries_from_entries_list(all_entries)
    hdfs_client = self.hdfs_client_factory()
    for entry in all_entries:
      if entry.seqno < max_entries[entry.key_string].seqno:
        if not self._dry_run:
          try:
            hdfs_client.delete(entry.encode(self.hdfs_data_directory))
          except:
            # TODO: log this
            #       (why is this a TODO and not just a logging statement, you ask?
            #        well, there is a more general TODO to add fine-grained dev logging
            #        to all of the peristence/incremental stuff...and, of course, yolo.
            pass

class HdfsFilepathKeyValueStoreEntry(object):
  def __init__(self, s, k, v):
    self._seqno = int(s)
    self._key_string = k
    self._value_string = v

  @property
  def seqno(self):
    return self._seqno

  @property
  def key_string(self):
    return self._key_string

  @property
  def value_string(self):
    return self._value_string

  def encode(self, path_prefix):
    encoded_entry = urllib.parse.urlencode({ 's': self.seqno, 'k': self.key_string, 'v': self.value_string })
    return '{}/{}'.format(path_prefix.rstrip('/'), encoded_entry)

  def __str__(self):
    return "HdfsFilepathKeyValueStoreEntry(s={}, k='{}', v='{}')".format(
        self._seqno, self._key_string, self._value_string)

  @staticmethod
  def decode(full_path):
    # NOTE: see 'encode' above for why this works -- i.e. we explicitly insert a slash before
    #       the encoded entry suffix...and the URL-encoded entry suffix cannot contain any
    #       slashes (thanks to the URL encoding)
    encoded_entry = full_path[full_path.rfind('/') + 1:]
    decoded_dict = urllib.parse.parse_qs(encoded_entry)
    missing = set(['s', 'k', 'v']) - set(decoded_dict.keys())
    assert not missing, 'error decoding kv string -- missing keys: {}'.format(', '.join(missing))
    assert all(len(_) == 1 for _ in decoded_dict.values())
    decoded_dict = { k: v[0] for k, v in decoded_dict.items() }

    return HdfsFilepathKeyValueStoreEntry(**decoded_dict)

