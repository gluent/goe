""" Test implementation of key-value store
    LICENSE_TEXT
"""

import pickle

from goe.persistence.key_value_store import KeyValueStore


class PickleKeyValueStore(KeyValueStore):
    def __init__(self, pickle_path, dry_run=False):
        self._pickle_path = pickle_path
        self._dry_run = dry_run

    def initialize(self):
        pass

    def get(self, key_string):
        kv_dict = self.get_all()
        return kv_dict[key_string]

    def put(self, key_string, value_string, seqno=-1):
        kv_dict = self.get_all()
        kv_dict[key_string] = value_string
        self.put_all(kv_dict)

    def get_all(self, key_string_list=None):
        try:
            with open(self._pickle_path, "rb") as handle:
                kv_dict = pickle.load(handle)
        except FileNotFoundError:
            kv_dict = {}
        if key_string_list:
            return {k: v for k, v in kv_dict.items() if k in key_string_list}
        else:
            return kv_dict

    def put_all(self, key_string_to_value_string_map):
        if not self._dry_run:
            with open(self._pickle_path, "wb") as handle:
                pickle.dump(key_string_to_value_string_map, handle)

    def run_gc(self):
        pass
