""" GLUENT_REPO implementation of key-value store
LICENSE_TEXT
"""

from goe.persistence.factory.orchestration_repo_client_factory import orchestration_repo_client_factory
from goe.persistence.key_value_store import KeyValueStore


class GluentRepoKeyValueStore(KeyValueStore):

    def __init__(self, hybrid_owner, hybrid_view, orchestration_config, messages,
                 dry_run=False, repo_client=None, execution_id=None):
        self._hybrid_owner = hybrid_owner
        self._hybrid_view = hybrid_view
        self._orchestration_config = orchestration_config
        self._messages = messages
        self._dry_run = dry_run
        self._execution_id = execution_id
        if repo_client:
            self._repo_client = repo_client
        else:
            self._repo_client = orchestration_repo_client_factory(orchestration_config, messages, dry_run=dry_run)

    def initialize(self):
        pass

    def get(self, key_string):
        kv_dict = self.get_all()
        return kv_dict[key_string]

    def put(self, key_string, value_string):
        kv_dict = self.get_all()
        kv_dict[key_string] = value_string
        self.put_all(kv_dict)

    def get_all(self, key_string_list=None):
        kv_dict = self._repo_client.get_incremental_update_extraction_metadata(self._hybrid_owner, self._hybrid_view)
        if not kv_dict or all(_ is None for _ in kv_dict.values()):
            kv_dict = {}
        if key_string_list:
            return {k: v for k, v in kv_dict.items() if k in key_string_list}
        else:
            return kv_dict

    def put_all(self, key_string_to_value_string_map):
        if not self._dry_run:
            self._repo_client.save_incremental_update_extraction_metadata(
                self._hybrid_owner, self._hybrid_view, key_string_to_value_string_map, self._execution_id
            )

    def run_gc(self):
        pass
