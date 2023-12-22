"""
LICENSE_TEXT
"""


class KeyValueStore(object):
    def initialize(self):
        """do any work that is required before a key-value store instance can
        be used (e.g. create a directory/file, a database table, etc)"""
        pass

    def run_gc(self):
        """run any applicable garbage collection for a given logical key-value store"""
        pass

    def get(self, key_string):
        """return the persisted, string-serialized value associated with the given key,
        raising an exception if no such mapping is found"""
        raise NotImplementedError("KeyValueStore#get")

    def put(self, key_string, value_string):
        """persist a mapping from the given key to the given string-serialized value,
        overwriting any pre-existing mappings for the specified key"""
        raise NotImplementedError("KeyValueStore#put")

    def get_all(self, key_string_list):
        """return a dictionary of { key_string: value_string }, containing each key in
        the given list of key strings. keys for which no values are found will be
        mapped to None."""
        raise NotImplementedError("KeyValueStore#get_all")

    def put_all(self, key_string_to_value_string_map):
        """persist all of the given key_string/value_string mappings"""
        raise NotImplementedError("KeyValueStore#put_all")


HDFS_FILEPATH_FLAVOR = "hdfs_filepath"
GLUENT_REPO_FLAVOR = "gluent_repo"
# PICKLE_FLAVOR is only for test purposes (not Monster Munch)
PICKLE_FLAVOR = "pickle_flavor"
SUPPORTED_KV_STORE_FLAVORS = [HDFS_FILEPATH_FLAVOR, GLUENT_REPO_FLAVOR]


def build_kv_store(
    kv_store_flavor,
    # all remaining arguments are flavor-specific, so may or may not be required
    # depending on which flavor is being constructed
    location_for_dataset=None,
    hdfs_client_factory=None,
    db=None,
    table_name=None,
    orchestration_config=None,
    messages=None,
    dry_run=False,
    repo_client=None,
    execution_id=None,
):
    assert (
        kv_store_flavor in SUPPORTED_KV_STORE_FLAVORS
    ), "'{}' is not a supported key value store method (supported methods are: {})".format(
        kv_store_flavor, ", ".join(SUPPORTED_KV_STORE_FLAVORS)
    )

    if kv_store_flavor == HDFS_FILEPATH_FLAVOR:
        from .hdfs_filepath_key_value_store import HdfsFilepathKeyValueStore

        assert location_for_dataset, "{} kv store requires location_for_dataset".format(
            kv_store_flavor
        )
        assert hdfs_client_factory, "{} kv store requires hdfs_client_factory".format(
            kv_store_flavor
        )
        return HdfsFilepathKeyValueStore(
            location_for_dataset, hdfs_client_factory, dry_run=dry_run
        )
    elif kv_store_flavor == PICKLE_FLAVOR:
        from .pickle_key_value_store import PickleKeyValueStore

        return PickleKeyValueStore(location_for_dataset, dry_run=dry_run)
