#! /usr/bin/env python3
""" Test module for gluentlib.persistence.key_value_dataset.py
"""

import json
import unittest

from goe.persistence.key_value_dataset import (
    KeyValueDatasetSnapshot,
    KeyValueDatasetSpecification,
    KeyValueDatasetPersistence,
    SerializationError,
)


class TestKeyValueDataset(unittest.TestCase):
    #####################################
    # tests for KeyValueDatasetSnapshot #
    #####################################

    def test_get_required_field_present(self):
        kv_snapshot = KeyValueDatasetSnapshot({"one_int": 1, "one_string": "1"})

        self.assertEqual(kv_snapshot.get("one_int"), 1)
        self.assertEqual(kv_snapshot.get("one_string"), "1")

    def test_get_required_field_missing(self):
        kv_snapshot = KeyValueDatasetSnapshot({})
        with self.assertRaises(KeyError):
            kv_snapshot.get("foo")

    def test_get_optional_field_present(self):
        kv_snapshot = KeyValueDatasetSnapshot({"one_int": 1, "one_string": "1"})

        self.assertEqual(kv_snapshot.get_optional("one_int", None), 1)
        self.assertEqual(kv_snapshot.get_optional("one_string", None), "1")

    def test_get_optional_field_missing(self):
        kv_snapshot = KeyValueDatasetSnapshot({"one_int": 1, "one_string": "1"})

        self.assertEqual(kv_snapshot.get_optional("asdfasdfsdf", None), None)

    def test_update_nonexistent_field(self):
        kv_snapshot = KeyValueDatasetSnapshot({"foo": "bar"})

        # assert initial conditions
        self.assertEqual(kv_snapshot.get("foo"), "bar")
        self.assertEqual(kv_snapshot.get_optional("baz", None), None)

        kv_snapshot.update("baz", "bam")
        self.assertEqual(kv_snapshot.get_optional("baz", None), "bam")
        self.assertEqual(kv_snapshot.get("baz"), "bam")
        # assert that bindings for other fields are unaffected
        self.assertEqual(kv_snapshot.get("foo"), "bar")

    def test_update_overrides_original(self):
        kv_snapshot = KeyValueDatasetSnapshot({"key": "original-value"})

        self.assertEqual(kv_snapshot.get("key"), "original-value")

        kv_snapshot.update("key", "new-value")
        self.assertEqual(kv_snapshot.get("key"), "new-value")

    def test_get_updates_dict(self):
        kv_snapshot = KeyValueDatasetSnapshot({"k1": "k1-original", "k2": "k2-value"})

        self.assertEqual(kv_snapshot.updates_dict, {})
        kv_snapshot.update("k1", "k1-new")
        self.assertEqual(kv_snapshot.updates_dict, {"k1": "k1-new"})
        kv_snapshot.update("k3", "k3-value")
        self.assertEqual(kv_snapshot.updates_dict, {"k1": "k1-new", "k3": "k3-value"})
        kv_snapshot.update("k1", "k1-newer")
        self.assertEqual(kv_snapshot.updates_dict, {"k1": "k1-newer", "k3": "k3-value"})

    def test_get_full_dataset_dict(self):
        kv_snapshot = KeyValueDatasetSnapshot({"k1": "k1-original", "k2": "k2-value"})

        self.assertEqual(
            kv_snapshot.full_dataset_dict, {"k1": "k1-original", "k2": "k2-value"}
        )
        kv_snapshot.update("k1", "k1-new")
        self.assertEqual(
            kv_snapshot.full_dataset_dict, {"k1": "k1-new", "k2": "k2-value"}
        )
        kv_snapshot.update("k3", "k3-value")
        self.assertEqual(
            kv_snapshot.full_dataset_dict,
            {"k1": "k1-new", "k2": "k2-value", "k3": "k3-value"},
        )
        kv_snapshot.update("k1", "k1-newer")
        self.assertEqual(
            kv_snapshot.full_dataset_dict,
            {"k1": "k1-newer", "k2": "k2-value", "k3": "k3-value"},
        )

    def test_deep_copies(self):
        original_dict = {"k1": ["k1-original"], "k2": ["k2-original"]}
        kv_snapshot = KeyValueDatasetSnapshot(original_dict)

        original_dict["k1"].extend(("some", "new", "values"))

        self.assertEqual(kv_snapshot.get("k1"), ["k1-original"])
        self.assertEqual(
            kv_snapshot.full_dataset_dict,
            {"k1": ["k1-original"], "k2": ["k2-original"]},
        )

        kv_snapshot.update("k1", ["k1-new"])
        updates_dict = kv_snapshot.updates_dict
        self.assertEqual(updates_dict, {"k1": ["k1-new"]})
        updates_dict["k1"].extend(("some", "more", "new", "values"))

        self.assertEqual(kv_snapshot.get("k1"), ["k1-new"])
        self.assertEqual(kv_snapshot.updates_dict, {"k1": ["k1-new"]})
        self.assertEqual(
            kv_snapshot.full_dataset_dict, {"k1": ["k1-new"], "k2": ["k2-original"]}
        )

    ##########################################
    # tests for KeyValueDatasetSpecification #
    ##########################################

    def test_kv_datset_spec_constructor_fields(self):
        required_fields = ["req1", "req2"]
        optional_fields = ["opt1", "opt2"]

        kvd_spec = KeyValueDatasetSpecification(
            required_fields=required_fields, optional_fields=optional_fields
        )
        self.assertEqual(kvd_spec.required_fields, set(required_fields))
        self.assertEqual(kvd_spec.optional_fields, set(optional_fields))
        self.assertEqual(
            kvd_spec.all_fields, set(required_fields).union(optional_fields)
        )

        kvd_spec_no_required = KeyValueDatasetSpecification(
            optional_fields=optional_fields
        )
        self.assertEqual(kvd_spec_no_required.required_fields, set())
        self.assertEqual(kvd_spec_no_required.optional_fields, set(optional_fields))
        self.assertEqual(kvd_spec_no_required.all_fields, set(optional_fields))

        kvd_spec_no_optional = KeyValueDatasetSpecification(
            required_fields=required_fields
        )
        self.assertEqual(kvd_spec_no_optional.required_fields, set(required_fields))
        self.assertEqual(kvd_spec_no_optional.optional_fields, set())
        self.assertEqual(kvd_spec_no_optional.all_fields, set(required_fields))

    def test_kv_dataset_check_for_missing_fields(self):
        required_fields = ["req1", "req2"]
        optional_fields = ["opt1", "opt2"]

        kvd_spec = KeyValueDatasetSpecification(
            required_fields=required_fields, optional_fields=optional_fields
        )
        kvd_spec.check_for_missing_fields(
            {
                "req1": "present",
                "req2": "present",
                "opt1": "present",
                "opt2": "present",
            },
            "",
        )
        kvd_spec.check_for_missing_fields({"req1": "present", "req2": "present"}, "")

        with self.assertRaises(AssertionError) as cm:
            kvd_spec.check_for_missing_fields({}, "arbitrary test prefix")

        self.assertTrue(
            str(cm.exception).startswith("arbitrary test prefix"),
            msg="unexpected exception message: '{}'".format(str(cm.exception)),
        )

        with self.assertRaises(AssertionError) as cm:
            kvd_spec.check_for_missing_fields(
                {"req1": "present", "opt1": "present", "opt2": "present"},
                "arbitrary test prefix",
            )

        self.assertTrue(
            str(cm.exception).startswith("arbitrary test prefix"),
            msg="unexpected exception message: '{}'".format(str(cm.exception)),
        )

    def test_kv_dataset_check_for_unrecognized_fields(self):
        required_fields = ["req1", "req2"]
        optional_fields = ["opt1", "opt2"]

        kvd_spec = KeyValueDatasetSpecification(
            required_fields=required_fields, optional_fields=optional_fields
        )
        kvd_spec.check_for_unrecognized_fields({}, "")
        kvd_spec.check_for_unrecognized_fields(
            {"req1": "present", "req2": "present"}, ""
        )
        kvd_spec.check_for_unrecognized_fields(
            {"opt1": "present", "opt2": "present"}, ""
        )
        kvd_spec.check_for_unrecognized_fields(
            {
                "req1": "present",
                "req2": "present",
                "opt1": "present",
                "opt2": "present",
            },
            "",
        )
        with self.assertRaises(AssertionError) as cm:
            kvd_spec.check_for_unrecognized_fields(
                {"invalid-field": "present"}, "arbitrary test prefix"
            )

        self.assertTrue(
            str(cm.exception).startswith("arbitrary test prefix"),
            msg="unexpected exception message: '{}'".format(str(cm.exception)),
        )

        with self.assertRaises(AssertionError) as cm:
            kvd_spec.check_for_unrecognized_fields(
                {
                    "req1": "present",
                    "req2": "present",
                    "opt1": "present",
                    "opt2": "present",
                    "invalid-field": "present",
                },
                "arbitrary test prefix",
            )

        self.assertTrue(
            str(cm.exception).startswith("arbitrary test prefix"),
            msg="unexpected exception message: '{}'".format(str(cm.exception)),
        )

    def test_kv_dataset_spec_serialization(self):
        required_fields = ["req_string_field", "req_int_field", "req_float_field"]
        optional_fields = ["opt_list_field", "opt_dict_field"]

        kvd_spec = KeyValueDatasetSpecification(
            required_fields=required_fields,
            optional_fields=optional_fields,
            field_names_to_deserializers={
                "req_int_field": int,
                "req_float_field": float,
                "opt_list_field": json.loads,
                "opt_dict_field": json.loads,
            },
            field_names_to_serializers={
                "opt_list_field": json.dumps,
                "opt_dict_field": json.dumps,
            },
        )

        # first, test SERIALIZATION

        # since no serializer function is explicitly provided for 'req_string_field', the default will be used: str
        self.assertEqual(kvd_spec.serialize("req_string_field", "a_string"), "a_string")
        self.assertEqual(kvd_spec.serialize("req_int_field", 666), "666")
        self.assertEqual(
            kvd_spec.serialize("req_float_field", 666.789987), "666.789987"
        )

        list_val = ["foo", "bar", 1, 2]
        list_val_json_serialized = json.dumps(list_val)
        self.assertEqual(
            kvd_spec.serialize("opt_list_field", list_val), list_val_json_serialized
        )

        dict_val = {"s": "string", "l": [1, 2], "d": {"k": "v"}}
        dict_val_json_serialized = json.dumps(dict_val)
        self.assertEqual(
            kvd_spec.serialize("opt_dict_field", dict_val), dict_val_json_serialized
        )

        # next, test DESERIALIZATION
        self.assertEqual(
            kvd_spec.deserialize("req_string_field", "a_string"), "a_string"
        )
        self.assertEqual(kvd_spec.deserialize("req_int_field", "666"), 666)
        self.assertEqual(
            kvd_spec.deserialize("req_float_field", "666.789987"), 666.789987
        )

        self.assertEqual(
            kvd_spec.deserialize("opt_list_field", list_val_json_serialized), list_val
        )
        self.assertEqual(
            kvd_spec.deserialize("opt_dict_field", dict_val_json_serialized), dict_val
        )

    def test_kv_dataset_spec_serialization_errors(self):
        required_fields = ["dict_field", "int_field"]

        kvd_spec = KeyValueDatasetSpecification(
            required_fields=required_fields,
            field_names_to_deserializers={"dict_field": json.loads, "int_field": int},
            field_names_to_serializers={"dict_field": json.dumps},
        )

        with self.assertRaises(SerializationError):
            # python sets are not JSON-serializable
            kvd_spec.serialize("dict_field", set())

        with self.assertRaises(SerializationError):
            kvd_spec.deserialize(
                "dict_field", "this statement is neither true nor valid json"
            )

        with self.assertRaises(SerializationError):
            kvd_spec.deserialize("int_field", "123.456")

        with self.assertRaises(SerializationError):
            kvd_spec.deserialize("int_field", "clearly not an int")

    ########################################
    # tests for KeyValueDatasetPersistence #
    ########################################

    def test_initialize_and_run_gc(self):
        mock_kv_store = MockKeyValueStore()
        kvdp = KeyValueDatasetPersistence(
            mock_kv_store, YET_ANOTHER_DATASET_SPEC, YetAnotherSnapshotClass
        )

        self.assertTrue(
            mock_kv_store.initialized,
            msg="KeyValueStore for KeyValueDatasetPersistence was not initialized",
        )

        self.assertEqual(
            mock_kv_store.run_gc_count,
            0,
            msg="MockKeyValueStore should start out with a run_gc_count of 0...",
        )
        kvdp.run_gc()
        self.assertEqual(
            mock_kv_store.run_gc_count,
            1,
            msg="KeyValueStore#run_gc was NOT called when KeyValueDatasetPersistence#run_gc was called...",
        )

    def test_load_dataset_snapshot(self):
        required_values_dict = {
            INT_FIELD: 123,
            FLOAT_FIELD: -123.456,
            STRING_FIELD: "foo",
        }
        required_values_serialized_dict = {
            INT_FIELD: "123",
            FLOAT_FIELD: "-123.456",
            STRING_FIELD: "foo",
        }
        mock_kv_store = MockKeyValueStore(required_values_serialized_dict)
        kvdp = KeyValueDatasetPersistence(
            mock_kv_store, YET_ANOTHER_DATASET_SPEC, YetAnotherSnapshotClass
        )

        expected_snapshot = YetAnotherSnapshotClass(required_values_dict)
        snapshot = kvdp.load_dataset_snapshot()
        self.assertEqual(snapshot, expected_snapshot)

        optional_values_dict = {LIST_FIELD: [1, "two", [3]], DICT_FIELD: {"bar": "baz"}}
        optional_values_serialized_dict = {
            LIST_FIELD: json.dumps([1, "two", [3]]),
            DICT_FIELD: json.dumps({"bar": "baz"}),
        }

        all_values_dict = optional_values_dict
        all_values_dict.update(required_values_dict)
        all_values_serialized_dict = optional_values_serialized_dict
        all_values_serialized_dict.update(required_values_serialized_dict)

        mock_kv_store = MockKeyValueStore(all_values_serialized_dict)
        kvdp = KeyValueDatasetPersistence(
            mock_kv_store, YET_ANOTHER_DATASET_SPEC, YetAnotherSnapshotClass
        )

        expected_snapshot = YetAnotherSnapshotClass(all_values_dict)
        snapshot = kvdp.load_dataset_snapshot()
        self.assertEqual(snapshot, expected_snapshot)

    def test_load_dataset_snapshot_empty_kv_store(self):
        mock_kv_store = MockKeyValueStore()
        kvdp = KeyValueDatasetPersistence(
            mock_kv_store, YET_ANOTHER_DATASET_SPEC, YetAnotherSnapshotClass
        )

        with self.assertRaises(AssertionError) as cm:
            kvdp.load_dataset_snapshot()

        # NOTE: this test is a wee bit brittle...the error message prefix is just copied over from the source of
        #       KeyValueDatasetPersistence#load_dataset_snapshot (and thus, this test will fail if that ever changes)
        self.assertTrue(str(cm.exception).startswith("error loading dataset"))

    def test_persist_updates(self):
        required_values_dict = {
            INT_FIELD: 123,
            FLOAT_FIELD: -123.456,
            STRING_FIELD: "foo",
        }
        required_values_serialized_dict = {
            INT_FIELD: "123",
            FLOAT_FIELD: "-123.456",
            STRING_FIELD: "foo",
        }
        mock_kv_store = MockKeyValueStore(required_values_serialized_dict)
        kvdp = KeyValueDatasetPersistence(
            mock_kv_store, YET_ANOTHER_DATASET_SPEC, YetAnotherSnapshotClass
        )

        # check conditions before updates
        expected_snapshot = YetAnotherSnapshotClass(required_values_dict)
        snapshot = kvdp.load_dataset_snapshot()
        self.assertEqual(snapshot, expected_snapshot)

        snapshot.update_int_field(321)
        snapshot.update_float_field(654.321)
        snapshot.update_string_field("bar")

        kvdp.persist_updates(snapshot)
        updated_snapshot = kvdp.load_dataset_snapshot()
        self.assertFalse(
            updated_snapshot is snapshot
        )  # paranoia -- check against trivial equality
        self.assertEqual(updated_snapshot, snapshot)

        self.assertIsNone(snapshot.list_field)
        self.assertIsNone(snapshot.dict_field)

        list_val = ["one", 2, "3"]
        dict_val = {"one": 2, "buckle": "my belt(?)"}
        snapshot.update_list_field(list_val)
        snapshot.update_dict_field(dict_val)

        kvdp.persist_updates(snapshot)
        updated_snapshot = kvdp.load_dataset_snapshot()

        self.assertFalse(
            updated_snapshot is snapshot
        )  # paranoia -- check against trivial equality
        self.assertEqual(updated_snapshot, snapshot)

        self.assertEqual(updated_snapshot.list_field, list_val)
        self.assertEqual(updated_snapshot.dict_field, dict_val)

    def test_persist_full_dataset_snapshot(self):
        values_dict = {
            INT_FIELD: 123,
            FLOAT_FIELD: -123.456,
            STRING_FIELD: "foo",
            LIST_FIELD: [1, "two", [3]],
            DICT_FIELD: {"bar": "baz"},
        }

        snapshot = YetAnotherSnapshotClass(values_dict)

        mock_kv_store = (
            MockKeyValueStore()
        )  # in this case, the mock KV store starts out empty
        kvdp = KeyValueDatasetPersistence(
            mock_kv_store, YET_ANOTHER_DATASET_SPEC, YetAnotherSnapshotClass
        )

        kvdp.persist_full_dataset_snapshot(snapshot)

        loaded_snapshot = kvdp.load_dataset_snapshot()
        self.assertFalse(
            loaded_snapshot is snapshot
        )  # paranoia -- check against trivial equality
        self.assertEqual(snapshot, loaded_snapshot)


INT_FIELD = "int_field"
FLOAT_FIELD = "float_field"
STRING_FIELD = "string_field"
LIST_FIELD = "list_field"
DICT_FIELD = "dict_field"

YET_ANOTHER_DATASET_SPEC = KeyValueDatasetSpecification(
    required_fields=[INT_FIELD, STRING_FIELD, FLOAT_FIELD],
    optional_fields=[LIST_FIELD, DICT_FIELD],
    field_names_to_deserializers={
        INT_FIELD: int,
        FLOAT_FIELD: float,
        LIST_FIELD: lambda _: list(json.loads(_)),
        DICT_FIELD: lambda _: dict(json.loads(_)),
    },
    field_names_to_serializers={LIST_FIELD: json.dumps, DICT_FIELD: json.dumps},
)


class YetAnotherSnapshotClass(KeyValueDatasetSnapshot):
    def __init__(self, config_dict):
        super(YetAnotherSnapshotClass, self).__init__(config_dict)

    @property
    def int_field(self):
        return self.get(INT_FIELD)

    def update_int_field(self, new_int_field_value):
        self.update(INT_FIELD, new_int_field_value)

    @property
    def string_field(self):
        return self.get(STRING_FIELD)

    def update_string_field(self, new_string_field_value):
        self.update(STRING_FIELD, new_string_field_value)

    @property
    def float_field(self):
        return self.get(FLOAT_FIELD)

    def update_float_field(self, new_float_field_value):
        self.update(FLOAT_FIELD, new_float_field_value)

    @property
    def list_field(self):
        return self.get_optional(LIST_FIELD, None)

    def update_list_field(self, new_list_field_value):
        self.update(LIST_FIELD, new_list_field_value)

    @property
    def dict_field(self):
        return self.get_optional(DICT_FIELD, None)

    def update_dict_field(self, new_dict_field_value):
        self.update(DICT_FIELD, new_dict_field_value)

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__)
            and self.full_dataset_dict == other.full_dataset_dict
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class MockKeyValueStore(object):
    def __init__(self, initial_fieldname_to_serialized_values_dict={}):
        self._fieldnames_to_serialized_values_dict = (
            initial_fieldname_to_serialized_values_dict
        )
        self.initialized = False
        self.run_gc_count = 0

    def initialize(self):
        self.initialized = True

    def run_gc(self):
        self.run_gc_count += 1

    def get_all(self, field_list):
        return self._fieldnames_to_serialized_values_dict.copy()

    def put_all(self, serialized_kv_dict):
        self._fieldnames_to_serialized_values_dict.update(serialized_kv_dict)


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    unittest.main(testRunner=runner)
