"""
LICENSE_TEXT
"""
import copy

class KeyValueDatasetSnapshot(object):
  def __init__(self, kv_dict):
    self._kv_dict_original = copy.deepcopy(kv_dict)
    self._kv_updates = {}

  def get(self, key_string):
    return self._kv_updates[key_string] if key_string in self._kv_updates else  self._kv_dict_original[key_string]

  def get_optional(self, key_string, default_value):
    return self._kv_updates[key_string] if key_string in self._kv_updates else  self._kv_dict_original.get(key_string, default_value)

  def update(self, key_string, value):
    self._kv_updates[key_string] = value

  @property
  def updates_dict(self):
    return copy.deepcopy(self._kv_updates)

  @property
  def full_dataset_dict(self):
    res = copy.deepcopy(self._kv_dict_original)
    res.update(self.updates_dict)
    return res


class SerializationError(Exception):
  pass

class KeyValueDatasetSpecification(object):
  def __init__(self, required_fields=[],
                     optional_fields=[],
                     field_names_to_deserializers={},
                     field_names_to_serializers={}):

    self._required_fields = frozenset(required_fields)
    self._optional_fields = frozenset(optional_fields)

    def get_deserializer_func(fieldname, deserializer):
      def do_deserialize(value_string):
        try:
          return deserializer(value_string)
        except Exception as e:
          raise SerializationError('error deserializing value, "{}", for field, {}: {}'.format(
              value_string, fieldname, e))
      return do_deserialize

    def get_serializer_func(fieldname, serializer):
      def do_serialize(value):
        try:
          return serializer(value)
        except Exception as e:
          raise SerializationError('error serializing value, {}, for field, {}: {}'.format(
              value, fieldname, e))
      return do_serialize

    self._field_deserializers = {}
    for fieldname in self.required_fields.union(self.optional_fields):
      deserializer = field_names_to_deserializers.get(fieldname, lambda _: _)
      self._field_deserializers[fieldname] = get_deserializer_func(fieldname, deserializer)

    self._field_serializers = {}
    for fieldname in self.required_fields.union(self.optional_fields):
      serializer = field_names_to_serializers.get(fieldname, str)
      self._field_serializers[fieldname] = get_serializer_func(fieldname, serializer)

  @property
  def required_fields(self):
    return self._required_fields

  @property
  def optional_fields(self):
    return self._optional_fields

  @property
  def all_fields(self):
    return self.required_fields.union(self.optional_fields)

  def check_for_missing_fields(self, kv_dict, error_msg_prefix):
    missing = self.required_fields - set(kv_dict.keys())
    assert not missing, '{} -- missing keys: ({})'.format(error_msg_prefix, ', '.join(missing))
 
  def check_for_unrecognized_fields(self, kv_dict, error_msg_prefix):
    unrecognized = set(kv_dict.keys()) - self.all_fields
    assert not unrecognized, '{} -- unrecognized keys: ({})'.format(error_msg_prefix, ', '.join(unrecognized))

  def deserialize(self, key_string, value_string):
    return self._field_deserializers[key_string](value_string)

  def serialize(self, key_string, value):
    return self._field_serializers[key_string](value)


class KeyValueDatasetPersistence(object):
  def __init__(self, key_value_store, key_value_dataset_spec, dataset_snapshot_class):
    self.key_value_store = key_value_store
    self.key_value_dataset_spec = key_value_dataset_spec
    self.dataset_snapshot_class = dataset_snapshot_class
    self.key_value_store.initialize()

  def run_gc(self):
    self.key_value_store.run_gc()

  def load_dataset_snapshot(self):
    kv_string_dict = self.key_value_store.get_all(self.key_value_dataset_spec.all_fields)
    kv_dict = {k: self.key_value_dataset_spec.deserialize(k, v) for k,v in kv_string_dict.items() if v is not None}
    self.key_value_dataset_spec.check_for_missing_fields(kv_dict, 'error loading dataset')
    return self.dataset_snapshot_class(kv_dict)

  def persist_updates(self, kv_dataset_snapshot):
    updates_dict = kv_dataset_snapshot.updates_dict
    self.key_value_dataset_spec.check_for_unrecognized_fields(updates_dict, 'error persisting updates')
    kv_serialized = { k: self.key_value_dataset_spec.serialize(k, v) for k, v in updates_dict.items() }
    self.key_value_store.put_all(kv_serialized)

  def persist_full_dataset_snapshot(self, kv_dataset_snapshot):
    full_dataset_dict = kv_dataset_snapshot.full_dataset_dict
    self.key_value_dataset_spec.check_for_missing_fields(full_dataset_dict, 'error persisting full dataset')
    self.key_value_dataset_spec.check_for_unrecognized_fields(full_dataset_dict, 'error persisting full dataset')
    kv_serialized = { k: self.key_value_dataset_spec.serialize(k, v) for k, v in full_dataset_dict.items() }
    self.key_value_store.put_all(kv_serialized)
