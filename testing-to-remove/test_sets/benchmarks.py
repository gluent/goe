from datetime import datetime
import pickle

from goe.gluent import version


class Benchmarks(object):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Benchmarks, cls).__new__(cls)
            cls._instance._benchmark_filename = 'test.bench'
            cls._instance._benchmark = {}
            cls._instance._benchmark_id = None
            cls._instance._checkpoint_id = (version(), datetime.now())
        return cls._instance

    def load(self):
        try:
            self._benchmark = pickle.load(open(self._benchmark_filename, mode='rb'))
        except (IOError, EOFError) as exc:
            pass
        except UnicodeDecodeError as exc:
            # The file contents are str because it was created by Python 2, overwrite it
            pass

        self._benchmark_id = self._benchmark.get('checkpoints', [(None, None)])[0]
        self._benchmark[self._benchmark_id] = self._benchmark.get(self._benchmark_id, {})
        self._benchmark['checkpoints'] = self._benchmark.get('checkpoints', []) + [self._checkpoint_id]
        self._benchmark[self._checkpoint_id] = self._benchmark.get(self._checkpoint_id, {})
        while len(self._benchmark['checkpoints']) > 4:
            del self._benchmark[self._benchmark['checkpoints'][1]]
            self._benchmark['checkpoints'].remove(self._benchmark['checkpoints'][1])

    def set_benchmark(self):
        if 'checkpoints' in self._benchmark:
            while len(self._benchmark['checkpoints']) > 1:
                del self._benchmark[self._benchmark['checkpoints'][0]]
                self._benchmark['checkpoints'].remove(self._benchmark['checkpoints'][0])

    def save(self):
        pickle.dump(self._benchmark, open(self._benchmark_filename, 'wb'))

    def add_result(self, response_key, response_value):
        self._benchmark[self._checkpoint_id][response_key] = response_value

    def get_previous_result(self, response_key):
        return self._benchmark[self._benchmark_id].get(response_key)

    def get_checkpoints(self):
        return self._benchmark['checkpoints']

    def get_result(self, checkpoint, response_key):
        return self._benchmark[checkpoint].get(response_key)

    def get_benchmark_id(self):
        return self._benchmark_id
