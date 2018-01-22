from time import time
from array import array

from .utils import NAN


class Buffer:
    def __init__(self, size, resolution, flush_size, past_size, max_points, now=None):
        self.size = size
        self.resolution = resolution
        self.flush_size = flush_size
        self.past_size = past_size
        self.max_points = max_points

        self.data = {}
        self.new_names = []

        self.empty_row = array('d', [NAN] * size)
        self.past_points = 0
        self.future_points = 0
        self.received_points = 0
        self.flushed_points = 0
        self.last_size = 0

        self.set_ts((now or time()) - self.past_size * resolution)

    def get_data(self, keys):
        result = {}
        for k in keys:
            try:
                result[k] = list(self.data[k])
            except KeyError:
                pass

        return {'start': self.ts,
                'result': result,
                'resolution': self.resolution,
                'size': self.size}

    def cut_data(self, size):
        result = []
        empty_part = array('d', [NAN] * size)
        for k, v in self.data.items():
            result.append((k, v[:size]))
            v[:-size] = v[size:]
            v[-size:] = empty_part[:]
        return result

    def get_row(self, name):
        try:
            return self.data[name]
        except KeyError:
            self.new_names.append(name)
        result = self.data[name] = self.empty_row[:]
        return result

    def set_ts(self, ts):
        self.ts = int(ts) // self.resolution * self.resolution

    def flush(self, size):
        self.flushed_points += len(self.data) * size

        data = self.cut_data(size)
        if data:
            result = (data, self.ts, self.resolution, size)
        else:
            result = None

        self.ts += self.resolution * size
        self.last_size = 0
        self.new_names[:] = []
        return result

    def add(self, ts, name, value, gen_metrics=True):
        self.received_points += 1
        idx = (int(ts) - self.ts) // self.resolution
        try:
            self.get_row(name)[idx] = value  # TODO: optional merge
        except IndexError:
            if idx < 0:
                self.past_points += 1
            else:
                self.future_points += 1

    def tick(self, force=False, now=None):
        now = int(now or time())
        size = (now - self.past_size * self.resolution - self.ts) // self.resolution

        if size < 0:
            return None, None

        if size != self.last_size:
            self.add(now, b'hisser.flushed-points', self.flushed_points, False)
            self.add(now, b'hisser.received-points', self.received_points, False)
            self.add(now, b'hisser.past-points', self.past_points, False)
            self.add(now, b'hisser.future-points', self.future_points, False)
            self.last_size = size

        if size >= self.size:
            return self.flush(self.size), None

        if size >= self.flush_size:
            return self.flush(self.flush_size), None

        if size * len(self.data) > self.max_points:
            return self.flush(size), None

        if force:
            size = (now - self.ts) // self.resolution
            return self.flush(min(size, self.size)), None

        if size > 0 and self.new_names:
            new_names = self.new_names[:]
            self.new_names[:] = []
            return None, new_names

        return None, None
