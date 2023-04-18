import logging
from math import isnan
from time import time
from resource import getrusage, RUSAGE_SELF, RUSAGE_CHILDREN
from threading import RLock

import numpy as np

log = logging.getLogger(__name__)


class DataChunk:
    def __init__(self, size, min_grow_size=5):
        self.min_grow_size = min_grow_size
        self.size = size
        self.data = np.full((0, size), np.nan, dtype=np.double)
        self.names = np.full(0, b'', dtype='O')
        self.new_names = []
        self.name_idx = {}
        self.lock = RLock()
        self.ts = None

    def __len__(self):
        return len(self.name_idx)

    def get_row(self, name):
        try:
            idx = self.name_idx[name]
        except KeyError:
            self.new_names.append(name)
            idx = len(self.name_idx)
            self.name_idx[name] = idx
            if idx >= len(self.names):
                with self.lock:
                    add_amount = max(self.min_grow_size, len(self.data))
                    new_chunk = np.full((add_amount, self.size), np.nan, dtype=np.double)
                    self.data = np.append(self.data, new_chunk, axis=0)
                    self.names = np.append(self.names, np.full(add_amount, b'', dtype='O'))
        self.names[idx] = name
        return self.data[idx]

    def compact(self, ratio):
        idx = ~np.all(np.isnan(self.data), axis=1)
        non_empty_metrics = np.count_nonzero(idx)
        if self.name_idx and non_empty_metrics / len(self.name_idx) < ratio / 2:
            log.info('Compact data %d -> %d', len(self.name_idx), non_empty_metrics)
            newdata = self.data[idx]
            newnames = self.names[idx]
            newnameidx = {it: i for i, it in enumerate(newnames)}
            with self.lock:
                self.data = newdata
                self.names = newnames
                self.name_idx = newnameidx

    def cut(self, start, size, ts):
        result = self.data[...,start:start+size]
        idx = ~np.all(np.isnan(result), axis=1)

        newdata = np.append(
            self.data[...,size:],
            np.full((len(self.names), size), np.nan, dtype=np.double),
            axis=1)

        with self.lock:
            newnames = self.new_names[:]
            self.new_names[:] = []
            self.ts = ts
            self.data = newdata

        return list(zip(self.names[idx], result[idx])), newnames

    def get_data(self, keys, ts=None):
        with self.lock:
            ts = self.ts or ts
            d = self.data
            nidx = self.name_idx

        result = {}
        for it in keys:
            try:
                result[it] = list(d[nidx[it]])
            except KeyError:
                pass
        return ts, result


class Buffer:
    def __init__(self, size, resolution, flush_size, past_size, max_points,
                 compact_ratio, now=None):
        self.size = size
        self.resolution = resolution
        self.flush_size = flush_size
        self.past_size = past_size
        self.max_points = max_points
        self.compact_ratio = compact_ratio

        self.chunk = DataChunk(size+flush_size)
        self.collected_metrics = 0

        self.past_points = 0
        self.future_points = 0
        self.received_points = 0
        self.flushed_points = 0
        self.last_size = 0

        self.set_ts(now or time())

    def get_data(self, keys):
        ts, result = self.chunk.get_data(keys, self.ts)
        return {'start': ts - self.flush_size * self.resolution,
                'result': result,
                'resolution': self.resolution,
                'size': self.size + self.flush_size}

    def set_ts(self, ts):
        self.ts = int(ts) // self.resolution * self.resolution - self.past_size * self.resolution

    def flush(self, size):
        # print(self.flush_size, size)
        # print(self.chunk.data)
        ts = self.ts
        next_ts = ts + self.resolution * size
        data, newnames = self.chunk.cut(self.flush_size, size, next_ts)
        self.ts = next_ts

        if data:
            self.flushed_points += len(data) * size
            result = (data, ts, self.resolution, size, newnames)
            self.chunk.compact(self.compact_ratio)
        else:
            result = None

        self.collected_metrics = 0
        self.last_size = 0
        return result

    def add(self, ts, name, value):
        self.received_points += 1
        idx = (int(ts) - self.ts) // self.resolution + self.flush_size
        row = self.chunk.get_row(name)
        try:
            oldvalue = row[idx]
            row[idx] = value
            self.collected_metrics += isnan(oldvalue)
        except IndexError:
            if idx < 0:
                self.past_points += 1
            else:
                self.future_points += 1

    def add_internal_metrics(self, now):
        self.add(now, b'hisser.flushed-points', self.flushed_points)
        self.add(now, b'hisser.received-points', self.received_points)
        self.add(now, b'hisser.past-points', self.past_points)
        self.add(now, b'hisser.future-points', self.future_points)

        r_main = getrusage(RUSAGE_SELF)
        self.add(now, b'hisser.cpu.main.user', r_main.ru_utime)
        self.add(now, b'hisser.cpu.main.sys', r_main.ru_stime)
        self.add(now, b'hisser.mem.main.maxrss', r_main.ru_maxrss)
        self.add(now, b'hisser.io.main.blocks_read', r_main.ru_inblock)
        self.add(now, b'hisser.io.main.blocks_write', r_main.ru_oublock)

        r_forks = getrusage(RUSAGE_CHILDREN)
        self.add(now, b'hisser.cpu.forks.user', r_forks.ru_utime)
        self.add(now, b'hisser.cpu.forks.sys', r_forks.ru_stime)
        self.add(now, b'hisser.mem.forks.maxrss', r_forks.ru_maxrss)
        self.add(now, b'hisser.io.forks.blocks_read', r_forks.ru_inblock)
        self.add(now, b'hisser.io.forks.blocks_write', r_forks.ru_oublock)

    def tick(self, force=False, now=None):
        now = int(now or time())
        size = (now - self.past_size * self.resolution - self.ts) // self.resolution

        if size < 0:
            return None, None

        if size != self.last_size:
            self.add_internal_metrics(now)
            self.last_size = size

        if force:
            size = (now - self.ts) // self.resolution
            print('@@', size, self.size)
            return self.flush(min(size, self.size)), None

        if size >= self.size:
            return self.flush(self.size), None

        if size >= self.flush_size:
            return self.flush(self.flush_size), None

        if size * len(self.chunk) > self.max_points:
            return self.flush(size), None

        if size > 0 and self.chunk.new_names:
            new_names = self.chunk.new_names[:]
            self.chunk.new_names[:] = []
            return None, new_names

        return None, None
