import logging
from math import isnan
from time import time
from array import array
from resource import getrusage, RUSAGE_SELF, RUSAGE_CHILDREN

from .utils import NAN, empty_rows

log = logging.getLogger(__name__)


class Buffer:
    def __init__(self, size, resolution, flush_size, past_size, max_points,
                 compact_ratio, now=None):
        self.size = size
        self.resolution = resolution
        self.flush_size = flush_size
        self.past_size = past_size
        self.max_points = max_points
        self.compact_ratio = compact_ratio

        self.data = {}
        self.new_names = []
        self.names_to_check = []
        self.collected_metrics = 0

        self.empty_row = array('d', [NAN] * size)
        self.past_points = 0
        self.future_points = 0
        self.received_points = 0
        self.flushed_points = 0
        self.last_size = 0

        self.set_ts(now or time())

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
        self.ts = int(ts) // self.resolution * self.resolution - self.past_size * self.resolution

    def flush(self, size):
        self.flushed_points += len(self.data) * size

        data = self.cut_data(size)
        if data:
            result = (data, self.ts, self.resolution, size, self.new_names[:])

            estimated_metrics = self.collected_metrics // size
            if not self.names_to_check and estimated_metrics / len(self.data) < self.compact_ratio:
                log.info('Compact data %d -> %d', len(self.data), estimated_metrics)
                self.names_to_check = list(self.data)
        else:
            result = None

        self.ts += self.resolution * size
        self.collected_metrics = 0
        self.last_size = 0
        self.new_names[:] = []
        return result

    def add(self, ts, name, value):
        self.received_points += 1
        idx = (int(ts) - self.ts) // self.resolution
        try:
            row = self.get_row(name)
            oldvalue = row[idx]
            row[idx] = value
            self.collected_metrics += isnan(oldvalue)
        except IndexError:
            if idx < 0:
                self.past_points += 1
            else:
                self.future_points += 1

    def check_and_drop_names(self):
        if not self.names_to_check:
            return

        log.info('Check for empty names %d', len(self.names_to_check))
        names = self.names_to_check[-10000:]
        self.names_to_check = self.names_to_check[:-10000]
        d = self.data
        for n in empty_rows(((k, d[k]) for k in names), self.size):
            del d[n]

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
        self.check_and_drop_names()

        now = int(now or time())
        size = (now - self.past_size * self.resolution - self.ts) // self.resolution

        if size < 0:
            return None, None

        if size != self.last_size:
            self.add_internal_metrics(now)
            self.last_size = size

        if force:
            size = (now - self.ts) // self.resolution
            return self.flush(min(size, self.size)), None

        if size >= self.size:
            return self.flush(self.size), None

        if size >= self.flush_size:
            return self.flush(self.flush_size), None

        if size * len(self.data) > self.max_points:
            return self.flush(size), None

        if size > 0 and self.new_names:
            new_names = self.new_names[:]
            self.new_names[:] = []
            return None, new_names

        return None, None
