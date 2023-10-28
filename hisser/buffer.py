import logging
from time import time
from resource import getrusage, RUSAGE_SELF, RUSAGE_CHILDREN
from threading import RLock

import numpy as np
from hisser import utils

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
        # repeat check because cut block could omit existing metrics
        if non_empty_metrics > 0 and len(self.name_idx) / non_empty_metrics > ratio:
            log.info('Compact data %d -> %d', len(self.name_idx), non_empty_metrics)
            newdata = self.data[idx]
            newnames = self.names[idx]
            newnameidx = {it: i for i, it in enumerate(newnames)}
            with self.lock:
                self.data = newdata
                self.names = newnames
                self.name_idx = newnameidx

    def cut_data(self, start, size):
        result = self.data[...,start:start+size]
        idx = ~np.all(np.isnan(result), axis=1)
        return list(zip(self.names[idx], result[idx]))

    def cut_new_names(self):
        if self.new_names:
            newnames = self.new_names[:]
            self.new_names[:] = []
            return newnames

    def trim(self, start, size, modsize):
        with self.lock:
            if size >= modsize:
                self.data[...,:] = np.nan
            else:
                for s, e in iter_slices(start, start+size, self.size):
                    self.data[...,s:e] = np.nan
                for s, e in iter_slices(start+modsize, start+size+modsize, self.size):
                    self.data[...,s:e] = np.nan

    def get_data(self, keys, start, size):
        with self.lock:
            d = self.data
            nidx = self.name_idx

        result = {}
        for it in keys:
            try:
                result[it] = list(d[nidx[it]][start:start+size])
            except KeyError:
                pass
        return result


def iter_slices(start, end, size):
    if end > size:
        yield start, size
        yield 0, end % size
    else:
        yield start, end


class Buffer:
    def __init__(self, flush_size, resolution, compact_ratio, now=None):
        self.flush_size = flush_size
        self.size = flush_size * 3
        self.future_tolerance = flush_size // 2
        self.reservation = self.flush_size + self.future_tolerance
        self.resolution = resolution
        self.compact_ratio = compact_ratio

        self.chunk = DataChunk(self.size*2)  # needed for continuous ring buffer

        self.collected_metrics = 0
        self.received_points = 0
        self.flushed_points = 0
        self.last_size = 0

        self.last_flush = utils.norm_res(int(now or time()), self.resolution)
        self.buf_ts = self.last_flush
        self.last_trim = self.last_flush

    def get_data(self, keys, now=None):
        start = utils.norm_res(now or time(), self.resolution) - self.reservation * self.resolution
        idx = self.bufidx(start)
        result = self.chunk.get_data(keys, idx, self.reservation)
        return {'start': start,
                'result': result,
                'resolution': self.resolution,
                'size': self.reservation}

    def bufidx(self, ts):
        return (ts - self.buf_ts) // self.resolution % self.size

    def trim(self, ts):
        trim_size = (ts - self.last_trim) // self.resolution
        if trim_size < 1:
            return

        s = self.bufidx(ts + (self.size - self.reservation - trim_size) * self.resolution)
        log.debug('TRIM %s: %s - %s', ts, s, trim_size)
        self.chunk.trim(s, trim_size, self.size)
        self.last_trim = utils.norm_res(ts, self.resolution)

    def flush(self, size):
        ts = self.last_flush
        self.last_flush += self.resolution * size
        idx = self.bufidx(ts)
        log.debug('FLUSH %s: %s - %s', ts, idx, size)
        data = self.chunk.cut_data(idx, size)

        if data:
            self.flushed_points += len(data) * size
            result = (data, ts, self.resolution, size)
            if len(self.chunk) / len(data) > self.compact_ratio:
                self.chunk.compact(self.compact_ratio)
        else:
            result = None

        self.collected_metrics = 0
        self.last_size = 0
        return result

    def add(self, ts, name, value):
        self.received_points += 1
        idx = self.bufidx(int(ts))
        row = self.chunk.get_row(name)
        row[idx] = value
        row[idx + self.size] = value
        self.collected_metrics += 1

    def add_internal_metrics(self, now):
        self.add(now, b'hisser.flushed-points', self.flushed_points)
        self.add(now, b'hisser.received-points', self.received_points)

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
        now_size = (now - self.last_flush) // self.resolution
        size = now_size - self.future_tolerance

        new_names = None
        if size != self.last_size:
            self.trim(now)
            self.add_internal_metrics(now)
            self.last_size = size
            new_names = self.chunk.cut_new_names()

        if force and now_size > 0:
            return self.flush(min(now_size, self.reservation)), new_names

        if size >= self.flush_size:
            return self.flush(self.flush_size), new_names

        return None, new_names
