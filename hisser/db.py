import os.path
import pathlib

from time import time
from math import isnan
from fnmatch import fnmatch
from contextlib import contextmanager
from collections import namedtuple

import lmdb

from .utils import (estimate_data_size, mdumps, mloads, NAN, overlap, safe_unlink,
                    MB, page_size, map_size_for_path, norm_res)


BlockInfo = namedtuple('BlockInfo', 'start end resolution size path')


class BlockSlice(namedtuple('BlockSlice', 'block start end idx size')):
    @staticmethod
    def make(block):
        return BlockSlice(block, block.start, block.end, 0, block.size)

    def split(self, ts):
        if ts <= self.start:
            return None, self
        elif ts >= self.end:
            return self, None

        return self.slice_to(ts), self.slice_from(ts)

    def slice(self, start, stop=None):
        result = self
        if start is not None:
            result = result.slice_from(start)
        if stop is not None:
            result = result and result.slice_to(stop)
        return result

    def slice_from(self, ts):
        if ts <= self.start:
            return self

        if ts >= self.end:
            return None

        b = self.block
        start = ts
        end = self.end
        return BlockSlice(b, start, end,
                          (start - b.start) // b.resolution,
                          (end - start) // b.resolution)

    def slice_to(self, ts):
        if ts <= self.start:
            return None

        if ts >= self.end:
            return self

        b = self.block
        start = self.start
        end = ts
        return BlockSlice(b, start, end,
                          (start - b.start) // b.resolution,
                          (end - start) // b.resolution)


class BlockList:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self._scanned = False
        self._last_state = 0

    def check(self, refresh):
        if not self._scanned or refresh:
            self.rescan()

        try:
            new_state = os.path.getmtime(os.path.join(self.data_dir, 'blocks.state'))
        except OSError:
            new_state = 0

        if self._last_state != new_state:
            self._last_state = new_state
            self.rescan()

    def blocks(self, refresh=False):
        self.check(refresh)
        return self._blocks

    def blocks_and_intervals(self, refresh=False):
        self.check(refresh)
        return self._blocks, self._intervals

    def rescan(self):
        self._blocks = {}
        self._intervals = {}
        for e in os.scandir(self.data_dir):
            if not e.name.endswith('.hdb') or not e.is_file():
                continue
            try:
                info = get_info(e.path)
            except ValueError:
                pass
            else:
                self._blocks.setdefault(info.resolution, []).append(info)

        for res, blocks in self._blocks.items():
            blocks.sort()
            self._intervals[res] = blocks[0].start, blocks[-1].end

        self._scanned = True


class Reader:
    def __init__(self, block_list):
        self.block_list = block_list

    def metric_names(self):
        blocks = self.block_list.blocks()
        if not blocks:
            return
        min_res = sorted(blocks.keys())[0]
        path = blocks[min_res][-1].path
        with cursor(path, readonly=True) as cur:
            for k in cur.iternext(True, False):
                yield k.decode()

    def find_metrics(self, queries):
        names = [r.split('.') for r in self.metric_names()]
        lnames = [(len(r), r) for r in names]
        matched_metrics = {}
        for q in queries:
            qparts = q.split('.')
            qlen = len(qparts)
            result = [(l, n) for l, n in lnames if l == qlen]
            for idx, pattern in enumerate(qparts):
                result = [(l, n) for l, n in result if fnmatch(n[idx], pattern)]
            matched_metrics[q] = ['.'.join(n) for _, n in result]
        return matched_metrics

    def fetch(self, keys, start, stop):
        blocks, intervals = self.block_list.blocks_and_intervals()
        overlaps = [(res, overlap(interval, (start, stop)))
                    for res, interval in intervals.items()]
        overlaps.sort(key=lambda r: (r[1][0], -r[0]))

        res, (duration, start, stop) = overlaps[-1]
        start = start // res * res
        stop = stop // res * res
        if not duration:
            return (0, 0, 0), {}

        result = {}
        size = (stop - start) // res
        empty_row = [None] * size
        for b in blocks[res]:
            if b.end <= start or b.start > stop:
                continue
            data = read_block(b.path, keys)
            for k, values in data.items():
                try:
                    row = result[k]
                except KeyError:
                    row = result[k] = empty_row[:]

                r_start_idx = (b.start - start) // res
                if r_start_idx < 0:
                    c_start_idx = -r_start_idx
                    r_start_idx = 0
                else:
                    c_start_idx = 0

                r_end_idx = (b.end - start) // res
                if r_end_idx > size:
                    c_end_idx = b.size - (r_end_idx - size)
                    r_end_idx = size
                else:
                    c_end_idx = b.size

                row[r_start_idx:r_end_idx] = [None if isnan(v) else v
                                              for v in values[c_start_idx:c_end_idx]]

        return (start, stop, res), result


class Storage:
    def __init__(self, data_dir, merge_finder):
        self.data_dir = data_dir
        self.merge_finder = merge_finder

    def new_block(self, data, ts, resolution, size):
        data = sorted((k, list(v)) for k, v in data)
        return new_block(self.data_dir, data, ts, resolution, size, append=True)

    def do_merge(self):
        all_blocks = BlockList(self.data_dir).blocks()
        for res, blocks in all_blocks.items():
            for p1, p2 in self.merge_finder(res, blocks):
                print('Merge', p1, p2)
                merge(self.data_dir, [p1, p2])


def find_blocks_to_merge(resolution, blocks, *, max_size, keep_size,
                         max_gap_size, ratio, now=None):
    result = []
    now = now or time()
    stop = now - keep_size * resolution
    found = False
    for b1, b2 in zip(blocks[:-1], blocks[1:]):
        if found:
            found = False
            continue

        if b2.start - b1.end > max_gap_size * resolution:
            continue

        if (b2.end - b1.start) // resolution >= max_size:
            continue

        if b2.end > stop:
            continue

        if max(b1.size, b2.size) / min(b1.size, b2.size) > ratio:
            continue

        found = True
        result.append((b1.path, b2.path))

    return result


def find_blocks_to_downsample(resolution, blocks, new_resolution, start,
                              max_gap, min_size, max_size):
    assert new_resolution % resolution == 0
    start = norm_res(start, new_resolution)
    result = []
    segment = None
    it = (BlockSlice.make(r) for r in blocks if r.end > start)
    b = None
    while True:
        b = b or next(it, None)
        if not b:
            break

        prev = segment and segment[-1]
        if not segment or (b.start - prev.end) // resolution > max_gap:
            segment = []
            if b.start <= start:
                s_start = start
            else:
                s_start = norm_res(b.start, new_resolution)
            s_stop = norm_res(s_start + max_size * resolution, new_resolution)
            result.append((segment, s_start))

        cur, b = b.slice(s_start).split(s_stop)
        s_start = cur.end
        segment.append(cur)
        if s_start >= s_stop:
            segment = []

    if result:
        last = result[-1][0]
        ssize = (last[-1].end - last[0].start) // resolution
        if ssize < min_size:
            result = result[:-1]

    return result


def merge(data_dir, paths):
    blocks = list(map(get_info, paths))
    first = blocks[0]
    last = blocks[-1]
    assert all(first.resolution == r.resolution for r in blocks), 'All blocks must have same resolution'
    res = first.resolution
    size = (last.end - first.start) // res
    empty_row = [NAN] * size

    data = {}
    for k, v in dump(first.path):
        row = data[k] = empty_row[:]
        row[:first.size] = v

    for b in blocks[1:]:
        idx = (b.start - first.start) // res
        for k, v in dump(b.path):
            row = data.get(k)
            if not row:
                row = data[k] = empty_row[:]
            row[idx:idx+b.size] = v

    new_block(data_dir, sorted(data.items()), first.start, res, size, append=True)

    for p in paths:
        os.unlink(p)
        safe_unlink(p + '-lock')

    notify_blocks_changed(data_dir)


def new_block(data_dir, data, timestamp, resolution, size, append=False):
    fname = '{}.{}.{}.hdb'.format(timestamp, resolution, size)
    path = os.path.join(data_dir, fname)
    size = estimate_data_size(data, size) * 2 + 100*MB
    data = ((k, mdumps(v)) for k, v in data)
    with cursor(path, page_size(size)) as cur:
        cur.putmulti(data, overwrite=False, append=append)
    notify_blocks_changed(data_dir)
    return path


def read_block(path, keys):
    result = {}
    with cursor(path, readonly=True) as cur:
        for k in keys:
            v = cur.get(k.encode(), None)
            if v is not None:
                result[k] = mloads(v)
    return result


def dump(path):
    with cursor(path, readonly=True) as cur:
        for k, v in cur:
            yield k, mloads(v)


@contextmanager
def cursor(path, map_size=None, readonly=False):
    with lmdb.open(path, map_size or map_size_for_path(path),
                   subdir=False, readonly=readonly, lock=not readonly) as env:
        with env.begin(write=not readonly) as txn:
            with txn.cursor() as cur:
                yield cur


def notify_blocks_changed(data_dir):
    pathlib.Path(os.path.join(data_dir, 'blocks.state')).touch(exist_ok=True)


def get_info(path):
    ts, res, size, *rest = os.path.basename(path).split('.')
    ts, res, size = int(ts), int(res), int(size)
    return BlockInfo(ts, ts + res * size, res, size, path)
