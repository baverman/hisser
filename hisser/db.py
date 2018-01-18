import logging
import os.path
import pathlib

from math import isnan
from collections import namedtuple

from .utils import (estimate_data_size, mdumps, mloads, NAN, safe_unlink,
                    MB, page_size, norm_res, cursor)

log = logging.getLogger(__name__)


def abs_ratio(a, b):
    return max(a, b) / (min(a, b) or 1)


class BlockInfo(namedtuple('BlockInfo', 'start end idx size resolution path')):
    @staticmethod
    def make(start, size, resolution, path):
        return BlockInfo(start, start + size * resolution, 0, size, resolution, path)

    def split(self, ts):
        return BlockSlice.make(self).split(ts)

    def slice(self, start, stop=None):
        return BlockSlice.make(self).slice(start, stop)


class BlockSlice(namedtuple('BlockSlice', 'start end idx size resolution path bstart')):
    @staticmethod
    def make(block):
        return BlockSlice(block.start, block.end, 0, block.size,
                          block.resolution, block.path, block.start)

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

        start = ts
        end = self.end
        return self._replace(start=start, end=end,
                             idx=(start - self.bstart) // self.resolution,
                             size=(end - start) // self.resolution)

    def slice_to(self, ts):
        if ts <= self.start:
            return None

        if ts >= self.end:
            return self

        start = self.start
        end = ts
        return self._replace(start=start, end=end,
                             idx=(start - self.bstart) // self.resolution,
                             size=(end - start) // self.resolution)


class BlockList:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self._last_state = {}
        self._blocks = {}

    def check(self, resolution, refresh):
        if refresh or resolution not in self._last_state:
            self.rescan(resolution)
            self._last_state[resolution] = 0
            return

        try:
            new_state = os.path.getmtime(os.path.join(self.data_dir,
                                                      str(resolution), 'blocks.state'))
        except OSError:
            new_state = 0

        if self._last_state[resolution] < new_state:
            self._last_state[resolution] = new_state
            self.rescan(resolution)

    def blocks(self, resolution, refresh=False):
        self.check(resolution, refresh)
        return self._blocks[resolution]

    def rescan(self, resolution):
        blocks = self._blocks[resolution] = []
        data_path = os.path.join(self.data_dir, str(resolution))

        try:
            entries = os.scandir(data_path)
        except FileNotFoundError:
            os.makedirs(data_path, exist_ok=True)
            entries = []

        for e in entries:
            if not e.name.endswith('.hdb') or not e.is_file():
                continue
            try:
                info = get_info(e.path, resolution)
            except ValueError:
                pass
            else:
                blocks.append(info)

        blocks.sort()


class Reader:
    def __init__(self, block_list, retentions, rpc_client):
        self.block_list = block_list
        self.retentions = retentions
        self.rpc_client = rpc_client

    def fetch(self, keys, start, stop, res=None, rest_res=None):
        if not res:
            resolutions = [r[0] for r in self.retentions]
            resolutions.reverse()
            res = min(resolutions, key=lambda r: abs_ratio((stop - start) // r, 1000))
            rest_res = [r for r in resolutions if r < res]

        rstop = stop

        blocks = self.block_list.blocks(res)
        start = start // res * res
        stop = stop // res * res

        blocks = [b for b in blocks if b.end > start and b.start < stop]
        if not blocks:
            return (0, 0, 0), {}

        blocks[0] = blocks[0].slice(start, stop)
        blocks[-1] = blocks[-1].slice(start, stop)

        result = {}
        start = blocks[0].start
        size = (blocks[-1].end - start) // res
        empty_row = [None] * size
        for b in blocks:
            r_start_idx = (b.start - start) // res
            r_end_idx = r_start_idx + b.size
            c_end_idx = b.idx + b.size
            data = read_block(b.path, keys)
            for k, values in data.items():
                try:
                    row = result[k]
                except KeyError:
                    row = result[k] = empty_row[:]

                row[r_start_idx:r_end_idx] = [None if isnan(v) else v
                                              for v in values[b.idx:c_end_idx]]

        stop = start + size * res
        if not rest_res and rstop > stop:
            return self.add_rest_data_from_buffer(keys, start, stop, rstop, res, size, result)
        return (start, stop, res), result

    def add_rest_data_from_buffer(self, keys, start, stop, rstop, res, size, result):
        if not self.rpc_client:
            return (start, stop, res), result
        cur_data = self.rpc_client.call('fetch', keys=[r.encode() for r in keys])
        cur_result = {k.decode(): v for k, v in cur_data['result'].items()}
        cur_slice = BlockInfo.make(cur_data['start'], cur_data['resolution'], cur_data['size'], 'tmp')
        ib = cur_slice.slice(stop, rstop)
        if ib:
            add = [None] * ((ib.end - stop) // res)
            s_idx = size + (ib.start - stop) // res
            for k, v in result.items():
                v += add
                if k in cur_result:
                    v[s_idx: s_idx + ib.size] = cur_result[k]
            stop = ib.end

        return (start, stop, res), result


class Storage:
    def __init__(self, data_dir, retentions, merge_finder,
                 downsample_finder, agg_rules):
        self.data_dir = data_dir
        self.retentions = retentions
        self.merge_finder = merge_finder
        self.downsample_finder = downsample_finder
        self.agg_rules = agg_rules

    def new_block(self, data, ts, resolution, size):
        data = sorted((k, list(v)) for k, v in data)
        return new_block(self.data_dir, data, ts, resolution, size, append=True)

    def do_housework(self):
        self.do_merge()
        self.do_downsample()

    def do_merge(self):
        block_list = BlockList(self.data_dir)
        for res, _ in self.retentions:
            blocks = block_list.blocks(res)
            for p1, p2 in self.merge_finder(res, blocks):
                log.info('Merge %s %s', p1, p2)
                merge(self.data_dir, res, [p1, p2])

    def do_downsample(self):
        block_list = BlockList(self.data_dir)
        resolutions = [r[0] for r in self.retentions]
        for res, new_res in zip(resolutions[:-1], resolutions[1:]):
            blocks = block_list.blocks(res)
            if not blocks:
                continue
            new_blocks = block_list.blocks(new_res)
            start = new_blocks and new_blocks[-1].end or 0
            segments = self.downsample_finder(res, blocks, new_res, start)
            if segments:
                downsample(self.data_dir, new_res, segments, self.agg_rules)


def find_blocks_to_merge(resolution, blocks, *, max_size,
                         max_gap_size, ratio):
    result = []
    found = False
    for b1, b2 in zip(blocks[:-1], blocks[1:]):
        if found:
            found = False
            continue

        if b2.start - b1.end > max_gap_size * resolution:
            continue

        if (b2.end - b1.start) // resolution >= max_size:
            continue

        if max(b1.size, b2.size) / min(b1.size, b2.size) > ratio:
            continue

        found = True
        result.append((b1.path, b2.path))

    return result


def find_blocks_to_downsample(resolution, blocks, new_resolution,
                              max_gap_size, min_size, max_size, start=0):
    assert new_resolution % resolution == 0
    start = norm_res(start, new_resolution)
    result = []
    segment = None
    it = (b for b in blocks if b.end > start)
    b = None
    while True:
        b = b or next(it, None)
        if not b:
            break

        prev = segment and segment[-1]
        if not segment or (b.start - prev.end) // new_resolution > max_gap_size:
            segment = []
            if b.start <= start:
                s_start = start
            else:
                s_start = norm_res(b.start, new_resolution)
            stop = norm_res(s_start + max_size * resolution, new_resolution)
            result.append((segment, s_start))

        cur, b = b.slice(s_start).split(stop)
        s_start = cur.end
        segment.append(cur)
        if s_start >= stop:
            segment = []

    if result:
        last = result[-1][0]
        ssize = (last[-1].end - last[0].start) // new_resolution
        if ssize < min_size:
            result = result[:-1]

    final_result = []
    for segment, s_start in result:
        s_stop = norm_res(segment[-1].end, new_resolution)
        if s_stop < segment[-1].end:
            s_stop += new_resolution
        final_result.append((segment, s_start, s_stop))

    return final_result


def downsample(data_dir, new_resolution, segments, agg_rules):
    for (blocks, s_start, s_stop) in segments:
        resolution = blocks[0].resolution
        s_size = (s_stop - s_start) // resolution
        empty_row = [NAN] * s_size
        data = {}

        for b in blocks:
            s_idx = (b.start - s_start) // resolution
            s_to = s_idx + b.size
            b_to = b.idx + b.size
            for k, values in dump(b.path):
                try:
                    row = data[k]
                except KeyError:
                    row = data[k] = empty_row[:]
                row[s_idx:s_to] = values[b.idx:b_to]

        result = []
        csize = new_resolution // resolution
        for k, values in data.items():
            agg_method = agg_rules.get_method(k, use_bin=True)
            agg = [agg_method(values[k:k+csize]) for k in range(0, len(values), csize)]
            result.append((k, agg))

        result.sort()
        path = new_block(data_dir, result, s_start, new_resolution, s_size // csize, append=True)
        log.info('Downsample %s', path)


def merge(data_dir, res, paths):
    blocks = [get_info(p, res) for p in paths]
    first = blocks[0]
    last = blocks[-1]
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

    new_block(data_dir, sorted(data.items()),
              first.start, res, size, append=True, notify=False)

    for p in paths:
        os.unlink(p)
        safe_unlink(p + '-lock')

    notify_blocks_changed(data_dir, res)


def new_block(data_dir, data, timestamp, resolution, size, append=False, notify=True):
    fname = '{}.{}.hdb'.format(timestamp, size)
    path = os.path.join(data_dir, str(resolution), fname)
    tmp_path = path + '.tmp'

    size = estimate_data_size(data, size) * 2 + 100*MB
    data = ((k, mdumps(v)) for k, v in data)
    with cursor(tmp_path, page_size(size), lock=False) as cur:
        cur.putmulti(data, overwrite=False, append=append)

    os.rename(tmp_path, path)

    if notify:
        notify_blocks_changed(data_dir, resolution)

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


def notify_blocks_changed(data_dir, resolution):
    path = os.path.join(data_dir, str(resolution), 'blocks.state')
    pathlib.Path(path).touch(exist_ok=True)


def get_info(path, res=0):
    ts, size, *rest = os.path.basename(path).split('.')
    ts, size = int(ts), int(size)
    return BlockInfo.make(ts, size, res, path)
