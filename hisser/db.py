import logging
import os.path
import heapq

from math import isnan
from time import time
from itertools import islice, groupby

from .blocks import Block, BlockList, notify_blocks_changed, get_info
from .utils import (estimate_data_size, mdumps, mloads, NAN, safe_unlink,
                    MB, page_size, norm_res, cursor, non_empty_rows, open_env)

log = logging.getLogger(__name__)


def abs_ratio(a, b):
    return max(a, b) / (min(a, b) or 1)


class Reader:
    def __init__(self, block_list, retentions, rpc_client):
        self.block_list = block_list
        self.retentions = retentions
        self.rpc_client = rpc_client

    def fetch(self, names, start, stop, res=None, rest_res=None):
        if not res:
            resolutions = [r[0] for r in self.retentions]
            resolutions.reverse()
            res = min(resolutions, key=lambda r: abs_ratio((stop - start) // r, 1000))
            rest_res = [r for r in resolutions if r < res]

        blocks = self.block_list.blocks(res)
        start = start // res * res
        stop = rstop = stop // res * res + res

        result = {}
        blocks = [b for b in blocks if b.end > start and b.start < stop]
        if blocks:
            blocks[0] = blocks[0].slice(start, stop)
            blocks[-1] = blocks[-1].slice(start, stop)

            start = blocks[0].start
            size = (blocks[-1].end - start) // res
            empty_row = [None] * size
            for b in blocks:
                r_start_idx = (b.start - start) // res
                r_end_idx = r_start_idx + b.size
                c_end_idx = b.idx + b.size
                data = read_block(b.path, names)
                for k, values in data.items():
                    try:
                        row = result[k]
                    except KeyError:
                        row = result[k] = empty_row[:]

                    row[r_start_idx:r_end_idx] = [None if isnan(v) else v
                                                  for v in values[b.idx:c_end_idx]]

            stop = start + size * res
        else:
            stop = start
            size = 0

        if not rest_res and rstop > stop:
            return self.add_rest_data_from_buffer(names, start, stop, rstop, res, size, result)
        return (start, stop, res), result

    def add_rest_data_from_buffer(self, keys, start, stop, rstop, res, size, result):
        if not self.rpc_client:
            return (start, stop, res), result

        try:
            cur_data = self.rpc_client.call('fetch', keys=keys)
        except Exception:
            log.exception('Error getting data')
            return (start, stop, res), result

        cur_result = cur_data['result']
        cur_slice = Block.make(cur_data['start'], cur_data['size'],
                               cur_data['resolution'], 'tmp')
        ib = cur_slice.slice(stop, rstop)
        if ib:
            add = [None] * ((ib.end - stop) // res)
            s_idx = size + (ib.start - stop) // res
            for name in keys:
                row = result.get(name)
                if row is None:
                    row = result[name] = [None] * size + add
                else:
                    row += add
                values = cur_result.get(name)
                if values is not None:
                    row[s_idx: s_idx + ib.size] = values[ib.idx:ib.idx+ib.size]
            stop = ib.end

        return (start, stop, res), result


class Storage:
    def __init__(self, data_dir, retentions, merge_finder, downsample_finder,
                 agg_rules, metric_index):
        self.data_dir = data_dir
        self.retentions = retentions
        self.merge_finder = merge_finder
        self.downsample_finder = downsample_finder
        self.agg_rules = agg_rules
        self.metric_index = metric_index

    def new_block(self, data, ts, resolution, size, new_names):
        self.new_names(new_names)
        data = sorted((k, list(v)) for k, v in non_empty_rows(data, size))
        if data:
            return new_block(self.data_dir, data, ts, resolution, size, append=True)

    def new_names(self, new_names):
        if new_names:
            self.metric_index.add(sorted(new_names))

    def do_housework(self, now=None):
        self.do_merge()
        self.do_downsample()
        self.do_cleanup(now)

    def do_merge(self):
        block_list = BlockList(self.data_dir)
        for res, _ in self.retentions:
            blocks = block_list.blocks(res)
            for s in self.merge_finder(res, blocks):
                log.info('Merge %r', s)
                merge(self.data_dir, res, s)

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

    def do_cleanup(self, now=None):
        block_list = BlockList(self.data_dir)
        now = now or time()
        for res, ret in self.retentions:
            for b in block_list.blocks(res):
                if b.end < now - ret:
                    os.unlink(b.path)
                    log.info('Cleanup old block %s', b.path)
            notify_blocks_changed(self.data_dir, res)


def split_descending_blocks(blocks, ratio):
    blocks = blocks[::-1]
    for idx, (p, n) in enumerate(zip(blocks[:-1], blocks[1:]), 1):
        if p.size / n.size > ratio:
            break
    else:
        idx = None

    if idx is not None:
        r1 = blocks[idx:][::-1]
        r2 = blocks[:idx][::-1]
    else:
        r1 = []
        r2 = blocks[::-1]

    if len(r2) > 1:
        for p, n in zip(r2[:-1], r2[1:]):
            if max(p.size, n.size) / min(p.size, n.size) <= ratio:
                r2 = [p, n]
                break
        else:
            r2 = []

    return [r1, r2]


def find_blocks_to_merge(resolution, blocks, *, max_size, max_gap_size, ratio):
    result = []
    segment = []
    it = iter(blocks)
    b = None
    while True:
        b = b or next(it, None)
        if not b:
            break

        if not segment:
            sstart = send = b.start
        else:
            sstart = segment[0].start
            send = segment[-1].end

        is_ok = True
        if is_ok and b.start - send > max_gap_size * resolution:
            is_ok = False

        if is_ok and (b.end - sstart) // resolution > max_size:
            is_ok = False

        if is_ok:
            segment.append(b)
            b = None
        elif segment:
            result.append(segment)
            segment = []

    if segment:
        result.append(segment)

    if result and len(result[-1]) > 1:
        result = result[:-1] + split_descending_blocks(result[-1], ratio)

    return [[b.path for b in s] for s in result if len(s) > 1]


def find_blocks_to_downsample(resolution, blocks, new_resolution,
                              max_gap_size, min_size, max_size, start=0):
    assert new_resolution % resolution == 0
    start = norm_res(start, new_resolution)
    result = []
    segment = None
    it = (r for r in blocks if r.end > start)
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

        bs = b.slice(s_start)
        if not bs:  # pragma: nocover
            break
        cur, b = bs.split(stop)
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
        iters = [iter_dump(b.path, idx) for idx, b in enumerate(blocks)]
        stream = groupby(heapq.merge(*iters), lambda r: r[0])

        resolution = blocks[0].resolution
        s_size = (s_stop - s_start) // resolution
        f_size = (s_stop - s_start) // new_resolution
        csize = new_resolution // resolution
        empty_row = [NAN] * s_size
        max_size, max_block = max((os.path.getsize(b.path), b) for b in blocks)
        map_size = page_size(max_size * f_size / max_block.size * 5)

        s_slices = []
        b_slices = []
        for b in blocks:
            idx = (b.start - s_start) // resolution
            s_slices.append(slice(idx, idx+b.size))
            b_slices.append(slice(b.idx, b.idx+b.size))

        def gen():
            for k, g in stream:
                row = empty_row[:]
                for _, bn, values in g:
                    row[s_slices[bn]] = values[b_slices[bn]]

                agg_method = agg_rules.get_method(k, use_bin=True)
                agg = [agg_method(row[r:r+csize]) for r in range(0, s_size, csize)]
                yield k, agg

        path = new_block(data_dir, gen(), s_start, new_resolution, s_size // csize,
                         map_size=map_size, append=True)
        log.info('Downsample %s', path)


def merge(data_dir, res, paths):
    blocks = [get_info(p, res) for p in paths]
    iters = [iter_dump(b.path, idx) for idx, b in enumerate(blocks)]

    first = blocks[0]
    last = blocks[-1]
    size = (last.end - first.start) // res
    empty_row = [NAN] * size

    max_size, max_block = max((os.path.getsize(b.path), b) for b in blocks)
    map_size = page_size(max_size * size / max_block.size * 3)

    slices = []
    overlaps = []
    last_idx = None
    for b in blocks:
        idx = (b.start - first.start) // res
        slices.append(slice(idx, idx+b.size))
        overlaps.append(last_idx and idx <= last_idx)
        last_idx = max(last_idx or 0, idx + b.size)

    stream = groupby(heapq.merge(*iters), lambda r: r[0])

    def gen():
        for k, g in stream:
            row = empty_row[:]
            for _, bn, values in g:
                if overlaps[bn]:
                    values = [r if isnan(v) else v
                              for r, v in zip(row[slices[bn]], values)]
                row[slices[bn]] = values
            yield k, row

    new_block(data_dir, gen(), first.start, res, size,
              map_size=map_size, append=True, notify=False)

    for p in paths:
        os.unlink(p)
        safe_unlink(p + '-lock')

    notify_blocks_changed(data_dir, res)


def new_block(data_dir, data, timestamp, resolution, size,
              map_size=None, append=False, notify=True):
    fname = '{}.{}.hdb'.format(timestamp, size)
    path = os.path.join(data_dir, str(resolution), fname)
    tmp_path = path + '.tmp'

    map_size = map_size or estimate_data_size(data, size) * 2 + 100*MB
    data = ((k, mdumps(v)) for k, v in data)
    with cursor(tmp_path, page_size(map_size), lock=False) as cur:
        cur.putmulti(data, overwrite=False, append=append)

    os.rename(tmp_path, path)

    if notify:
        notify_blocks_changed(data_dir, resolution)

    return path


def read_block(path, keys):
    result = {}
    with cursor(path, readonly=True) as cur:
        for k in keys:
            v = cur.get(k, None)
            if v is not None:
                result[k] = mloads(v)
    return result


def dump(path):  # pragma: nocover
    with cursor(path, readonly=True) as cur:
        for k, v in cur:
            yield k, mloads(v)


def iter_dump(path, idx, size=10000):
    k = None
    while True:
        with open_env(path, readonly=True) as env:
            with env.begin(write=False) as txn:
                with txn.cursor() as cur:
                    if k:
                        cur.set_key(k)
                    else:
                        cur.first()

                    for k, v in islice(cur, size):
                        yield k, idx, mloads(v)

                    if cur.next():
                        k = cur.key()
                    else:
                        break
