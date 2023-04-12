import logging
import os.path
import heapq
import array
import zlib

import numpy as np
from math import isnan
from time import time
from itertools import islice, groupby

from .blocks import Block, BlockList, notify_blocks_changed, get_info
from .pack import pack, unpack, unpack_into
from .utils import (estimate_data_size, NAN, safe_unlink,
                    MB, page_size, norm_res, cursor, non_empty_rows,
                    open_env, make_key)

log = logging.getLogger(__name__)


def abs_ratio(a, b):
    return max(a, b) / (min(a, b) or 1)


class Reader:
    def __init__(self, block_list, retentions, rpc_client, buf_size):
        self.block_list = block_list
        self.retentions = retentions
        self.rpc_client = rpc_client
        self.buf_size = buf_size

    def need_data_from_buf(self, stop, resolution, now=None):
        now = now or time()
        buf_duration = self.buf_size * resolution
        return (resolution == self.retentions[0][0]
                and stop > now - buf_duration)

    def fetch(self, names, start, stop, res=None, now=None):
        now = now or time()
        if not res:
            resolutions = [r[0] for r in self.retentions]
            res_list = sorted(resolutions, key=lambda r: abs_ratio((stop - start) // r, 1000))
            if self.need_data_from_buf(stop, res_list[0], now):
                res_list = res_list[:1]
        else:  # pragma: no cover
            res_list = [res]

        ostart = start
        ostop = stop
        for res in res_list:
            blocks = self.block_list.blocks(res)
            start = ostart // res * res
            stop = rstop = (ostop + res) // res * res
            blocks = [b for b in blocks if b.end > start and b.start < stop]
            if blocks:
                break

        result = {}
        rnames = []
        if blocks:
            blocks[0] = blocks[0].slice(start, stop)
            blocks[-1] = blocks[-1].slice(start, stop)

            start = blocks[0].start
            size = (blocks[-1].end - start) // res
            max_bsize = 0
            for b in blocks:
                r_start_idx = (b.start - start) // res
                r_end_idx = r_start_idx + b.size
                c_end_idx = b.idx + b.size
                max_bsize = max(max_bsize, c_end_idx)
                data, info = read_block_raw(b.path, names)
                for name, raw_values in data.items():
                    try:
                        ndata = result[name]
                    except KeyError:
                        ndata = result[name] = []
                        rnames.append(name)

                    ndata.append((slice(r_start_idx, r_end_idx),
                                  slice(b.idx, c_end_idx),
                                  raw_values))

            buf = np.full(max_bsize, np.nan, dtype='d')
            ds_data = np.full((len(rnames), size), np.nan, dtype='d')
            for i, name in enumerate(rnames):
                for dst_slice, src_slice, raw_values in result[name]:
                    if src_slice.start == 0:
                        unpack_into(ds_data[i, dst_slice], raw_values)
                    else:  # pragma: no cover
                        unpack_into(buf, raw_values)  # TODO
                        ds_data[i, dst_slice] = buf[src_slice]

            stop = start + size * res
        else:
            res = res_list[0]
            stop = start = ostart // res * res
            rstop = (ostop + res) // res * res
            size = 0
            ds_data = np.full((len(rnames), 0), np.nan, dtype='d')

        if self.need_data_from_buf(rstop, res, now):
            return self.add_rest_data_from_buffer(names, start, stop, rstop, res, size, ds_data, rnames)
        return (start, stop, res), ds_data, rnames

    def add_rest_data_from_buffer(self, keys, start, stop, rstop, res, size, result, names):
        if not self.rpc_client:
            return (start, stop, res), result, names

        try:
            cur_data = self.rpc_client.call('fetch', keys=keys)
        except Exception:
            log.exception('Error getting data')
            return (start, stop, res), result, names

        cur_result = cur_data['result']
        cur_slice = Block.make(cur_data['start'], cur_data['size'],
                               cur_data['resolution'], 'tmp')
        ib = cur_slice.slice(stop, rstop)
        if ib:
            enames = {it: i for i, it in enumerate(names)}
            add = np.full((len(names), (ib.end - stop) // res), np.nan)
            result = np.hstack((result, add))
            s_idx = size + (ib.start - stop) // res
            newnames = [it for it in set(cur_result).difference(enames) if cur_result[it]]
            enames.update({it: i for i, it in enumerate(newnames, len(names))})
            if newnames:
                result = np.vstack((result, np.full((len(newnames), result.shape[1]), np.nan)))
                names.extend(newnames)
            for name, values in cur_result.items():
                if not values: continue
                idx = enames[name]
                result[idx, s_idx: s_idx + ib.size] = values[ib.idx:ib.idx+ib.size]

            stop = ib.end

        return (start, stop, res), result, names


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
        filtered = list(non_empty_rows(data))
        if filtered:
            data = sorted((make_key(k), v) for k, v in filtered)
            path = new_block(self.data_dir, data, ts, resolution, size, append=True)
            write_name_block(nblock_fname(path), (k for k, v in filtered))
            log.info('flushed %d metrics into %s', len(data), path)
            return path

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
                    safe_unlink(b.path + 'm')
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
    for blocks, s_start, s_stop in segments:
        iters = [iter_dump(b.path, idx) for idx, b in enumerate(blocks)]
        stream = groupby(heapq.merge(*iters), lambda r: r[0])

        resolution = blocks[0].resolution
        s_size = (s_stop - s_start) // resolution
        f_size = (s_stop - s_start) // new_resolution
        csize = new_resolution // resolution
        empty_row = array.array('d', [NAN] * s_size)
        max_size, max_block = max((os.path.getsize(b.path), b) for b in blocks)
        map_size = page_size(max_size * f_size / max_block.size * 5)

        s_slices = []
        b_slices = []
        for b in blocks:
            idx = (b.start - s_start) // resolution
            s_slices.append(slice(idx, idx+b.size))
            b_slices.append(slice(b.idx, b.idx+b.size))

        agg_funcs = {}
        agg_default = agg_rules.default
        for b in blocks:
            names = read_name_block(nblock_fname(b.path))
            agg_funcs.update(agg_rules.get_methods(names, use_bin=True)[0])

        agg_funcs = {make_key(k): v for k, v in agg_funcs.items()}

        def gen():
            for k, g in stream:
                row = empty_row[:]
                for _, bn, values in g:
                    row[s_slices[bn]] = values[b_slices[bn]]

                agg_method = agg_funcs.get(k, agg_default)
                agg = array.array('d', (agg_method(row[r:r+csize])
                                        for r in range(0, s_size, csize)))
                yield k, agg

        path = new_block(data_dir, gen(), s_start, new_resolution, s_size // csize,
                         map_size=map_size, append=True)

        merge_block_names([nblock_fname(it.path) for it in blocks],
                          nblock_fname(path))
        log.info('Downsample %s', path)


def merge(data_dir, res, paths):
    blocks = [get_info(p, res) for p in paths]
    iters = [iter_dump(b.path, idx) for idx, b in enumerate(blocks)]

    first = blocks[0]
    last = blocks[-1]
    size = (last.end - first.start) // res
    empty_row = array.array('d', [NAN] * size)

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
                    values = array.array('d', (r if isnan(v) else v
                              for r, v in zip(row[slices[bn]], values)))
                row[slices[bn]] = values
            yield k, row

    np = new_block(data_dir, gen(), first.start, res, size,
                   map_size=map_size, append=True, notify=False)

    merge_block_names(map(nblock_fname, paths), nblock_fname(np))

    for p in paths:
        os.unlink(p)
        safe_unlink(p + 'm')
        safe_unlink(p + '-lock')

    notify_blocks_changed(data_dir, res)


def merge_block_names(paths, dst):
    iters = [read_name_block(it) for it in paths]
    names = (k for k, g in groupby(heapq.merge(*iters)))
    write_name_block(dst, names, sort=False)


def new_block(data_dir, data, timestamp, resolution, size,
              map_size=None, append=False, notify=True):
    fname = '{}.{}.hdb'.format(timestamp, size)
    path = os.path.join(data_dir, str(resolution), fname)
    tmp_path = path + '.tmp'

    map_size = map_size or estimate_data_size(data, size) * 2 + 100*MB
    data = ((k, pack(v)) for k, v in data)
    with cursor(tmp_path, page_size(map_size), lock=False) as cur:
        cur.putmulti(data, overwrite=False, append=append)

    os.rename(tmp_path, path)

    if notify:
        notify_blocks_changed(data_dir, resolution)

    return path


def write_name_block(path, names, sort=True):
    tmp_path = path + '.tmp'
    if sort:
        names = sorted(names)
    with open(tmp_path, 'wb') as f:
        f.write(zlib.compress(b'\n'.join(names)))

    os.rename(tmp_path, path)
    return path


def read_name_block(path):
    if os.path.exists(path):
        with open(path, 'rb') as f:
            return zlib.decompress(f.read()).splitlines()
    return []


def dump_name_block(path, buf):
    d = zlib.decompressobj()
    read_size = 1 << 20
    with open(path, 'rb') as f:
        while True:
            data = f.read(read_size)
            if not data:
                buf.write(d.flush())
                break
            buf.write(d.decompress(data))


def nblock_fname(path):
    return path + 'm'


def read_block_raw(path, keys):
    k2k = {make_key(it): it for it in keys}
    keys = sorted(k2k)
    info = get_info(path)

    result = {}
    with cursor(path, readonly=True) as cur:
        for k in keys:
            v = cur.get(k, None)
            if v is not None:
                result[k2k[k]] = v
    return result, info


def dump(path):  # pragma: nocover
    info = get_info(path)
    with cursor(path, readonly=True) as cur:
        for k, v in cur:
            yield k, unpack(v, info.size)


def iter_dump(path, idx, size=10000):
    info = get_info(path)
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
                        yield k, idx, unpack(v, info.size)

                    if cur.next():
                        k = cur.key()
                    else:
                        break
