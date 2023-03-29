import os
import resource

from functools import partial
from itertools import islice
from math import ceil
from contextlib import contextmanager

from xxhash import xxh64_digest
import msgpack
import lmdb

from hisser.pack import array_is_empty

NAN = float('nan')
MB = 1 << 20
PAGE_SIZE = resource.getpagesize()

mdumps = msgpack.dumps
mloads = msgpack.loads
mloads_t = partial(msgpack.loads, use_list=False)


def norm_res(ts, res):
    return int(ts) // res * res


def estimate_data_size(data, size):
    return (1000 * len(data) + size * 8 * len(data))


def page_size(size):
    return max(PAGE_SIZE, ceil(size / PAGE_SIZE) * PAGE_SIZE)


def safe_unlink(path):
    try:
        os.unlink(path)
    except OSError:
        pass


def map_size_for_path(path):
    return page_size(os.path.getsize(path))


def overlap(range1, range2):  # pragma: nocover
    left = max(range1[0], range2[0])
    right = min(range1[1], range2[1])
    result = right - left
    if result > 0:
        return result, left, right
    else:
        return 0, 0, 0


class cached_property(object):
    def __init__(self, func):
        self.__doc__ = getattr(func, '__doc__')
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:  # pragma: nocover
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


def open_env(path, map_size=None, readonly=False, lock=None):
    lock = not readonly if lock is None else lock
    try:
        if map_size and map_size < 0:
            map_size = map_size_for_path(path) - map_size
        else:
            map_size = map_size or map_size_for_path(path)
    except FileNotFoundError:
        map_size = 10*MB
    return lmdb.open(path, map_size, subdir=False, readonly=readonly, lock=lock)


@contextmanager
def cursor(path, map_size=None, readonly=False, lock=None, buffers=False):
    with open_env(path, map_size, readonly=readonly, lock=lock) as env:
        with env.begin(write=not readonly, buffers=buffers) as txn:
            with txn.cursor() as cur:
                yield cur


@contextmanager
def txn_cursor(env, write=False, *dbs):
    with env.begin(write=write) as txn:
        cursors = [txn.cursor(it) for it in dbs]
        try:
            if len(cursors) == 1:
                yield cursors[0]
            else:
                yield cursors
        finally:
            for it in cursors:
                it.close()


def empty_rows(data):
    for k, v in data:
        if array_is_empty(v):
            yield k


def non_empty_rows(data):
    for k, v in data:
        if not array_is_empty(v):
            yield k, v


def make_key(name):
    return name[:8] + xxh64_digest(name)


def make_key_u(name):
    return name.encode()[:8] + xxh64_digest(name)


def iter_chunks(it, size):  # pragma: no cover
    it = iter(it)
    while True:
        data = list(islice(it, size))
        if not data:
            break
        yield data


def clone(src, **kwargs):
    """Clones object with optionally overridden fields"""
    obj = object.__new__(type(src))
    obj.__dict__.update(src.__dict__)
    obj.__dict__.update(kwargs)
    return obj


def parse_interval(value):
    if type(value) is int:
        return True, value

    if value.endswith('s'):
        return False, int(value[:-1])
    if value.endswith('min'):
        return False, int(value[:-3]) * 60
    elif value.endswith('h'):
        return False, int(value[:-1]) * 3600
    elif value.endswith('d'):
        return False, int(value[:-1]) * 86400
    elif value.endswith('w'):
        return False, int(value[:-1]) * 86400 * 7
    elif value.endswith('mon'):
        return False, int(value[:-3]) * 86400 * 30
    elif value.endswith('y'):
        return False, int(value[:-1]) * 86400 * 365

    return True, int(value)
