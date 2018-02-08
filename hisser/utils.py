import os
import resource
import array

from time import time
from functools import partial
from math import ceil
from collections import namedtuple
from contextlib import contextmanager

import msgpack
import lmdb

NAN = float('nan')
MB = 1 << 20
PAGE_SIZE = resource.getpagesize()

mdumps = partial(msgpack.dumps, use_bin_type=True)
mloads = partial(msgpack.loads, encoding='utf-8')

Fork = namedtuple('Fork', 'pid start')


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


def run_in_fork(func, *args, **kwargs):
    pid = os.fork()
    if pid == 0:  # pragma: nocover
        try:
            func(*args, **kwargs)
        except Exception:
            import traceback
            traceback.print_exc()
            os._exit(1)
        else:
            os._exit(0)
    else:
        return Fork(pid, time())


def wait_childs():
    return os.waitpid(-1, os.WNOHANG)


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
def txn_cursor(env, write=False, db=None):
    with env.begin(write=write) as txn:
        with txn.cursor(db) as cur:
            yield cur


def empty_rows(data, size):
    nanbuf = array.array('d', [NAN] * size).tobytes()
    for k, v in data:
        if v.tobytes() == nanbuf:
            yield k


def non_empty_rows(data, size):
    nanbuf = array.array('d', [NAN] * size).tobytes()
    for k, v in data:
        if v.tobytes() != nanbuf:
            yield k, v
