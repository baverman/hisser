import os
import resource

from time import time
from functools import partial
from math import isnan, ceil
from collections import namedtuple

import msgpack

NAN = float('nan')
MB = 1 << 20
PAGE_SIZE = resource.getpagesize()

mdumps = partial(msgpack.dumps, use_bin_type=True)
mloads = partial(msgpack.loads, encoding='utf-8')


def norm_res(ts, res):
    return int(ts) // res * res


def estimate_data_size(data, size):
    return (1000 * len(data) + size * 8 * len(data))


def page_size(size):
    return max(PAGE_SIZE, ceil(size / PAGE_SIZE) * PAGE_SIZE)


def safe_unlink(path):
    try:
        os.unlink(path)
    except:
        pass


def map_size_for_path(path):
    return page_size(os.path.getsize(path))


def overlap(range1, range2):
    left = max(range1[0], range2[0])
    right = min(range1[1], range2[1])
    result = right - left
    if result > 0:
        return result, left, right
    else:
        return 0, 0, 0


Fork = namedtuple('Fork', 'pid start')

def run_in_fork(func, *args, **kwargs):
    pid = os.fork()
    if pid == 0:
        try:
            func(*args, **kwargs)
        except:
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
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value
