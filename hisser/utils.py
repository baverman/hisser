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
TIME_SUFFIXES = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400,
                 'w': 86400 * 7, 'y': 86400 * 365}

mdumps = partial(msgpack.dumps, use_bin_type=True)
mloads = partial(msgpack.loads, encoding='utf-8')


def norm_res(ts, res):
    return int(ts) // res * res


def estimate_data_size(data, size):
    return (1000 * len(data) + size * 8 * len(data))


def is_not_nan(num):
    return not isnan(num)


def _sum(data):
    non_empty = list(filter(is_not_nan, data))
    return sum(non_empty), len(non_empty)


def safe_avg(data):
    total, n = _sum(data)
    if n:
        return total / n


def safe_sum(data):
    total, n = _sum(data)
    if n:
        return total


def safe_max(data):
    return max(filter(is_not_nan, data), default=None)


def safe_min(data):
    return min(filter(is_not_nan, data), default=None)


def safe_last(data):
    try:
        return list(filter(is_not_nan, data))[-1]
    except IndexError:
        pass


def parse_seconds(interval):
    if isinstance(interval, int):
        return interval

    interval = interval.strip()
    if interval.isdigit():
        return int(interval)

    return int(interval[:-1]) * TIME_SUFFIXES[interval[-1]]


def parse_retentions(string):
    result = (part.split(':') for part in string.split(','))
    result = ((parse_seconds(res), parse_seconds(ret)) for res, ret in result)
    return sorted((res, ret // res) for res, ret in result)


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
