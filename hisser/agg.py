import re
from math import isnan

from .utils import NAN


def is_not_nan(num):
    return not isnan(num)


def _sum(data):
    non_empty = list(filter(is_not_nan, data))
    return sum(non_empty), len(non_empty)


def safe_avg(data):
    total, n = _sum(data)
    if n:
        return total / n
    return NAN


def safe_sum(data):
    total, n = _sum(data)
    if n:
        return total
    return NAN


def safe_max(data):
    return max(filter(is_not_nan, data), default=NAN)


def safe_min(data):
    return min(filter(is_not_nan, data), default=NAN)


def safe_last(data):
    try:
        return list(filter(is_not_nan, data))[-1]
    except IndexError:
        return NAN


METHODS = {
    'avg': safe_avg,
    'sum': safe_sum,
    'max': safe_max,
    'min': safe_min,
    'last': safe_last
}


class AggRules:
    def __init__(self, rules, default='avg'):
        self.rules = tuple((re.compile(r), METHODS[m]) for r, m in rules)
        self.rules_bin = tuple((re.compile(r.encode()), METHODS[m]) for r, m in rules)
        self.default = METHODS[default]

    def get_methods(self, names, use_bin=False):
        result = {}
        rules = self.rules_bin if use_bin else self.rules
        for n in names:
            for p, v in rules:
                if p.search(n):
                    result[n] = v
                    break
        return result, self.default

    def get_method(self, name, use_bin=False):
        rules = self.rules_bin if use_bin else self.rules
        for p, v in rules:
            if p.search(name):
                return v
        return self.default
