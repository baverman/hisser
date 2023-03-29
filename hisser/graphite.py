import re
import logging
from functools import lru_cache
from time import perf_counter

from graphite.node import LeafNode, BranchNode
from graphite.finders.utils import BaseFinder
from graphite.render.grammar import grammar

from . import config
from .dataset import Dataset, Name

log = logging.getLogger('hisser.graphite')

old_parse = grammar.parseString
@lru_cache(maxsize=16384)
def parseString(instring, parseAll=False):
    return old_parse(instring, parseAll)
grammar.parseString = parseString


def scream(fn):  # pragma: nocover
    def inner(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            log.exception('Error getting data from hisser')
            raise
    return inner


def parse_tagspec(tagspec):
    m = re.match('^([^;!=]+)(!?=~?)([^;]*)$', tagspec)
    if m is None:  # pragma: no cover
        raise ValueError("Invalid tagspec %s" % tagspec)

    tag = m.group(1)
    operator = m.group(2)
    spec = m.group(3)

    return tag, operator, spec


class Finder(BaseFinder):
    tags = True

    def __init__(self, cfg=None):
        self.cfg = cfg or config.get_config({})
        self.reader = self.cfg.reader
        self.metric_index = self.cfg.metric_index

    @scream
    def find_nodes(self, query):
        for l, r in self.metric_index.find_tree(query.pattern):
            if l:
                yield LeafNode(r.decode(), None)
            else:
                yield BranchNode(r.decode())

    def find_metric_by_tags_many(self, patterns, cache):
        result = {}
        for pattern in patterns:
            args = grammar.parseString(pattern).expression.call.args
            exprs = [t.string[1:-1] for t in args if t.string]
            queries = [parse_tagspec(e) for e in exprs]
            result[pattern] = self.metric_index.match_by_tags(queries, cache)
        return result

    @scream
    def fetch(self, patterns, start_time, stop_time, now=None, requestContext=None):
        spatterns = []
        tpatterns = []
        cache = {}

        for pattern in patterns:
            if pattern.startswith('seriesByTag('):
                tpatterns.append(pattern)
            else:
                spatterns.append(pattern)

        names = set()
        smetrics = tmetrics = None

        if spatterns:
            smetrics = self.metric_index.find_metrics_many(spatterns)
            for v in smetrics.values():
                names.update(v)

        if tpatterns:
            tmetrics = self.find_metric_by_tags_many(tpatterns, cache)
            for v in tmetrics.values():
                names.update(v)

        names = sorted(names)
        time_info, data, rnames = self.reader.fetch(names, int(start_time), int(stop_time))

        nidx = {it: i for i, it in enumerate(rnames)}
        result = []

        for metrics in filter(None, (smetrics, tmetrics)):
            for query, names in metrics.items():
                enames = [nidx.get(it) for it in names]
                qnames = [(Name(rnames[it].decode()), it) for it in enames if it is not None]
                result.append(Dataset(query, qnames, data, *time_info))

        return result

    @scream
    def auto_complete_tags(self, exprs, tagPrefix=None, limit=None, requestContext=None):
        result = tags = [r.decode()
                         for r in self.metric_index.get_tags()
                         if not r.startswith(b'.')]
        if tagPrefix:
            result = [r for r in result if r.startswith(tagPrefix)]
            if len(result) < 10:
                rset = set(result)
                result.extend(r for r in tags
                              if tagPrefix in r and r not in rset)
        return result

    @scream
    def auto_complete_values(self, exprs, tag, valuePrefix=None, limit=None, requestContext=None):
        result = values = [r.decode() for r in self.metric_index.get_tag_values(tag)]
        if valuePrefix:
            result = [r for r in result if r.startswith(valuePrefix)]
            if len(result) < 10:
                rset = set(result)
                result.extend(r for r in values
                              if valuePrefix in r and r not in rset)
        return result
