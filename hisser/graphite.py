import logging

from graphite.node import LeafNode, BranchNode
from graphite.finders.utils import BaseFinder

from . import config

log = logging.getLogger('hisser.graphite')


def scream(fn):  # pragma: nocover
    def inner(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            log.exception('Error getting data from hisser')
            raise
    return inner


class Finder(BaseFinder):
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

    @scream
    def fetch(self, patterns, start_time, stop_time, now=None, requestContext=None):
        metrics = self.metric_index.find_metrics_many(patterns)

        names = set()
        for v in metrics.values():
            names.update(v)
        names = sorted(names)

        time_info, data = self.reader.fetch(names, int(start_time), int(stop_time))

        result = []
        for query, names in metrics.items():
            for name in names:
                values = data.get(name)
                if values:
                    result.append({
                        'pathExpression': query,
                        'path': name.decode(),
                        'name': name.decode(),
                        'time_info': time_info,
                        'values': values,
                    })

        return result
