from graphite.node import LeafNode, BranchNode
from graphite.finders.utils import BaseFinder

from . import config


def scream(fn):
    def inner(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except:
            import traceback
            traceback.print_exc()
            raise
    return inner


class Finder(BaseFinder):
    def __init__(self):
        self.cfg = config.get_config({})
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
        metrics = {k: [r.decode() for r in v] for k, v in metrics.items()}

        keys = set()
        for v in metrics.values():
            keys.update(v)
        time_info, data = self.reader.fetch(keys, start_time, stop_time)

        result = []
        for query, names in metrics.items():
            for name in names:
                values = data.get(name)
                if values:
                    result.append({
                        'pathExpression': query,
                        'path': name,
                        'name': name,
                        'time_info': time_info,
                        'values': values,
                    })

        return result
