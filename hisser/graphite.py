from graphite.node import LeafNode, BranchNode
from graphite.finders.utils import BaseFinder

from . import config


def match(pattern, text):
    return pattern == '*' or pattern == text


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

    @scream
    def find_nodes(self, query):
        patterns = query.pattern.split('.')
        result = set()
        for m in self.reader.metric_names():
            mparts = m.split('.')
            if len(mparts) >= len(patterns) and all(match(p, t) for p, t in zip(patterns, mparts)):
                result.add((len(patterns) == len(mparts), '.'.join(mparts[:len(patterns)])))

        for l, r in sorted(result):
            if l:
                yield LeafNode(r, None)
            else:
                yield BranchNode(r)

    @scream
    def fetch(self, patterns, start_time, stop_time, now=None, requestContext=None):
        metrics = self.reader.find_metrics(patterns)
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
