import numpy as np
from hisser.utils import cached_property, clone


def make_idx(names):
    return np.array([it[1] for it in names], dtype='l')


class Name:
    def __init__(self, name, tags=None):
        self.name = name
        if tags is not None:
            self.tags = tags

    @cached_property
    def tags(self):
        return parse_tags(self.name)[1]

    @cached_property
    def norm(self):
        t = ';'.join('{}={}'.format(*it) for it in self.tags.items() if it[0] != 'name')
        return self.name.partition(';')[0] + (';' + t if t else '')

    def __eq__(self, other):
        if type(other) is Name:  # pragma: no cover
            return other.norm == self.norm
        else:
            return self.norm == other

    def __repr__(self):
        return self.norm


class Dataset:
    def __init__(self, expr, names, data, start, stop, step, consolidate=None):
        self.expr = expr
        self.names = names
        self.data = data
        self.start = start
        self.step = step
        self.consolidate = consolidate
        self.implicit_consolidate = None

    def __repr__(self):
        return 'Dataset[metrics={}, points={}, {}...]'.format(len(self.names), self.points_count, self.names[:3])

    @property
    def points_count(self):
        if self.data is not None:
            return self.data.shape[1]
        else:
            return 0

    def normalize(self):
        data = self.data[make_idx(self.names)]
        names = [(it[0], i) for i, it in enumerate(self.names)]
        return clone(self, data=data, names=names)

    def items(self):
        for name, idx in self.names:
            yield name, self.data[idx]


def parse_tags(name):
    metric, _, tdata = name.partition(';')
    tags = {}
    if tdata:
        for part in tdata.split(';'):
            tname, _, tvalue = part.partition('=')
            tags[tname] = tvalue
    tags['name'] = metric
    return metric, tags
