from hisser.utils import cached_property


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
    def __init__(self, expr, names, data, start, end, step, consolidate=None):
        self.expr = expr
        self.names = names
        self.data = data
        self.start = start
        self.end = end
        self.step = step
        self.consolidate = consolidate


def parse_tags(name):
    metric, _, tdata = name.partition(';')
    tags = {}
    if tdata:
        for part in tdata.split(';'):
            tname, _, tvalue = part.partition('=')
            tags[tname] = tvalue
    tags['name'] = metric
    return metric, tags
