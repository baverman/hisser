import re
import numpy as np

from hisser import aggop
from hisser.utils import clone, parse_interval
from hisser.dataset import Name

functions = {}

AGGOP_ALIAS = {
    'sum': 'sum',
    'total': 'sum',
    'first': 'first',
    'last': 'last',
    'current': 'last',
    'avg': 'mean',
    'average': 'mean',
    'mean': 'mean',
    'min': 'min',
    'max': 'max',
    'count': 'count',
}


def make_idx(data):
    return np.array(list(data), dtype='l')


def func(name=None, aliases=None):
    def decorator(fn):
        lname = name or fn.__name__
        functions[lname] = fn
        for lname in aliases or ():
            functions[lname] = fn
        return fn
    return decorator


def keyfunc(nodes):
    def inner(name):
        return tuple(name.tags.get(it, None) for it in nodes)
    return inner


@func(aliases=['groupByNode', 'groupByTags', 'groupBy'])
def aggregate(ds, aggfunc, *nodes):
    fname = AGGOP_ALIAS[aggfunc]
    expr = f'{fname}({ds.expr})'
    if nodes:
        kf = keyfunc(nodes)
        groups = {}
        for name, i in ds.names:
            key = kf(name)
            groups.setdefault(key, []).append((name, i))
    else:
        groups = {(): ds.names}

    newdata = np.empty((len(groups), ds.data.shape[1]), dtype='d')
    newnames = []
    for i, (k, g) in enumerate(groups.items()):
        idx = make_idx(it for _, it in g)
        aggop.op_idx_t(fname, ds.data, idx, newdata[i,:])
        tags = dict(zip(nodes, k))
        tags['_gsize'] = len(g)
        newnames.append((Name(g[0][0].name, tags), i))
    return clone(ds, expr=expr, names=newnames, data=newdata, consolidate=fname)


@func('sum', aliases=['sumSeries'])
def sum_agg(ds, *nodes):
    return aggregate(ds, 'sum', *nodes)


@func()
def scaleToSeconds(ds, seconds):  # pragma: no cover
    if seconds == ds.step:
        return ds
    factor = seconds / ds.step
    newdata = ds.data * factor
    return clone(ds, data=newdata)


@func()
def consolidateBy(ds, func):
    return clone(ds, consolidate=func)  # pragma: no cover


@func()
def alias(ds, template):
    """
    Takes one metric or a wildcard seriesList and a string in quotes.
    Prints the string instead of the metric name in the legend.

    .. code-block:: none

      &target=alias(Sales.widgets.largeBlue, "Large Blue Widgets {tag} {0}")
    """

    newnames = []
    for it, i in ds.names:
        parts = it.name.partition(';')[0].split('.')
        try:
            name = Name(template.format(*parts, **it.tags), it.tags)
        except:
            name = Name(template, it.tags)
        newnames.append((name, i))

    return clone(ds, names=newnames)


@func(aliases=['aliasByTags'])
def aliasByNode(ds, *nodes):
    return alias(ds, '.'.join('{{{}}}'.format(it) for it in nodes))


@func()
def offset(ds, value):
    return clone(ds, data=ds.data + value)


@func()
def exclude(ds, pattern):
    regex = re.compile(pattern)
    newnames = [it for it in ds.names if regex.search(it[0].name)]
    return clone(ds, names=newnames)


@func()
def summarize(ds, interval, func='sum', align=False):
    is_abs, delta = parse_interval(interval)
    assert not is_abs
    # TODO
    return ds


@func()
def limit(ds, n):
    newnames = ds.names[:n]
    return clone(ds, names=newnames)


@func()
def perSecond(ds, max_value=None, min_value=None):
    # TODO
    return ds
