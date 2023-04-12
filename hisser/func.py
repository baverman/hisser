import re
import math

import numpy as np
import graphite.functions
from functools import lru_cache

from hisser import aggop
from hisser.utils import clone, parse_interval
from hisser.dataset import Name, make_idx, Dataset

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


def get_function(name):
    try:
        return functions[name]
    except KeyError:
        pass

    fn = functions[name] = get_fallback_function(name)
    return fn


@lru_cache(None)
def get_fallback_function(name):
    return graphite_adapter(graphite.functions.SeriesFunction(name))


def graphite_adapter(fn):
    def inner(*args, **kwargs):
        fargs = []
        for it in args:
            if type(it) is Dataset:
                fargs.append(it.tslist)
            else:
                fargs.append(it)

        result = fn(*fargs, **kwargs)
        return Dataset.from_tslist(result, args[1])
    inner.need_context = True
    inner.fallback = True
    return inner


def func(name=None, aliases=None, context=False):
    def decorator(fn):
        fn.need_context = context
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


@func(context=True, aliases=['groupByNodes', 'groupByTags', 'groupBy'])
def aggregate(ctx, ds, aggfunc, *nodes):
    try:
        fname = AGGOP_ALIAS[aggfunc]
    except KeyError:
        return get_fallback_function(ctx['current_func_name'])(ctx, ds, aggfunc, *nodes)

    expr = f'{fname}({ds.expr})'
    groups = {}
    if nodes:
        kf = keyfunc(nodes)
        for name, i in ds.names:
            key = kf(name)
            groups.setdefault(key, []).append((name, i))
    elif ds.names:
        groups = {(): ds.names}

    newdata = np.empty((len(groups), ds.data.shape[1]), dtype='d')
    newnames = []
    for i, (k, g) in enumerate(groups.items()):
        idx = make_idx(g)
        aggop.op_idx_t(fname, ds.data, idx, newdata[i,:])
        tags = dict(zip(nodes, k))
        tags['_gsize'] = len(g)
        newnames.append((Name(g[0][0].name, tags), i))
    return clone(ds, expr=expr, names=newnames, data=newdata,
                 implicit_consolidate=fname)


@func('sum', aliases=['sumSeries'])
def sum_agg(ds, *nodes):
    return aggregate(None, ds, 'sum', *nodes)


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
    _, delta = parse_interval(interval)
    wsize = math.ceil(delta / ds.step)
    if wsize <= 1:
        return ds

    newstep = ds.step * wsize
    if not align:
        start = ds.start // newstep * newstep
        offset = (ds.start - start) // ds.step
    else:
        start = ds.start
        offset = 0

    fname = AGGOP_ALIAS[func]
    newdata = aggop.op_idx_window(fname, ds.data, make_idx(ds.names), wsize, offset)
    names = [(it[0], i) for i, it in enumerate(ds.names)]
    return clone(ds, names=names, data=newdata, start=start, step=newstep, implicit_consolidate=fname)


@func()
def limit(ds, n):
    newnames = ds.names[:n]
    return clone(ds, names=newnames)


def nandiff1d(data):
    cond = ~np.isnan(data)
    result = np.full(len(data), np.nan, dtype='d')
    result[cond.nonzero()[0][1:]] = np.diff(data[cond])
    return result


def _derivative(data):
    if not len(data):
        return data
    return np.apply_along_axis(nandiff1d, 1, data)


@func()
def derivative(ds):
    newdata = _derivative(ds.data)
    return clone(ds, data=newdata)


@func()
def nonNegativeDerivative(ds):
    newdata = _derivative(ds.data)
    newdata[newdata < 0] = np.nan
    return clone(ds, data=newdata)


@func()
def perSecond(ds, max_value=None, min_value=None):
    newdata = _derivative(ds.data)
    newdata[newdata < 0] = np.nan
    factor = 1 / ds.step
    newdata *= factor
    return clone(ds, data=newdata)


def consolidate_dataset(ds, max_points, multiplier=1):
    """
      0  3  6  9
      ----------  offset = 0, newstart = oldstart
      *  *  *  *

      1 3  6  9
      ---------  offset = 2, newstart = 3
        *  *  *

      23  6  9
      --------  offset = 1, newstart = 3
       *  *  *
    """
    if max_points is None:
        return ds

    max_points = max_points // multiplier
    pcount = ds.points_count
    if pcount == 0 or pcount <= max_points:
        return ds

    if max_points <= 1:
        wsize = pcount
        newstart = ds.start
        newstep = int(wsize * ds.step)
        offset = 0
    else:
        wsize = math.ceil(pcount / max_points)
        newstep = int(wsize * ds.step)
        newstart = ds.start // newstep * newstep
        soffset = (newstart - ds.step) % newstep
        offset = soffset // ds.step
        newstart = ds.start + soffset

    fname = AGGOP_ALIAS[ds.consolidate or ds.implicit_consolidate or 'mean']
    newdata = aggop.op_idx_window(fname, ds.data[:,offset:], make_idx(ds.names), wsize, 0)
    names = [(it[0], i) for i, it in enumerate(ds.names)]
    return clone(ds, names=names, data=newdata, start=newstart, step=newstep,
                 consolidate=None, implicit_consolidate=None)


@func(context=True)
def consolidate(ctx, ds, func=None, multiplier=1):
    if func:
        ds.consolidate = func
    return consolidate_dataset(ds, ctx['max_points'], multiplier)


@func()
def sortByTotal(ds):
    totals = np.nansum(ds.data, axis=1)
    totals[np.isnan(totals)] = 0
    newnames = sorted(ds.names, key=(lambda r: totals[r[1]]), reverse=True)
    return clone(ds, names=newnames)


def first1d(data):
    idx = np.where(~np.isnan(data))[0]
    if len(idx):
        return data[idx[0]]
    else:
        return np.nan


def agg_all(data, kind):
    if kind in ('total', 'sum'):
        d = np.nansum(data, axis=1)
    elif kind in ('avg', 'mean'):
        d = np.nanmean(data, axis=1)
    elif kind == 'median':
        d = np.nanmedian(data, axis=1)
    elif kind == 'max':
        d = np.nanmax(data, axis=1)
    elif kind == 'min':
        d = np.nanmin(data, axis=1)
    elif kind == 'first':
        d = np.apply_along_axis(first1d, 1, data)
    elif kind == 'last':
        notnan = (~np.isnan(data)).cumsum(1).argmax(1)
        d = data[np.arange(data.shape[0]), notnan]
    else:
        raise Exception('Invalid kind')

    return d


@func('tag')
def setTag(ds, tag, kind):
    ds = ds.normalize()
    for it, value in zip(ds.names, agg_all(ds.data, kind)):
        it[0].tags[tag] = value

    return ds
