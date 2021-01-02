import hisser.bsless

import math
from functools import lru_cache
from datetime import datetime, timezone

from graphite.render.grammar import grammar
from graphite.storage import STORE

from hisser import profile, jsonpoints, current, aggop
from hisser.graphite import Finder
from hisser.func import functions, AGGOP_ALIAS, consolidate_dataset


_finder = None
def get_finder():
    global _finder
    if not _finder:  # pragma: no cover
        _finder = Finder(current.config)
        STORE.finders = [_finder]
    return _finder


@profile.profile_func
def filter_data(ds_list, max_points):
    if not any(ds_list):  # pragma: no cover
        return []

    series_data = []

    for ds in filter(None, ds_list):
        consolidated = consolidate_dataset(ds, max_points)
        for name, points in consolidated.items():
            series_data.append(dict(target=name.name, tags=name.tags,
                                    datapoints=Datapoints(points, consolidated.start, consolidated.step)))

    return series_data


@lru_cache(16384)
@profile.profile_func
def get_eval_tree(expression):
    with profile.profile('parse'):
        root = grammar.parseString(expression)
    ctx = {'fetches': []}
    with profile.profile('build'):
        tree = build_eval_tree(ctx, root)
    tree.context = ctx
    return tree


class FetchNode:
    def __init__(self, expression):
        self.expression = expression

    def __repr__(self):  # pragma: no cover
        return 'FetchNode({})'.format(self.expression)

    @profile.profile_func('fetch-node')
    def __call__(self, ctx):
        cache = ctx.setdefault('data_cache', {})
        start_time = ctx['startTime'].timestamp()
        end_time = ctx['endTime'].timestamp()
        now = ctx['now'].timestamp()
        key = start_time, end_time, now, self.expression
        try:
            return cache[key]
        except KeyError:  # pragma: no cover
            return None


@profile.profile_func
def prefetch(ctx, paths):
    cache = ctx.setdefault('data_cache', {})
    start_time = ctx['startTime'].timestamp()
    end_time = ctx['endTime'].timestamp()
    now = ctx['now'].timestamp()
    keys = [(start_time, end_time, now, it) for it in paths]

    non_cached_paths = [it[-1] for it in keys if it not in cache]
    if non_cached_paths:
        results = get_finder().fetch(non_cached_paths, start_time, end_time, now, ctx)
        for it in results:
            key = (start_time, end_time, now, it.expr)
            cache[key] = it

        for k in [it[-1] for it in keys if it not in cache]: # pragma: no cover
            cache[k] = None


class FuncNode:
    def __init__(self, name, args, kwargs):
        self.name = name
        self.func = functions[name]
        self.args = args
        self.kwargs = kwargs
        self.need_context = getattr(self.func, 'need_context', None)

    def __repr__(self):  # pragma: no cover
        return '{}({}, {})'.format(self.name, self.args, self.kwargs)

    def __call__(self, ctx):
        with profile.profile('func ' + str(self.func)):
            args = [it(ctx) for it in self.args]
            kwargs = {key: value(ctx) for key, value in self.kwargs.items()}
            ctx['args'] = self.args
            if self.need_context:
                return self.func(ctx, *args, **kwargs)
            else:
                return self.func(*args, **kwargs)


class ScalarNode:
    def __init__(self, value):
        self.value = value

    def __repr__(self):  # pragma: no cover
        return str(self.value)

    def __call__(self, ctx):
        return self.value


def build_eval_tree(ctx, node, piped_arg=None):
    if node.expression:
        if node.expression.pipedCalls:
            right = node.expression.pipedCalls.pop()
            return build_eval_tree(ctx, right, node)
        return build_eval_tree(ctx, node.expression)

    if node.pathExpression:
        ctx['fetches'].append(node.pathExpression)
        return FetchNode(node.pathExpression)

    if node.call:
        if node.call.funcname == 'seriesByTag':
            ctx['fetches'].append(node.call.raw)
            return FetchNode(node.call.raw)

        args = node.call.args or []
        if piped_arg is not None:
            args.insert(0, piped_arg)
        args = [build_eval_tree(ctx, it) for it in args]
        kwargs = {it.argname: build_eval_tree(ctx, it.args[0])
                  for it in node.call.kwargs}
        return FuncNode(node.call.funcname, args, kwargs)

    if node.number:
        if node.number.integer:
            return ScalarNode(int(node.number.integer))
        if node.number.float:
            return ScalarNode(float(node.number.float))
        if node.number.scientific:
            return ScalarNode(float(node.number.scientific[0]))

    if node.string:
        return ScalarNode(node.string[1:-1])

    if node.boolean:
        return ScalarNode(node.boolean[0] == 'true')

    if node.none:  # pragma: no cover
        return ScalarNode(None)

    raise ValueError("unknown token in target evaluator")  # pragma: no cover


def fromtimestamp_with_tz(ts):
    return datetime.fromtimestamp(ts, timezone.utc)


def make_context(start, end, now=None, max_points=None):
    start = int(start)
    end = int(end)
    return {
        'startTime': fromtimestamp_with_tz(start),
        'endTime': fromtimestamp_with_tz(end),
        'now': now and fromtimestamp_with_tz(now) or datetime.now(timezone.utc),
        'localOnly': False,
        'template': {},
        'tzinfo': None,
        'forwardHeaders': False,
        'data': [],
        'prefetched': {},
        'xFilesFactor': 0,
        'max_points': max_points,
    }


@profile.profile_func
def evaluate_target(ctx, targets):
    if not isinstance(targets, list):
        targets = [targets]

    ds_list  = []
    tree_list = []

    for target in targets:
        if not target:
            continue

        if isinstance(target, str):
            if not target.strip():
                continue
            target = get_eval_tree(target)

        tree_list.append(target)

    fetch_paths = []
    for target in tree_list:
        fetch_paths.extend(target.context['fetches'])

    prefetch(ctx, fetch_paths)

    for target in tree_list:
        ds_list.append(target(ctx))

    return ds_list


def make_datapoints(series, values, vals_per_point):  # pragma: no cover
    timestamps = range(int(series.start), int(series.end) + 1, int(series.step * vals_per_point))
    return list(zip(values, timestamps))


class Datapoints:
    def __init__(self, values, start, step):
        self.values = values
        self.start = start
        self.step = step

    def __json__(self):
        return jsonpoints.datapoints_to_json_view(self.values, self.start, self.step)
