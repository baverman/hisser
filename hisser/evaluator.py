import hisser.bsless

import math
from functools import lru_cache
from datetime import datetime, timezone

from graphite.errors import NormalizeEmptyResultError
from graphite.functions import SeriesFunction
from graphite.render.grammar import grammar
from graphite.render.datalib import TimeSeries
from graphite.render import functions

from hisser import pack, profile, jsonpoints
from hisser.graphite import Finder


_finder = None
def get_finder():
    global _finder
    if not _finder:  # pragma: no cover
        _finder = Finder()
    return _finder


# @profile.profile_func
def filter_data(data, max_points):
    if not any(data):  # pragma: no cover
        return []

    series_data = []
    startTime = min([series.start for series in data])
    endTime = max([series.end for series in data])
    timeRange = endTime - startTime

    for series in data:
        if max_points == 1:
            datapoints = consolidate(series, len(series))
        else:
            numberOfDataPoints = timeRange/series.step
            if max_points < numberOfDataPoints:
                valuesPerPoint = math.ceil(float(numberOfDataPoints) / float(max_points))
                secondsPerPoint = int(valuesPerPoint * series.step)
                # Nudge start over a little bit so that the consolidation bands align with each call
                # removing 'jitter' seen when refreshing.
                nudge = secondsPerPoint + (series.start % series.step) - (series.start % secondsPerPoint)
                series.start = series.start + nudge
                valuesToLose = int(nudge/series.step)
                for _ in range(1, valuesToLose):
                    del series[0]
                datapoints = consolidate(series, valuesPerPoint)
            else:
                datapoints = Datapoints(series, series.start, series.step)

        series_data.append(dict(target=series.name, tags=series.tags, datapoints=datapoints))

    return series_data


@lru_cache(16384)
def get_eval_tree(expression):
    root = grammar.parseString(expression)
    ctx = {'fetches': []}
    tree = build_eval_tree(ctx, root)
    tree.context = ctx
    return tree


class FetchNode:
    def __init__(self, expression):
        self.expression = expression

    def __repr__(self):  # pragma: no cover
        return 'FetchNode({})'.format(self.expression)

    def __call__(self, ctx):
        cache = ctx.setdefault('data_cache', {})
        start_time = ctx['startTime'].timestamp()
        end_time = ctx['endTime'].timestamp()
        now = ctx['now'].timestamp()
        key = start_time, end_time, now, self.expression
        try:
            data = cache[key]
        except KeyError:  # pragma: no cover
            data = []

        return [TimeSeries(path, start, end, step, values,
                           xFilesFactor=0, pathExpression=self.expression)
                for path, (start, end, step), values in data]


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
            key = (start_time, end_time, now, it['pathExpression'])
            cache.setdefault(key, []).append((it['name'], it['time_info'], it['values']))

        for k in [it[-1] for it in keys if it not in cache]: # pragma: no cover
            cache[k] = []


class FuncNode:
    def __init__(self, name, args, kwargs):
        self.name = name
        self.func = SeriesFunction(name)
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):  # pragma: no cover
        return '{}({}, {})'.format(self.name, self.args, self.kwargs)

    def __call__(self, ctx):
        args = [it(ctx) for it in self.args]
        kwargs = {key: value(ctx) for key, value in self.kwargs.items()}
        ctx['args'] = self.args
        try:
            return self.func(ctx, *args, **kwargs)
        except NormalizeEmptyResultError:  # pragma: no cover
            return []


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


def make_context(start, end, now=None):
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
        'xFilesFactor': 0
    }


# @profile.profile_func
def evaluate_target(ctx, targets):
    if not isinstance(targets, list):
        targets = [targets]

    series_list  = []
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
        result = target(ctx)
        if isinstance(result, TimeSeries):  # pragma: no cover
            series_list.append(result)
        elif result:
            series_list.extend(result)

    return series_list

functions.evaluateTarget = evaluate_target


# @profile.profile_func
def consolidate(series, vals_per_point):
    try:
        cf = consolidation_functions[series.consolidationFunc]
    except KeyError:  # pragma: no cover
        raise Exception("Invalid consolidation function: '%s'" % series.consolidationFunc)

    # with profile.profile('make-values'):
    values = cf(series, vals_per_point)

    # with profile.profile('make-points'):
    return Datapoints(values, series.start, series.step * vals_per_point)


def make_datapoints(series, values, vals_per_point):  # pragma: no cover
    timestamps = range(int(series.start), int(series.end) + 1, int(series.step * vals_per_point))
    return list(zip(values, timestamps))


class Datapoints:
    def __init__(self, values, start, step):
        self.values = values
        self.start = start
        self.step = step

    def __json__(self):
        return jsonpoints.datapoints_to_json(self.values, self.start, self.step)


def moving_func(fn):
    def inner(seq, size):
        if not seq:
            return []
        result = [None] * ((len(seq) + size - 1) // size)
        fn(seq, size, result)
        return result
    return inner


consolidation_functions = {
    'sum': moving_func(pack.moving_sum),
    'average': moving_func(pack.moving_average),
    'max': moving_func(pack.moving_max),
    'min': moving_func(pack.moving_min),
    'first': moving_func(pack.moving_first),
    'last': moving_func(pack.moving_last),
}
