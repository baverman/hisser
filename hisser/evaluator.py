import hisser.bsless

import math
from functools import lru_cache
from itertools import zip_longest
from datetime import datetime, timezone

from graphite.errors import NormalizeEmptyResultError
from graphite.functions import SeriesFunction, aggfuncs
from graphite.render.grammar import grammar
from graphite.render.datalib import TimeSeries
from graphite.render import functions
from graphite.storage import STORE

from hisser import pack, profile, jsonpoints, current
from hisser.graphite import Finder


_finder = None
def get_finder():
    global _finder
    if not _finder:  # pragma: no cover
        _finder = Finder(current.config)
        STORE.finders = [_finder]
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


def skip_xffValues(values, xFilesFactor):  # pragma: no cover
    return values

functions.xffValues = skip_xffValues

aggfuncs.aggFuncs['average'] = pack.safe_average
aggfuncs.aggFuncAliases['avg'] = pack.safe_average
aggfuncs.aggFuncs['sum'] = pack.safe_sum
aggfuncs.aggFuncAliases['total'] = pack.safe_sum
aggfuncs.aggFuncs['count'] = pack.safe_count

from graphite.render.functions import (
    getAggFunc, normalize, formatPathExpressions, SeriesFunctions)


def aggregate(requestContext, seriesList, func, xFilesFactor=None):  # pragma: no cover
  """
  Aggregate series using the specified function.

  Example:

  .. code-block:: none

    &target=aggregate(host.cpu-[0-7].cpu-{user,system}.value, "sum")

  This would be the equivalent of

  .. code-block:: none

    &target=sumSeries(host.cpu-[0-7].cpu-{user,system}.value)

  This function can be used with aggregation functions ``average``, ``median``, ``sum``, ``min``,
  ``max``, ``diff``, ``stddev``, ``count``, ``range``, ``multiply`` & ``last``.
  """
  # strip Series from func if func was passed like sumSeries
  rawFunc = func
  if func[-6:] == 'Series':
    func = func[:-6]

  consolidationFunc = getAggFunc(func, rawFunc)

  # if seriesList is empty then just short-circuit
  if not seriesList:
    return []

  # if seriesList is a single series then wrap it for normalize
  if isinstance(seriesList[0], TimeSeries):
    seriesList = [seriesList]

  try:
    (seriesList, start, end, step) = normalize(seriesList)
  except NormalizeEmptyResultError:
    return []
  name = "%sSeries(%s)" % (func, formatPathExpressions(seriesList))
  values = (consolidationFunc(row) for row in zip_longest(*seriesList))
  tags = seriesList[0].tags
  for series in seriesList:
    tags = {tag: tags[tag] for tag in tags if tag in series.tags and tags[tag] == series.tags[tag]}
  if 'name' not in tags:
    tags['name'] = name
  tags['aggregatedBy'] = func
  series = TimeSeries(name, start, end, step, values, xFilesFactor=xFilesFactor, tags=tags)
  return [series]


def alias(requestContext, seriesList, newName):
    """
    Takes one metric or a wildcard seriesList and a string in quotes.
    Prints the string instead of the metric name in the legend.

    .. code-block:: none

      &target=alias(Sales.widgets.largeBlue, "Large Blue Widgets {tag} {0}")
    """
    if type(seriesList) is TimeSeries:
        slist = [seriesList]
    else:
        slist = seriesList

    for it in slist:
        if it.tags:
            parts = it.tags.get('name', '').split('.')
        else:  # pragma: no cover
            parts = it.name.split('.')
        it.name = newName.format(*parts, **it.tags)

    return seriesList


functions.aggregate = aggregate
functions.alias = alias

SeriesFunctions['aggregate'] = aggregate
SeriesFunctions['alias'] = alias
