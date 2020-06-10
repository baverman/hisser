import hisser.bsless

import time
import logging

from covador import opt, item
from graphite.functions import SeriesFunctions, functionInfo
from graphite.storage import STORE
from graphite.metrics.views import tree_json

from hisser.http import Application, Response, params
from hisser import evaluator, current

app = application = Application()
slow_log = logging.getLogger('hisser.slowlog')


def parse_date(value):
    value = value.decode()
    now = time.time()
    if value == 'now':
        return now
    elif value.endswith('min'):
        return now + int(value[:-3]) * 60
    elif value.endswith('h'):
        return now + int(value[:-1]) * 3600
    elif value.endswith('d'):
        return now + int(value[:-1]) * 86400
    elif value.endswith('w'):
        return now + int(value[:-1]) * 86400 * 7
    elif value.endswith('mon'):
        return now + int(value[:-3]) * 86400 * 30
    elif value.endswith('y'):
        return now + int(value[:-1]) * 86400 * 365
    else:
        return int(value)


@app.api('/render')
@params(
    targets=opt(str, [], src='target', multi=True),
    start=item(parse_date, src='from'),
    end=item(parse_date, src='until'),
    max_points=opt(int, src='maxDataPoints'),
)
def render(_req, targets, start, end, max_points):
    # print(targets, start, end, max_points)
    ctx = evaluator.make_context(start, end)
    data = ctx['data']

    dstart = time.perf_counter()

    data.extend(evaluator.evaluate_target(ctx, targets))
    series_data = evaluator.filter_data(data, max_points)

    duration = time.perf_counter() - dstart
    if duration > current.config.SLOW_LOG:
        slow_log.warn('Slow query: %dms %r %r %r %d', round(duration * 1000),
                     targets, start, end, max_points)

    return series_data


@app.api('/functions')
def function_list(_req):
    result = {name: functionInfo(name, func)
              for name, func in SeriesFunctions().items()}
    return result


@app.api('/metrics/find')
@params(query=str)
def metrics_find(_req, query):
    evaluator.get_finder()
    if '.' in query:
        base_path = query.rsplit('.', 1)[0] + '.'
    else:
        base_path = ''

    matches = list(STORE.find(query, -1, -1, local=True,
                              headers=False, leaves_only=False))
    matches.sort(key=lambda node: node.name)
    result = tree_json(matches, base_path, wildcards=False)
    return result


@app.api('/tags/autoComplete/tags')
@params(
    exprs=opt(str, [], src='expr', multi=True),
    tag_prefix=opt(str, src='tagPrefix'),
    limit=opt(int)
)
def autocomplete_tags(_req, exprs, tag_prefix, limit):
    evaluator.get_finder()
    return STORE.tagdb_auto_complete_tags(
        exprs,
        tagPrefix=tag_prefix,
        limit=limit,
        requestContext={})


@app.api('/tags/autoComplete/values')
@params(
    exprs=opt(str, [], src='expr', multi=True),
    tag=str,
    value_prefix=opt(str, src='valuePrefix'),
    limit=opt(int)
)
def autocomplete_tag_values(_req, exprs, tag, value_prefix, limit):
    evaluator.get_finder()
    return STORE.tagdb_auto_complete_values(
        exprs,
        tag,
        valuePrefix=value_prefix,
        limit=limit,
        requestContext={})


@app.api('/version')
def version(_req):
    return Response('1.1.7')
