import hisser.bsless

import time
from datetime import datetime, timezone

from ujson import dumps
from covador import opt, item
from graphite.functions import SeriesFunctions, functionInfo
from graphite.storage import STORE
from graphite.metrics.views import tree_json

from hisser.http import Application, Response, params, query_string
from hisser import evaluator, profile

app = application = Application()


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
    ctx = evaluator.make_context(start, end)
    data = ctx['data']
    data.extend(evaluator.evaluate_target(ctx, targets))
    series_data = evaluator.filter_data(data, max_points)
    return series_data


@app.api('/functions')
def function_list(_req):
    result = {name: functionInfo(name, func)
              for name, func in SeriesFunctions().items()}
    return result


@app.api('/metrics/find')
@query_string(query=str)
def metrics_find(_req, query):
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
@query_string(
    exprs=opt(str, [], src='expr', multi=True),
    tag_prefix=opt(str, src='tagPrefix'),
    limit=opt(int)
)
def autocomplete_tags(_req, exprs, tag_prefix, limit):
    return STORE.tagdb_auto_complete_tags(
        exprs,
        tagPrefix=tag_prefix,
        limit=limit,
        requestContext={})


@app.api('/tags/autoComplete/values')
@query_string(
    exprs=opt(str, [], src='expr', multi=True),
    tag=str,
    value_prefix=opt(str, src='valuePrefix'),
    limit=opt(int)
)
def autocomplete_tag_values(_req, exprs, tag, value_prefix, limit):
  return STORE.tagdb_auto_complete_values(
      exprs,
      tag,
      valuePrefix=value_prefix,
      limit=limit,
      requestContext={})
