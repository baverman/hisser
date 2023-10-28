import hisser.bsless

import json
import time
import logging

from covador import opt, item
from graphite.functions import SeriesFunctions, functionInfo
from graphite.storage import STORE
from graphite.metrics.views import tree_json

from hisser.http import Application, Response, params
from hisser import evaluator, current, utils, profile

app = application = Application()
slow_log = logging.getLogger('hisser.slowlog')

try:
    import uwsgi
except ImportError:
    pass
else:
    current.config.setup_logging(daemon=True)


def parse_date(value):
    value = value.decode()
    now = int(time.time())
    if value == 'now':
        return now

    is_abs, delta = utils.parse_interval(value)
    if is_abs:
        return delta
    return now + delta


@app.api('/render')
@params(
    targets=opt(str, [], src='target', multi=True),
    start=item(parse_date, src='from'),
    end=item(parse_date, src='until'),
    max_points=opt(int, src='maxDataPoints'),
)
@profile.profile_func
def render(_req, targets, start, end, max_points):
    if max_points == 0: max_points = None
    ctx = evaluator.make_context(start, end, max_points=max_points)
    data = ctx['data']

    with profile.slowlog(current.config.SLOW_LOG, slow_log.warn,
                         'Slow query: %r %r %r %d', targets, start, end, max_points):
        data.extend(evaluator.evaluate_target(ctx, targets))
        series_data = evaluator.filter_data(data, max_points)

    return series_data


def jsonEncoder(obj):
  if hasattr(obj, 'toJSON'):
    return obj.toJSON()
  return obj.__dict__


@app.api('/functions')
def function_list(_req):
    result = {name: functionInfo(name, func)
              for name, func in SeriesFunctions().items()}
    result = Response(json.dumps(result, ensure_ascii=False, default=jsonEncoder),
                      status=200,
                      content_type='application/json')
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
