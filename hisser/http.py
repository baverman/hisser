from functools import wraps

from ujson import dumps
from covador import list_schema
from covador.utils import parse_qs
from covador.vdecorator import ValidationDecorator, ErrorHandler, mergeof
from covador.errors import error_to_json

from hisser.utils import cached_property


@ErrorHandler
def error_handler(ctx):
    return Response(error_to_json(ctx.exception),
                    content_type='application/json', status=400)


def get_qs(request):
    try:
        return request._covador_qs
    except AttributeError:
        qs = request._covador_qs = parse_qs(
            request.environ.get('QUERY_STRING', ''))
        return qs


def get_form(request):
    try:
        return request._covador_form
    except AttributeError:
        ctype = request.content_type or ''
        if ctype.startswith('application/x-www-form-urlencoded'):
            form = parse_qs(request.body)
        else:
            form = {}
        request._covador_form = form
        return form


query_string = ValidationDecorator(get_qs, error_handler, list_schema)
form = ValidationDecorator(get_form, error_handler, list_schema)
params = mergeof(query_string, form)

STATUS_CODES = {
    200: '200 OK',
    404: '404 NOT FOUND',
    400: '400 BAD REQUEST',
    500: '500 SERVER ERROR'
}


class Application:
    def __init__(self):
        self.routes = {}

    def api(self, path):
        def decorator(func):
            @wraps(func)
            def inner(req, *args, **kwargs):
                try:
                    result = func(req, *args, **kwargs)
                    status = 200
                except Exception as e:
                    status = 500
                    result = {'error': 'server-error',
                              'message': str(e)}

                if not isinstance(result, Response):
                    result = Response(dumps(result, ensure_ascii=False),
                                      status=status,
                                      content_type='application/json')
                return result
            return self.route(path)(inner)
        return decorator

    def route(self, path):
        def decorator(func):
            spath = path.rstrip('/')
            self.routes[spath] = func
            self.routes[spath + '/'] = func
            return func
        return decorator

    def __call__(self, env, sr):
        path = env['PATH_INFO']
        try:
            fn = self.routes[path]
        except KeyError:
            resp = Response('Not found', 404, content_type='text/plain')
        else:
            req = Request(env)
            try:
                resp = fn(req)
            except Exception:
                resp = Response('Internal server error', 500,
                                content_type='text/plain')

        return [resp.render(sr)]


class Request:
    def __init__(self, environ):
        self.environ = environ

    @property
    def content_type(self):
        return self.environ.get('CONTENT_TYPE')

    @cached_property
    def body(self):
        return self.environ['wsgi.input'].read()


class Response:
    def __init__(self, body, status=200, content_type=None, headers={}):
        if isinstance(body, str):
            body = body.encode()

        self.body = body
        self.status = status
        self.headers = headers
        self.headers['Content-Length'] = str(len(self.body))
        if content_type:
            self.headers['Content-Type'] = content_type

    def render(self, sr):
        sr(STATUS_CODES[self.status], list(self.headers.items()))
        return self.body
