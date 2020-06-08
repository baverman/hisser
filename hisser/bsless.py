import sys
import logging

Module = type(sys)

lm = Module('graphite.logger')
sys.modules['graphite.logger'] = lm
lm.log = logging.getLogger('graphite')
cache_log = logging.getLogger('graphite.cache')
rendering_log = logging.getLogger('graphite.rendering')
lm.log.cache = lambda msg, *args, **kwargs: cache_log.info(msg, *args, **kwargs)
lm.log.rendering = lambda msg, *args, **kwargs: rendering_log.info(msg, *args, **kwargs)


class DjangoSetting:
    STORAGE_FINDERS = ()
    TAGDB = None
    DATE_FORMAT = '%Y-%m-%d'
    FUNCTION_PLUGINS = []
    METRICS_FIND_WARNING_THRESHOLD = float('Inf')
    METRICS_FIND_FAILURE_THRESHOLD = float('Inf')
    FIND_TIMEOUT = 3.0
    FETCH_TIMEOUT = 6.0
    USE_WORKER_POOL = False
    DEFAULT_XFILES_FACTOR = 0.0


class FakeModule(type(sys)):
    def __init__(self, name, **kwargs):
        super().__init__(name)
        vars(self).update(kwargs)

    def __getattr__(self, name):
        def inner(*args, **kwargs):  # pragma: no cover
            raise Exception('function {} not implemented')
        return inner


sys.modules['django'] = Module('django')
sys.modules['django.conf'] = FakeModule('django.conf', settings=DjangoSetting)
sys.modules['django.core.cache'] = FakeModule('django.core.cache')
sys.modules['django.utils.timezone'] = FakeModule('django.utils.timezone')
sys.modules['django.http'] = FakeModule('django.http')

sys.modules['graphite.compat'] = FakeModule('graphite.compat')
sys.modules['graphite.events.models'] = Module('graphite.events.models')
sys.modules['graphite.user_util'] = FakeModule('graphite.user_util')
