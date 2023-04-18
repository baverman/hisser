import os
BACKEND = os.environ.get('HISSER_INDEX_BACKEND') or 'lmdb'

if BACKEND == 'lmdb':
    from hisser.metrics_lmdb import *
elif BACKEND == 'sqlite':
    from hisser.metrics_sqlite import *
elif BACKEND == 'old':
    from hisser.metrics_old import *
else:
    raise ImportError('Invalid index backend: ' + BACKEND)
