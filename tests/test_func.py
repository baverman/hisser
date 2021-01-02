import numpy as np
from hisser import func

from .helpers import make_ds


def test_simple():
    ds = make_ds({
        'boo;foo=10': [1, 2, 3],
        'boo;foo=20': [4, 5, np.nan]
    })

    r = func.aggregate(ds, 'sum')
    print(r.names, r.data)
    r = func.aggregate(ds, 'sum', 'foo')
    print(r.names, r.data)
    r = func.alias(func.scaleToSeconds(ds, 1), 'zoo:{foo}')
    print(r.names, r.data)
