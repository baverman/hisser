import numpy as np
from hisser.dataset import Dataset, Name


def make_ds(series, start=0, step=60):
    names = [(Name(it), i) for i, it in enumerate(series)]
    data = np.empty((len(names), len(series[names[0][0].name])), dtype='d')
    for n, i in names:
        data[i,:] = series[n.name]
    return Dataset('expr', names, data, start, None, step)


def assert_naneq(actual, expected):
    e = np.array(expected, dtype='d')
    assert np.allclose(actual, e, equal_nan=True)
