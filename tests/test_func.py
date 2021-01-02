import numpy as np
from hisser import func, dataset

from .helpers import make_ds, assert_naneq


def test_simple():
    ds = make_ds({
        'boo;foo=10': [1, 2, 3],
        'boo;foo=20': [4, 5, np.nan]
    })

    r = func.aggregate(None, ds, 'sum')
    assert_naneq(r.data, [[5, 7, 3]])

    r = func.aggregate(None, ds, 'sum', 'foo')
    assert_naneq(r.data, [[1, 2, 3], [4, 5, np.nan]])

    r = func.aggregate(None, dataset.Dataset('boo', [], np.empty((0, 10)), 0, 0, 0), 'sum')


def test_summarize():
    ds = make_ds({
        'boo;foo=10': [1, 2, 3],
        'boo;foo=20': [4, 5, np.nan]
    }, start=60)

    r = func.summarize(ds, 120, align=True)
    assert_naneq(r.data, [[3, 3], [9, np.nan]])
    assert r.start == 60

    r = func.summarize(ds, 120, align=False)
    assert_naneq(r.data, [[1, 5], [4, 5]])
    assert r.start == 0


def test_per_second():
    ds = make_ds({
        'boo;foo=10': [3, 5, 9],
        'boo;foo=20': [10, 1, 3]
    }, start=60)

    r = func.perSecond(ds)
    assert_naneq(r.data, [[np.nan, 2/60, 4/60], [np.nan, np.nan, 2/60]])


def test_derivative():
    ds = make_ds({
        'boo;foo=10': [3, np.nan, 9],
        'boo;foo=20': [1, 10, 15]
    }, start=60)

    r = func.derivative(ds)
    assert_naneq(r.data, [[np.nan, np.nan, 6], [np.nan, 9, 5]])


    r = func._derivative(np.array([[1, 3, np.nan, np.nan, 5, np.nan, 7, 9]]))
    assert_naneq(r, [[np.nan, 2, np.nan, np.nan, 2, np.nan, 2, 2]])


def test_set_tag():
    ds = make_ds({
        'boo;foo=10': [np.nan, 6, 9, 3, np.nan],
    }, start=60)

    r = func.setTag(ds, 'val', 'sum')
    assert r.names[0][0].tags['val'] == 18

    r = func.setTag(ds, 'val', 'total')
    assert r.names[0][0].tags['val'] == 18

    r = func.setTag(ds, 'val', 'avg')
    assert r.names[0][0].tags['val'] == 18/3

    r = func.setTag(ds, 'val', 'mean')
    assert r.names[0][0].tags['val'] == 18/3

    r = func.setTag(ds, 'val', 'median')
    assert r.names[0][0].tags['val'] == 6

    r = func.setTag(ds, 'val', 'max')
    assert r.names[0][0].tags['val'] == 9

    r = func.setTag(ds, 'val', 'min')
    assert r.names[0][0].tags['val'] == 3

    r = func.setTag(ds, 'val', 'last')
    assert r.names[0][0].tags['val'] == 3

    r = func.setTag(ds, 'val', 'first')
    assert r.names[0][0].tags['val'] == 6


def test_sort_by_total():
    ds = make_ds({
        'boo;foo=10': [3, np.nan, 9],
        'boo;foo=30': [np.nan, np.nan, np.nan],
        'boo;foo=20': [1, 10, 15]
    }, start=60)

    r = func.sortByTotal(ds)
    assert r.names == [('boo;foo=20', 2), ('boo;foo=10', 0), ('boo;foo=30', 1)]
