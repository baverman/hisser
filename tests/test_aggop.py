import numpy as np
from hisser import aggop

from .helpers import assert_naneq


def test_indexed_transposed():
    d = np.array([[1, 2, np.nan],
                  [4, 5, 6],
                  [7, np.nan, np.nan]], dtype='d')
    idx = np.array([0, 2], dtype='l')
    r = aggop.op_idx_t('sum', d, idx, None)
    assert_naneq(r, [8, 2, np.nan])


def test_moving():
    d = np.array([1, 2, 3, np.nan, 1, 2, np.nan, np.nan, np.nan], dtype='d')
    r = aggop.op_window('sum', d, 3, 0)
    assert_naneq(r, [6, 3, np.nan])
    del r

    r = aggop.op_window('sum', d, 3, 1)
    assert_naneq(r, [1, 5, 3, np.nan])

    r = aggop.op_window('sum', d, 3, 2)
    assert_naneq(r, [3, 4, 2, np.nan])


def test_idx_window():
    d = np.array([[1, 2, np.nan],
                  [4, 5, 6],
                  [7, np.nan, np.nan]], dtype='d')
    idx = np.array([2, 0], dtype='l')
    r = aggop.op_idx_window('sum', d, idx, 2, 0)
    assert_naneq(r, [[7, np.nan], [3, np.nan]])
