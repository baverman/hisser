from math import isnan
from hisser.buffer import Buffer


def fnan(seq):
    return [None if isnan(r) else r for r in seq]


def test_empty_buffer():
    buf = Buffer(30, 10, 5, 3, 5000, now=1000)
    result = buf.flush(5)
    assert result is None


def test_simple():
    buf = Buffer(30, 10, 5, 3, 5000, now=1000)
    buf.add(1000, 'm1', 1)
    buf.add(1010, 'm1', 2)
    buf.add(1020, 'm1', 3)
    buf.add(2000, 'm1', 3)
    buf.add(500, 'm1', 3)

    result = buf.get_data(['m1', 'm2'])
    result['result']['m1'] = fnan(result['result']['m1'])

    assert result == {'start': 970,
                      'result': {'m1': [None] * 3 + [1.0, 2.0, 3.0] + [None]*24},
                      'resolution': 10,
                      'size': 30}

    data, new_names = buf.tick(now=900)
    assert data is None
    assert new_names is None

    data, new_names = buf.tick(now=1000)
    assert data is None
    assert new_names is None

    data, new_names = buf.tick(now=1000)
    assert data is None
    assert new_names is None

    data, new_names = buf.tick(now=1010)
    assert data is None
    assert 'm1' in new_names

    (data, start, res, size, nn, cm), new_names = buf.tick(now=1100)
    d = dict(data)['m1']
    assert (start, res, size, nn, cm) == (970, 10, 5, [], 31)
    assert fnan(d) == [None, None, None, 1.0, 2.0]

    (data, start, res, size, nn, cm), new_names = buf.tick(now=1100 + 300)
    d = dict(data)['m1']
    assert (start, res, size, nn, cm) == (1020, 10, 30, [], 0)
    assert fnan(d) == [3.0] + [None] * 29

    (data, start, res, size, nn, cm), new_names = buf.tick(force=True, now=1500)
    assert 'm1' not in dict(data)
    assert (start, res, size, cm) == (1320, 10, 18, 14)
    assert nn
    assert fnan(d) == [3.0] + [None] * 29
