from math import isnan
from hisser.buffer import Buffer


def fnan(seq):
    return [None if isnan(r) else r for r in seq]


def test_empty_buffer():
    buf = Buffer(10, 10, 1.5, now=1000)
    result = buf.flush(5)
    assert result is None


def test_compact():
    buf = Buffer(10, 10, 1.5, now=1000)
    for i in range(10):
        buf.add(1000, f'm{i}', 1)

    buf.tick(now=1010)
    buf.tick(now=1310)


def norm_result(metric, data, names):
    mdata = dict(data and data[0] or {}).get(metric)
    return fnan(mdata) if mdata is not None else None, names and [it for it in names if it == metric]


def test_normap_op():
    buf = Buffer(10, 10, 1.5, now=1000)
    result = {}
    value = 1
    for ts in range(1000, 1260):
        data, new_names = buf.tick(now=ts)
        if data or new_names:
            result[ts] = norm_result('m1', data, new_names)

        if ts % 10 == 0:
            buf.add(ts+1, 'm1', value)
            value += 1

    assert result == {
        1000: (None, []),
        1010: (None, ['m1']),
        1150: ([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0], None),
        1250: ([11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0], None)
    }


def test_tick_with_gaps():
    buf = Buffer(10, 10, 1.5, now=1000)
    result = {}
    value = 1
    ticks = [1200, 1250]
    for ts in range(1000, 1260):
        if ts in ticks:
            data, new_names = buf.tick(now=ts)
            if data or new_names:
                result[ts] = norm_result('m1', data, new_names)

        if ts % 10 == 0:
            buf.add(ts+1, 'm1', value)
            value += 1

    assert result == {
        1200: ([None, None, None, None, None, 6.0, 7.0, 8.0, 9.0, 10.0], ['m1']),
        1250: ([11.0, 12.0, 13.0, 14.0, 15.0, None, None, None, None, None], None),
    }
