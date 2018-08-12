from hisser import agg


def test_safe_sum():
    assert agg.safe_sum([1, 2, 3]) == 6
    assert agg.safe_sum([1, agg.NAN, 3]) == 4
    assert agg.isnan(agg.safe_sum([agg.NAN]))
    assert agg.isnan(agg.safe_sum([]))


def test_safe_avg():
    assert agg.safe_avg([1, 2, 3]) == 2
    assert agg.safe_avg([1, agg.NAN, 3]) == 2
    assert agg.isnan(agg.safe_avg([agg.NAN]))
    assert agg.isnan(agg.safe_avg([]))


def test_safe_min():
    assert agg.safe_min([1, 2, 3]) == 1
    assert agg.safe_min([1, agg.NAN, 3]) == 1
    assert agg.isnan(agg.safe_min([agg.NAN]))
    assert agg.isnan(agg.safe_min([]))


def test_safe_max():
    assert agg.safe_max([1, 2, 3]) == 3
    assert agg.safe_max([1, agg.NAN, 3]) == 3
    assert agg.isnan(agg.safe_max([agg.NAN]))
    assert agg.isnan(agg.safe_max([]))


def test_safe_last():
    assert agg.safe_last([1, 2, 3]) == 3
    assert agg.safe_last([1, agg.NAN, 3, agg.NAN]) == 3
    assert agg.isnan(agg.safe_last([agg.NAN]))
    assert agg.isnan(agg.safe_last([]))


def test_agg_rules():
    rules = ((r'\.count$', 'sum'), (r'^localhost\.', 'last'))
    ar = agg.AggRules(rules, 'min')

    names = ('boo.foo', 'boo.count', 'boo.last', 'boo.localhost', 'localhost.foo')
    result, default = ar.get_methods(names)
    assert default == agg.safe_min
    assert result == {'boo.count': agg.safe_sum, 'localhost.foo': agg.safe_last}

    names = (b'boo.foo', b'boo.count', b'boo.last', b'boo.localhost', b'localhost.foo')
    result, default = ar.get_methods(names, use_bin=True)
    assert default == agg.safe_min
    assert result == {b'boo.count': agg.safe_sum, b'localhost.foo': agg.safe_last}

    ar.get_method('boo.foo') == agg.safe_min
    ar.get_method('boo.count') == agg.safe_sum

    ar.get_method(b'boo.foo', use_bin=True) == agg.safe_min
    ar.get_method(b'boo.count', use_bin=True) == agg.safe_sum
