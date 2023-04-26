import pytest

from hisser import metrics_lmdb, metrics_sqlite

@pytest.fixture(params=['lmdb+native', 'lmdb+fast', 'sqlite'])
def api(monkeypatch, request):
    backend = request.param
    if backend == 'lmdb+native':
        monkeypatch.setattr(metrics_lmdb, 'FAST', False)
        m = metrics_lmdb
    elif backend == 'lmdb+fast':
        monkeypatch.setattr(metrics_lmdb, 'FAST', True)
        m = metrics_lmdb
    elif backend == 'sqlite':
        m = metrics_sqlite

    yield m


def test_simple_find(api, tmpdir):
    fname = str(tmpdir.join('metrics.db'))
    mi = api.MetricIndex(fname)

    mi.add([b'boo', b'foo'])

    result = mi.find_metrics_many(['z*'])
    assert result == {'z*': []}

    result = mi.find_metrics_many(['*'])
    assert result == {'*': [b'boo', b'foo']}

    result = mi.find_metrics_many(['b*'])
    assert result == {'b*': [b'boo']}


def test_prefix(api, tmpdir):
    fname = str(tmpdir.join('metrics.db'))
    mi = api.MetricIndex(fname)
    mi.add([b'app1.inst1.boo', b'app1.inst1.foo', b'app1.inst2.foo'])

    result = mi.find_metrics('app1.*.foo')
    assert result == [b'app1.inst1.foo', b'app1.inst2.foo']

    result = mi.find_metrics('app1.inst1.*')
    assert result == [b'app1.inst1.boo', b'app1.inst1.foo']

    result = mi.find_metrics('app1.*.f*')
    assert result == [b'app1.inst1.foo', b'app1.inst2.foo']

    result = mi.find_metrics('*.*.f*')
    assert result == [b'app1.inst1.foo', b'app1.inst2.foo']

    result = mi.find_tree('app1.*')
    assert result == [(False, b'app1.inst1'), (False, b'app1.inst2')]

    result = mi.find_tree('*')
    assert result == [(False, b'app1')]

    result = mi.find_tree('app1.inst1.*')
    assert result == [(True, b'app1.inst1.boo'), (True, b'app1.inst1.foo')]


def test_tags(api, tmpdir):
    fname = str(tmpdir.join('metrics.db'))
    bar = b'bar;dc=prod'
    boo = b'boo;dc=prod'
    foo = b'foo;dc=test;host=alpha'
    mi = api.MetricIndex(fname)
    mi.add([bar])
    mi.add([boo, foo])
    mi.add([bar, boo, foo])

    assert list(mi.iter_tags()) == [
        (b'dc', b'prod'), (b'dc', b'test'), (b'host', b'alpha'),
        (b'name', b'bar'), (b'name', b'boo'), (b'name', b'foo')]

    assert mi.get_tags() == [b'dc', b'host', b'name']
    assert mi.get_tag_values('dc') == [b'prod', b'test']
    assert mi.get_tag_values('host') == [b'alpha']
    assert mi.get_tag_values('foo') == []

    names = list(mi.iter_names())
    assert names == [
        b'bar;dc=prod', b'boo;dc=prod', b'foo;dc=test;host=alpha']

    # = empty
    result = mi.match_by_tags([('dc', '=', 'foo')])
    assert result == []

    # = op
    result = mi.match_by_tags([('dc', '=', 'prod')])
    assert set(result) == {bar, boo}

    result = mi.match_by_tags([('name', '=', 'foo')])
    assert set(result) == {foo}

    # != op
    result = mi.match_by_tags([('dc', '!=', 'prod')])
    assert set(result) == {foo}

    result = mi.match_by_tags([('dc', '!=', 'prod'), ('host', '=', 'alpha')])
    assert set(result) == {foo}

    # =~ op
    result = mi.match_by_tags([('dc', '=~', ':prod,test')])
    assert set(result) == {bar, boo, foo}

    result = mi.match_by_tags([('dc', '=~', ':stable')])
    assert set(result) == set()

    result = mi.match_by_tags([('name', '=~', '!bo*')])
    assert set(result) == {boo}

    result = mi.match_by_tags([('name', '=~', '!oo*')])
    assert set(result) == set()

    result = mi.match_by_tags([('dc', '=~', '(prod|test)')])
    assert set(result) == {bar, boo, foo}

    # !=~ op
    result = mi.match_by_tags([('dc', '!=~', ':prod,test')])
    assert set(result) == set()

    result = mi.match_by_tags([('dc', '!=~', ':stable')])
    assert set(result) == {bar, boo, foo}

    result = mi.match_by_tags([('name', '!=~', '!bo*')])
    assert set(result) == {bar, foo}

    result = mi.match_by_tags([('name', '!=~', '!oo*')])
    assert set(result) == {bar, boo, foo}

    result = mi.match_by_tags([('dc', '!=~', '(prod|test)')])
    assert set(result) == set()
