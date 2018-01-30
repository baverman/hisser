from hisser import metrics as api


def test_make_tree():
    result = list(api.make_tree([b'boo', b'foo']))
    assert result == [(b'.', b'boo'), (b'.', b'foo')]

    result = list(api.make_tree([b'boo.foo.bar']))
    assert result == [(b'.', b'boo'),
                      (b'boo', b'foo'),
                      (b'boo.foo', b'bar')]

    result = list(api.make_tree([b'boo.bar', b'boo.foo']))
    assert result == [(b'.', b'boo'),
                      (b'boo', b'bar'),
                      (b'boo', b'foo')]

    result = list(api.make_tree([b'app1.inst1.boo', b'app1.inst1.foo', b'app1.inst2.foo']))
    assert result == [(b'.', b'app1'),
                      (b'app1', b'inst1'),
                      (b'app1.inst1', b'boo'),
                      (b'app1.inst1', b'foo'),
                      (b'app1', b'inst2'),
                      (b'app1.inst2', b'foo')]

def test_query_parts():
    assert api.query_parts('localhost.boo.*.boo.foo') == (b'localhost.boo', [b'*', b'boo', b'foo'])
    assert api.query_parts('[abc].boo.foo') == (b'', [b'[abc]', b'boo', b'foo'])


def test_simple_find(tmpdir):
    fname = str(tmpdir.join('metrics.db'))
    mi = api.MetricIndex(fname)

    mi.add([b'boo', b'foo'])

    result = list(mi.iter_tree())
    assert result == [(b'.', b'boo'), (b'.', b'foo')]

    result = mi.find_metrics_many(['*'])
    assert result == {'*': [b'boo', b'foo']}

    result = mi.find_metrics_many(['b*'])
    assert result == {'b*': [b'boo']}


def test_prefix(tmpdir):
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
