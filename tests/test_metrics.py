from hisser import metrics as api


def test_allocate_ids(tmpdir):
    fname = str(tmpdir.join('metrics.db'))
    mn = api.MetricNames(fname)

    assert mn.allocate_ids(1) == 1
    assert mn.allocate_ids(1) == 2
    assert mn.allocate_ids(2) == 3
    assert mn.allocate_ids(2) == 5


def test_simple(tmpdir):
    fname = str(tmpdir.join('metrics.db'))
    mn = api.MetricNames(fname)

    new_names, ids = mn.add(['boo', 'foo'], encoded=False)
    assert new_names == [b'boo', b'foo']
    assert ids == [b'\x00\x00\x00\x00\x00\x00\x00\x01', b'\x00\x00\x00\x00\x00\x00\x00\x02']

    new_names, ids = mn.add(['boo', 'foo'], encoded=False)
    assert new_names == []
    assert ids == [b'\x00\x00\x00\x00\x00\x00\x00\x01', b'\x00\x00\x00\x00\x00\x00\x00\x02']

    new_names, ids = mn.add(['bar'], encoded=False)
    assert new_names == [b'bar']
    assert ids == [b'\x00\x00\x00\x00\x00\x00\x00\x03']

    assert mn.get_ids([b'foo', b'bar', b'zoo']) == [b'\x00\x00\x00\x00\x00\x00\x00\x02',
                                                    b'\x00\x00\x00\x00\x00\x00\x00\x03',
                                                    None]

    ids = [b'\x00\x00\x00\x00\x00\x00\x00\x02', b'\x00\x00\x00\x00\x00\x00\x00\x01', b'b']
    assert mn.get_names(ids) == [b'foo', b'boo', None]


def test_metric_index(tmpdir):
    fname = str(tmpdir.join('metrics.db'))
    mi = api.MetricIndex(fname)

    mi.add({b'boo.foo': b'1', b'boo.bar': b'2'})
    mi.add({b'boo.foo': b'1', b'boo.bar': b'2'})
    mi.add({b'boo.foo': b'1', b'boo.bar': b'2'})
    mi.add({b'boo.foo': b'1', b'boo.bar.zoo': b'3'})
    mi.add({b'boo.foo': b'1', b'boo.bar.zoo': b'3'})
    result = list(mi.iter_terms())
    assert result == [(b'1boo', b'1'), (b'1boo', b'2'), (b'1boo', b'3'),
                      (b'2bar', b'2'), (b'2bar', b'3'), (b'2foo', b'1'), (b'3zoo', b'3')]


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

    mi.add_tree([b'boo', b'foo'])

    result = mi.find_metrics_many(['*'])
    assert result == {'*': [b'boo', b'foo']}

    result = mi.find_metrics_many(['b*'])
    assert result == {'b*': [b'boo']}


def test_prefix(tmpdir):
    fname = str(tmpdir.join('metrics.db'))
    mi = api.MetricIndex(fname)
    mi.add_tree([b'app1.inst1.boo', b'app1.inst1.foo', b'app1.inst2.foo'])

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
