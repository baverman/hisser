import os
import ujson

import pytest
from hisser import evaluator, config
from array import array


@pytest.fixture
def finder(tmpdir):
    cfg = config.get_config({'DATA_DIR': str(tmpdir)})
    cfg.ensure_dirs()
    evaluator._finder = evaluator.Finder(cfg)
    return evaluator._finder


def test_avg():
    fn = evaluator.consolidation_functions['average']
    fn([1, 2, 3], 2) == [1.5, 3]
    fn([1, 2, 3, 4], 2) == [1.5, 3.5]
    fn([1, None, 3, 4], 2) == [1, 3.5]


def test_sum():
    fn = evaluator.consolidation_functions['sum']
    fn([1, 2, 3], 2) == [3, 3]
    fn([1, 2, 3, 4], 2) == [3, 7]
    fn([1, None, 3, 4], 2) == [1, 7]


def test_min():
    fn = evaluator.consolidation_functions['min']
    fn([1, 2, 3], 2) == [1, 3]
    fn([1, 2, 3, 4], 2) == [1, 3]
    fn([1, None, 3, 4], 2) == [1, 3]


def test_max():
    fn = evaluator.consolidation_functions['max']
    fn([1, 2, 3], 2) == [2, 3]
    fn([1, 2, 3, 4], 2) == [2, 4]
    fn([1, None, 3, 4], 2) == [1, 4]


def test_first():
    fn = evaluator.consolidation_functions['first']
    fn([1, 2, 3], 2) == [1, 3]
    fn([1, 2, 3, 4], 2) == [1, 3]
    fn([1, None, None, 4], 2) == [1, 4]
    fn([None, 2, None], 2) == [2, None]


def test_last():
    fn = evaluator.consolidation_functions['last']
    fn([1, 2, 3], 2) == [2, 3]
    fn([1, 2, 3, 4], 2) == [2, 4]
    fn([1, None, None, 4], 2) == [1, 4]
    fn([None, 2, None], 2) == [2, None]


def test_bounds():
    fn = evaluator.consolidation_functions['sum']
    fn([], 3) == []
    fn([1], 3) == [1]
    fn([1, 2], 3) == [3]
    fn([1, 2, 3], 3) == [6]
    fn([1, 2, 3, 4], 3) == [6, 4]


def test_datapoints():
    points = evaluator.Datapoints(list(range(10)), 100, 10)
    result = ujson.loads(ujson.dumps(points))
    assert result == [[0.0, 100], [1.0, 110], [2.0, 120], [3.0, 130],
                     [4.0, 140], [5.0, 150], [6.0, 160], [7.0, 170],
                     [8.0, 180], [9.0, 190]]

    values = [0.00001, 84.000234, 100000000, -100.100, -0.0000000056]
    points = evaluator.Datapoints(values, 100, 10)
    result = ujson.loads(ujson.dumps(points))
    assert result == [[1e-05, 100], [84.000234, 110],
                      [100000000.0, 120], [-100.1, 130], [-5.6e-09, 140]]

    values = [None]
    points = evaluator.Datapoints(values, 100, 10)
    result = ujson.loads(ujson.dumps(points))
    assert result == [[None, 100]]


def make_data(data):
    result = [(name.encode(), array('d', values)) for name, values in data.items()]
    return result, [it for it, _ in result]


def test_eval(finder):
    data, new_names = make_data({
        'root.m1': (10, 15), 'root.m2': (20, 25),
        'tagged;t1=v1;t2=v2': (10, 15),
        'tagged;t1=v1;t2=v3': (20, 25)})
    finder.cfg.storage.new_block(data, 60, 60, 2, new_names)

    ctx = evaluator.make_context(60, 300)
    m1, m2 = evaluator.evaluate_target(ctx, ['root.*'])
    assert m1.name == 'root.m1'
    assert list(m1) == [10.0, 15.0]
    assert m2.name == 'root.m2'
    assert list(m2) == [20.0, 25.0]

    ctx = evaluator.make_context(60, 300)
    result, = evaluator.evaluate_target(ctx, ['aliasByNode(sumSeries(root.*), 0, "boo")'])
    assert result.name == 'root.'
    assert list(result) == [30.0, 40.0]

    ctx = evaluator.make_context(60, 300)
    result, = evaluator.evaluate_target(ctx, [' ',
        'seriesByTag("name=tagged") | groupByTags("sum", "t1") | offset(0.5)'])
    assert result.name == 'offset(tagged;t1=v1,0.5)'
    assert list(result) == [30.5, 40.5]

    ctx = evaluator.make_context(60, 300)
    result, = evaluator.evaluate_target(ctx, [None,
        'seriesByTag("name=tagged") | groupByTags("sum", "t1") | offset(1e2)'])
    assert result.name == 'offset(tagged;t1=v1,100)'
    assert list(result) == [130, 140]

    ctx = evaluator.make_context(60, 300)
    result, = evaluator.evaluate_target(ctx,
        'seriesByTag("name=tagged") | groupByTags("sum", "t1") | offset(true)')
    assert result.name == 'offset(tagged;t1=v1,1)'
    assert list(result) == [31, 41]


def test_filter():
    ts = evaluator.TimeSeries('boo', 10, 60, 10, [1, 2, 3, 4, 5, 6])

    result, = ujson.loads(ujson.dumps(evaluator.filter_data([ts], 10)))
    assert result['datapoints'] == [
        [1.0, 10], [2.0, 20], [3.0, 30], [4.0, 40], [5.0, 50], [6.0, 60]]

    result, = ujson.loads(ujson.dumps(evaluator.filter_data([ts], 2)))
    assert result['datapoints'] == [
        [3.0, 30], [5.5, 60]]

    ts = evaluator.TimeSeries('boo', 10, 60, 10, [1, 2, 3, 4, 5, 6])
    result, = ujson.loads(ujson.dumps(evaluator.filter_data([ts], 1)))
    assert result['datapoints'] == [[3.5, 10]]


def test_alias():
    ts1 = evaluator.TimeSeries('boo.bar;foo=10', 10, 60, 10, [])
    ts2 = evaluator.TimeSeries('boo.bar;foo=20', 10, 60, 10, [])

    result = evaluator.alias(None, ts1, 'val: {1} {foo}')
    assert result.name == 'val: bar 10'

    result = evaluator.alias(None, ts1, 'val: {1} {zoo}')
    assert result.name == 'val: {1} {zoo}'

    r1, r2 = evaluator.alias(None, [ts1, ts2], 'val: {0} {foo}')
    assert r1.name == 'val: boo 10'
    assert r2.name == 'val: boo 20'
