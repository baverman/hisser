import os
import ujson

import numpy as np
import pytest

from hisser import evaluator, config, func
from array import array

from .helpers import make_ds


@pytest.fixture
def finder(tmpdir):
    cfg = config.get_config({'DATA_DIR': str(tmpdir)})
    cfg.ensure_dirs()
    evaluator._finder = evaluator.Finder(cfg)
    return evaluator._finder


def test_datapoints():
    points = evaluator.Datapoints(array('d', range(10)), 100, 10)
    result = ujson.loads(ujson.dumps(points))
    assert result == [[0.0, 100], [1.0, 110], [2.0, 120], [3.0, 130],
                     [4.0, 140], [5.0, 150], [6.0, 160], [7.0, 170],
                     [8.0, 180], [9.0, 190]]

    values = array('d', [0.00001, 84.000234, 100000000, -100.100, -0.0000000056])
    points = evaluator.Datapoints(values, 100, 10)
    result = ujson.loads(ujson.dumps(points))
    assert result == [[1e-05, 100], [84.000234, 110],
                      [100000000.0, 120], [-100.1, 130], [-5.6e-09, 140]]

    values = array('d', [np.nan])
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
    finder.cfg.storage.new_names(new_names)
    finder.cfg.storage.new_block(data, 60, 60, 2)

    ctx = evaluator.make_context(60, 300)
    ds, = evaluator.evaluate_target(ctx, ['root.*'])
    assert ds.names == [('root.m1', 0), ('root.m2', 1)]
    assert ds.data.tolist() == [[10.0, 15.0],
                                [20.0, 25.0]]

    ctx = evaluator.make_context(60, 300)
    ds, = evaluator.evaluate_target(ctx, ['aliasByNode(sumSeries(root.*), 0)'])
    assert ds.names == [('root;_gsize=2', 0)]
    assert ds.data.tolist() == [[30.0, 40.0]]

    # fallback to graphite
    ctx = evaluator.make_context(60, 300)
    ds, = evaluator.evaluate_target(ctx, ['absolute(root.*)'])
    assert ds.names == [('absolute(root.m1);absolute=1', 0), ('absolute(root.m2);absolute=1', 1)]
    assert ds.data.tolist() == [[10, 15], [20, 25]]

    # fallback to graphite
    ctx = evaluator.make_context(60, 300)
    ds, = evaluator.evaluate_target(ctx, ['alias(absolute(root.*), "abs {name}")'])
    assert ds.names == [('abs root.m1;absolute=1', 0), ('abs root.m2;absolute=1', 1)]
    assert ds.data.tolist() == [[10, 15], [20, 25]]

    # fallback to unsupported agg
    ctx = evaluator.make_context(60, 300)
    ds, = evaluator.evaluate_target(ctx, ['groupByNodes(root.*, "median", 0)'])
    assert ds.names == [('root;aggregatedBy=median', 0)]
    assert ds.data.tolist() == [[15.0, 20.0]]

    ctx = evaluator.make_context(60, 300)
    ds, = evaluator.evaluate_target(ctx, [' ',
        'seriesByTag("name=tagged") | groupByTags("sum", "t1") | offset(0.5)'])
    assert ds.names == [('tagged;t1=v1;_gsize=2', 0)]
    assert ds.data.tolist() == [[30.5, 40.5]]

    ctx = evaluator.make_context(60, 300)
    ds, = evaluator.evaluate_target(ctx, [None,
        'seriesByTag("name=tagged") | groupByTags("sum", "t1") | offset(1e2)'])
    assert ds.names == [('tagged;t1=v1;_gsize=2', 0)]
    assert ds.data.tolist() == [[130, 140]]

    ctx = evaluator.make_context(60, 300)
    ds, = evaluator.evaluate_target(ctx,
        'seriesByTag("name=tagged") | groupByTags("sum", "t1") | offset(true)')
    assert ds.names == [('tagged;t1=v1;_gsize=2', 0)]
    assert ds.data.tolist() == [[31, 41]]


def test_filter():
    ds = make_ds({'boo': [1, 2, 3, 4, 5, 6]}, 10, 10)

    result, = ujson.loads(ujson.dumps(evaluator.filter_data([ds], 10)))
    assert result['datapoints'] == [
        [1.0, 10], [2.0, 20], [3.0, 30], [4.0, 40], [5.0, 50], [6.0, 60]]

    result, = ujson.loads(ujson.dumps(evaluator.filter_data([ds], 2)))
    assert result['datapoints'] == [
        [4.0, 30], [6, 60]]

    result, = ujson.loads(ujson.dumps(evaluator.filter_data([ds], 1)))
    assert result['datapoints'] == [[3.5, 10]]


def test_alias():
    ds = make_ds({'boo.bar;foo=10': [],
                  'boo.bar;foo=20': []})

    result = func.alias(ds, 'val: {1} {foo}')
    assert result.names == [('val: bar 10;foo=10', 0), ('val: bar 20;foo=20', 1)]

    result = func.alias(ds, 'val: {1} {zoo}')
    assert result.names == [('val: {1} {zoo};foo=10', 0), ('val: {1} {zoo};foo=20', 1)]
