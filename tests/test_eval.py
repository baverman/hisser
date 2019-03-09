import os
os.environ['HISSER_DATA_DIR'] = '/tmp'
from hisser import evaluator

def _test_simple(tmpdir):
    print('@@', evaluator.get_eval_tree('scaleToSeconds(boo.foo,boo.zoo, 1)'))
    assert False


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


def test_make_datapoints():
    values = range(10)
    result = evaluator.pack.make_datapoints(values, 100, 10)
    assert result == [(0, 100), (1, 110), (2, 120), (3, 130),
                     (4, 140), (5, 150), (6, 160), (7, 170), (8, 180), (9, 190)]
