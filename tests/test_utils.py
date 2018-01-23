import array
import pytest

from hisser import utils


def test_empty_rows():
    result = list(utils.empty_rows({'boo': array.array('d', [utils.NAN, utils.NAN]),
                                    'foo': array.array('d', [1, 2])}.items(), 2))
    assert result == ['boo']


def test_non_empty_rows():
    result = list(utils.non_empty_rows({'boo': array.array('d', [utils.NAN, utils.NAN]),
                                        'foo': array.array('d', [1, 2])}.items(), 2))
    assert result == [('foo', array.array('d', [1, 2]))]
