import pytest

from hisser import utils


def test_safe_sum():
    assert utils.safe_sum([1, 2, 3]) == 6
    assert utils.safe_sum([1, utils.NAN, 3]) == 4
    assert utils.isnan(utils.safe_sum([utils.NAN]))
    assert utils.isnan(utils.safe_sum([]))


def test_safe_avg():
    assert utils.safe_avg([1, 2, 3]) == 2
    assert utils.safe_avg([1, utils.NAN, 3]) == 2
    assert utils.isnan(utils.safe_avg([utils.NAN]))
    assert utils.isnan(utils.safe_avg([]))


def test_safe_min():
    assert utils.safe_min([1, 2, 3]) == 1
    assert utils.safe_min([1, utils.NAN, 3]) == 1
    assert utils.isnan(utils.safe_min([utils.NAN]))
    assert utils.isnan(utils.safe_min([]))


def test_safe_max():
    assert utils.safe_max([1, 2, 3]) == 3
    assert utils.safe_max([1, utils.NAN, 3]) == 3
    assert utils.isnan(utils.safe_max([utils.NAN]))
    assert utils.isnan(utils.safe_max([]))


def test_safe_last():
    assert utils.safe_last([1, 2, 3]) == 3
    assert utils.safe_last([1, utils.NAN, 3, utils.NAN]) == 3
    assert utils.isnan(utils.safe_last([utils.NAN]))
    assert utils.isnan(utils.safe_last([]))


def test_parse_seconds():
    assert utils.parse_seconds(10) == 10
    assert utils.parse_seconds('10') == 10
    assert utils.parse_seconds(' 10 ') == 10
    assert utils.parse_seconds('10s') == 10
    assert utils.parse_seconds(' 10s ') == 10
    assert utils.parse_seconds('10m') == 600
    assert utils.parse_seconds('10h') == 36000
    assert utils.parse_seconds('10d') == 864000
    assert utils.parse_seconds('10w') == 6048000
    assert utils.parse_seconds('10y') == 315360000

    with pytest.raises(ValueError):
        utils.parse_seconds('boo')

    with pytest.raises(ValueError):
        utils.parse_seconds('10mm')


def test_parse_retentions():
    assert utils.parse_retentions('5m:30d,1m:7d') == [(60, 604800), (300, 2592000)]
    assert utils.parse_retentions(' 5m : 30d , 1m : 7d ') == [(60, 604800), (300, 2592000)]
