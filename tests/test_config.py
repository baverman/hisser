import pytest
from hisser.config import (parse_aggregation, parse_retentions,
                           parse_seconds, get_agg_rules_from_dict)


def test_parse_seconds():
    assert parse_seconds(10) == 10
    assert parse_seconds('10') == 10
    assert parse_seconds(' 10 ') == 10
    assert parse_seconds('10s') == 10
    assert parse_seconds(' 10s ') == 10
    assert parse_seconds('10m') == 600
    assert parse_seconds('10h') == 36000
    assert parse_seconds('10d') == 864000
    assert parse_seconds('10w') == 6048000
    assert parse_seconds('10y') == 315360000

    with pytest.raises(ValueError):
        parse_seconds('boo')

    with pytest.raises(ValueError):
        parse_seconds('10mm')


def test_parse_retentions():
    assert parse_retentions('5m:30d,1m:7d') == [(60, 604800), (300, 2592000)]
    assert parse_retentions(' 5m : 30d , 1m : 7d ') == [(60, 604800), (300, 2592000)]


def test_parse_aggregation():
    assert parse_aggregation('\.count$|sum') == ['\.count$', 'sum']
    assert parse_aggregation('(\.count|\.sum)$|sum') == ['(\.count|\.sum)$', 'sum']


def test_cfg_agg_rules():
    cfg = {
        'AGG_RULE_B': '\.count$|sum',
        'AGG_RULE_A': '\.min$|min',
        'AGG_RULE_C': ''
    }

    rules = get_agg_rules_from_dict(cfg)
    assert rules == [['\\.min$', 'min'], ['\\.count$', 'sum']]
