import os
import pytest
from hisser.config import (parse_aggregation, parse_retentions, parse_seconds,
                           get_agg_rules_from_dict, get_config, Config)


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
    assert parse_aggregation(r'\.count$|sum') == [r'\.count$', 'sum']
    assert parse_aggregation(r'(\.count|\.sum)$|sum') == [r'(\.count|\.sum)$', 'sum']


def test_cfg_agg_rules():
    cfg = {
        'AGG_RULE_B': r'\.count$|sum',
        'AGG_RULE_A': r'\.min$|min',
        'AGG_RULE_C': ''
    }

    rules = get_agg_rules_from_dict(cfg)
    assert rules == [['\\.min$', 'min'], ['\\.count$', 'sum']]


def test_config_from_file(tmpdir, monkeypatch):
    monkeypatch.setattr('hisser.config.defaults.BOO', 1, raising=False)
    tmpdir.join('boo').write('BOO = 10')
    cfg = get_config({}, str(tmpdir.join('boo')))
    assert cfg.BOO == 10


def test_config_from_env(monkeypatch):
    monkeypatch.setattr('hisser.config.defaults.FOO', '', raising=False)
    opts = {'FOO': 20}
    os.environ['HISSER_FOO'] = '10'
    cfg = get_config(opts)
    assert cfg.FOO == '10'


def test_config_error(monkeypatch):
    monkeypatch.setattr('hisser.config.defaults.BAR', 10, raising=False)
    monkeypatch.setattr('hisser.config.defaults.BOO', None, raising=False)

    with pytest.raises(Config.Error):
        cfg = get_config({'BAR': ''})

    with pytest.raises(Config.Error):
        cfg = get_config({'BAR': 'bar'})

    cfg = get_config({'BAR': '20', 'BOO': ''})
    assert cfg['BAR'] == 20
    assert cfg['BOO'] == None

    with pytest.raises(Config.Error):
        cfg.required('BOO')


def test_config_host_port():
    cfg = Config()

    cfg['host'] = ''
    assert cfg.host_port('host', required=False) is None

    cfg['host'] = ':8000'
    assert cfg.host_port('host') == ('0.0.0.0', 8000)


def test_config_pop_from_args():
    params = {'data_dir': '/tmp', 'boo': 'foo'}
    get_config(params)
    assert params == {'boo': 'foo'}
