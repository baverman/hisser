import array
import time

from hisser import utils


def test_empty_rows():
    result = list(utils.empty_rows({'boo': array.array('d', [utils.NAN, utils.NAN]),
                                    'foo': array.array('d', [1, 2])}.items()))
    assert result == ['boo']


def test_non_empty_rows():
    result = list(utils.non_empty_rows({'boo': array.array('d', [utils.NAN, utils.NAN]),
                                        'foo': array.array('d', [1, 2])}.items()))
    assert result == [('foo', array.array('d', [1, 2]))]


def test_open_env(tmpdir):
    with utils.open_env(str(tmpdir.join('boo'))) as env:
        assert env.info()['map_size'] == 10485760

    with utils.open_env(str(tmpdir.join('boo'))) as env:
        assert env.info()['map_size'] == 8192

    with utils.open_env(str(tmpdir.join('boo')), map_size=-8192) as env:
        assert env.info()['map_size'] == 16384


def test_make_key():
    assert utils.make_key(b'boo.foo.bam') == b"boo.foo.'\r\xf8\xb3\x1a\x14\xfa\x18"
    assert utils.make_key_u('boo.foo.bam') == b"boo.foo.'\r\xf8\xb3\x1a\x14\xfa\x18"


def test_parse_interval():
    assert utils.parse_interval(10) == (True, 10)
    assert utils.parse_interval('10') == (True, 10)
    assert utils.parse_interval('10s') == (False, 10)
    assert utils.parse_interval('10min') == (False, 600)
    assert utils.parse_interval('10h') == (False, 36000)
    assert utils.parse_interval('10d') == (False, 864000)
    assert utils.parse_interval('10w') == (False, 6048000)
    assert utils.parse_interval('10mon') == (False, 25920000)
    assert utils.parse_interval('10y') == (False, 315360000)
