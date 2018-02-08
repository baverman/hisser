import array
import time

from hisser import utils


def test_empty_rows():
    result = list(utils.empty_rows({'boo': array.array('d', [utils.NAN, utils.NAN]),
                                    'foo': array.array('d', [1, 2])}.items(), 2))
    assert result == ['boo']


def test_non_empty_rows():
    result = list(utils.non_empty_rows({'boo': array.array('d', [utils.NAN, utils.NAN]),
                                        'foo': array.array('d', [1, 2])}.items(), 2))
    assert result == [('foo', array.array('d', [1, 2]))]


def test_run_in_fork(tmpdir):
    def worker():
        tmpdir.join('boo').ensure()

    f = utils.run_in_fork(worker)

    stop = time.time() + 3
    while time.time() < stop:
        pid, _status = utils.wait_childs()
        if pid == f.pid:
            break
        time.sleep(0.1)


def test_open_env(tmpdir):
    with utils.open_env(str(tmpdir.join('boo'))) as env:
        assert env.info()['map_size'] == 10485760

    with utils.open_env(str(tmpdir.join('boo'))) as env:
        assert env.info()['map_size'] == 8192

    with utils.open_env(str(tmpdir.join('boo')), map_size=-8192) as env:
        assert env.info()['map_size'] == 16384
