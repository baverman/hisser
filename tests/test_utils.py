import array
import time
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
