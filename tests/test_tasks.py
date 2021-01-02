import time
from hisser import tasks


def test_simple():
    def fn(boo, foo):
        time.sleep(0.1)
        assert boo == 'boo-val'
        assert foo == 'foo-val'

    tm = tasks.TaskManager()
    assert tm.is_running() == False
    assert tm.check() == False

    tm.add('ok', fn, 'boo-val', foo='foo-val')
    tm.add('fail', fn, 10, foo=20)
    assert tm.check() == True

    time.sleep(1)
    assert tm.check() == False
    assert tm.last_status == {'ok': 0, 'fail': 256}
