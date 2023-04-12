import os
import sys
import errno

from time import time
from collections import namedtuple

import logging
log = logging.getLogger('hisser.tm')

Fork = namedtuple('Fork', 'pid start')


def run_in_fork(func, *args, **kwargs):
    pid = os.fork()
    if pid == 0:  # pragma: nocover
        try:
            func(*args, **kwargs)
        except Exception:
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(1)
        else:
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(0)
    else:
        return Fork(pid, time())


def wait_childs():
    return os.waitpid(-1, os.WNOHANG)


class TaskManager:
    def __init__(self):
        self.task_map = {}
        self.last_status = {}

    def is_running(self):
        return bool(self.task_map)

    def add(self, name, fn, *args, **kwargs):
        log.debug('Running %s %s', name, fn)
        pid = run_in_fork(fn, *args, **kwargs).pid
        self.task_map[pid] = name

    def name_is_running(self, name):
        return self.task_map and name in self.task_map.values()

    def check(self):
        if not self.task_map:
            return False

        while True:
            try:
                pid, status = wait_childs()
            except OSError as e:  # pragma: no cover
                if e.errno == errno.ECHILD:
                    self.task_map.clear()
                    break
                else:
                    raise
            else:
                if pid == 0:
                    break
                name = self.task_map.pop(pid, None)
                if name:
                    self.last_status[name] = status

        return bool(self.task_map)
