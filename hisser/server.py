import errno
import socket
from selectors import DefaultSelector, EVENT_READ

from .utils import run_in_fork, wait_childs


def loop(buf, storage, host_port, backlog):
    def accept(sock, _cdata):
        conn, _addr = sock.accept()
        conn.setblocking(False)
        sel.register(conn, EVENT_READ, (read, {}))

    def read(conn, cdata):
        data = conn.recv(4096)
        olddata = cdata.get('olddata', b'')
        if data:
            cdata['olddata'] = process(olddata + data)
        else:
            if olddata:
                process(olddata, True)
            sel.unregister(conn)
            conn.close()

    def process(data, end=False):
        lines = data.splitlines(True)
        next_chunk = b''
        if not end and not lines[-1].endswith(b'\n'):
            next_chunk = lines[-1]
            lines = lines[:-1]

        for line in lines:
            parts = line.split()

            try:
                name = parts[0]
                value = float(parts[1])
                ts = int(float(parts[2]))
            except (ValueError, IndexError):
                pass
            else:
                buf.add(ts, name, value)

        return next_chunk

    sel = DefaultSelector()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(host_port)
    sock.listen(backlog)
    sock.setblocking(False)
    sel.register(sock, EVENT_READ, (accept, None))

    ready_to_merge = False
    flush_pids = set()
    merge_pid = None

    try:
        while True:
            events = sel.select(3)
            for key, _mask in events:
                callback, data = key.data
                callback(key.fileobj, data)

            if flush_pids or merge_pid:
                try:
                    pid, _exit = wait_childs()
                except OSError as e:
                    if e.errno == errno.ECHILD:
                        flush_pids.clear()
                        merge_pid = None
                    else:
                        raise
                else:
                    if flush_pids and pid in flush_pids:
                        flush_pids.remove(pid)
                        ready_to_merge = True
                    if merge_pid and merge_pid == pid:
                        merge_pid = None

            result = buf.tick()
            if result:
                flush_pids.add(run_in_fork(storage.new_block, *result).pid)
                ready_to_merge = False

            if ready_to_merge and not merge_pid:
                print('Run housework')
                merge_pid = run_in_fork(storage.do_housework).pid
                ready_to_merge = False
    except KeyboardInterrupt:
        pass
