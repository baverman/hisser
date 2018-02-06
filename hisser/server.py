import os
import time
import errno
import socket
import signal
import fcntl
import logging
from selectors import DefaultSelector, EVENT_READ, EVENT_WRITE

from .utils import run_in_fork, wait_childs, mloads, mdumps

log = logging.getLogger(__name__)


def rpc_fetch(srv, keys):
    return srv.buf.get_data(keys)


class Server:
    def __init__(self, buf, storage, carbon_host_port_tcp,
                 carbon_host_port_udp=None, link_host_port=None, backlog=100):
        self.buf = buf
        self.storage = storage
        self.carbon_host_port_tcp = carbon_host_port_tcp
        self.carbon_host_port_udp = carbon_host_port_udp
        self.link_host_port = link_host_port
        self.backlog = backlog

        self.ready_to_merge = False
        self.flush_pids = set()
        self.merge_pid = None

        self.time_to_exit = False

    def accept(self, sock, cdata):
        conn, _addr = sock.accept()
        conn.setblocking(False)
        self.sel.register(conn, EVENT_READ, (cdata['handler'], {}))

    def carbon_read_tcp(self, conn, cdata):
        data = conn.recv(4096)
        olddata = cdata.get('buf', b'')
        if data:
            cdata['buf'] = self.process(olddata + data)
        else:
            if olddata:
                self.process(olddata, True)
            self.sel.unregister(conn)
            conn.close()

    def carbon_read_udp(self, conn, cdata):
        (data, _addr) = conn.recvfrom(4096)
        if data:
            self.process(data, end=True)

    def link_write(self, conn, cdata):
        buf = cdata['buf']
        count = conn.send(buf)
        cdata['buf'] = buf = buf[count:]

        if not buf:
            self.sel.unregister(conn)
            conn.close()

    def link_read(self, conn, cdata):
        data = conn.recv(4096)
        buf = cdata.get('buf', b'')
        if data:
            cdata['buf'] = buf + data
            return

        if not buf:  # pragma: nocover
            self.sel.unregister(conn)
            conn.close()
            return

        try:
            req = mloads(buf)
            method = req.pop('method')
            resp = mdumps(globals()['rpc_{}'.format(method)](self, **req))
        except Exception as e:
            resp = mdumps({'error': str(e)})

        self.sel.unregister(conn)
        self.sel.register(conn, EVENT_WRITE, (self.link_write, {'buf': resp}))

    def process(self, data, end=False):
        lines = data.splitlines(True)
        next_chunk = b''
        if not end and not lines[-1].endswith(b'\n'):
            next_chunk = lines[-1]
            lines = lines[:-1]

        buf = self.buf
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

    def signal_read(self, conn, cdata):  # pragma: nocover
        data = os.read(conn, 4096)
        if data:
            if data[-1] in (signal.SIGINT, signal.SIGTERM):
                log.info('Cought exit signal')
                self.time_to_exit = True

    def setup_signals(self, sel):  # pragma: nocover
        self.pipe_r, pipe_w = os.pipe()
        flags = fcntl.fcntl(pipe_w, fcntl.F_GETFL, 0)
        flags = flags | os.O_NONBLOCK
        fcntl.fcntl(pipe_w, fcntl.F_SETFL, flags)
        signal.set_wakeup_fd(pipe_w)

        sel.register(self.pipe_r, EVENT_READ, (self.signal_read, None))

        def dummy(signal, frame):
            pass

        signal.signal(signal.SIGINT, dummy)
        signal.signal(signal.SIGTERM, dummy)

    def listen(self, signals=True):
        sel = self.sel = DefaultSelector()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.carbon_host_port_tcp)
        sock.listen(self.backlog)
        sock.setblocking(False)
        sel.register(sock, EVENT_READ, (self.accept, {'handler': self.carbon_read_tcp}))

        if self.carbon_host_port_udp:
            sock_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock_udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock_udp.bind(self.carbon_host_port_udp)
            sock_udp.setblocking(False)
            sel.register(sock_udp, EVENT_READ, (self.carbon_read_udp, None))

        if self.link_host_port:
            link_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            link_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            link_sock.bind(self.link_host_port)
            link_sock.listen(self.backlog)
            link_sock.setblocking(False)
            sel.register(link_sock, EVENT_READ, (self.accept, {'handler': self.link_read}))

        if signals:  # pragma: nocover
            self.setup_signals(sel)

    def check_childs(self):
        if self.flush_pids or self.merge_pid:
            try:
                pid, _exit = wait_childs()
            except OSError as e:  # pragma: nocover
                if e.errno == errno.ECHILD:
                    self.flush_pids.clear()
                    self.merge_pid = None
                else:
                    raise
            else:
                if self.flush_pids and pid in self.flush_pids:
                    self.flush_pids.remove(pid)
                    self.ready_to_merge = True
                if self.merge_pid and self.merge_pid == pid:
                    self.merge_pid = None
            return True
        return False

    def check_loop(self):
        events = self.sel.select(3)
        for key, _mask in events:
            callback, data = key.data
            callback(key.fileobj, data)

    def check_buffer(self, now=None):
        data, new_names = self.buf.tick(now=now)
        if data:
            self.flush_pids.add(run_in_fork(self.storage.new_block, *data).pid)
            self.ready_to_merge = False

        if new_names:
            self.flush_pids.add(run_in_fork(self.storage.new_names, new_names).pid)

        if self.ready_to_merge and not self.merge_pid:
            self.merge_pid = run_in_fork(self.storage.do_housework).pid
            self.ready_to_merge = False

    def run(self):
        while not self.time_to_exit:
            self.check_loop()
            self.check_childs()
            self.check_buffer()

        while self.check_childs():  # pragma: nocover
            time.sleep(1)

        data, _new_names = self.buf.tick(force=True)
        if data:
            self.storage.new_block(*data)


class RpcClient:
    def __init__(self, host_port=('127.0.0.1', 8002), connect_timeout=1, timeout=5):
        self.host_port = host_port
        self.connect_timeout = connect_timeout
        self.timeout = timeout

    def call(self, method, **kwargs):
        s = socket.create_connection(self.host_port, self.connect_timeout)
        s.settimeout(self.timeout)
        kwargs['method'] = method
        s.sendall(mdumps(kwargs))
        s.shutdown(socket.SHUT_WR)
        payload = b''
        while True:
            data = s.recv(4096)
            if not data:
                break
            payload += data

        s.close()
        return mloads(payload)
