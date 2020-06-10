import os
import time
import errno
import socket
import signal
import logging
import threading

from nanoio import spawn, Loop, recv, accept, wait_io, WAIT_READ, sendall, sleep

from .utils import run_in_fork, wait_childs, mloads, mdumps

log = logging.getLogger(__name__)


class Server:
    def __init__(self, buf, storage, carbon_host_port_tcp,
                 carbon_host_port_udp=None, link_host_port=None,
                 backlog=100, disable_housework=False):
        self.buf = buf
        self.storage = storage
        self.carbon_host_port_tcp = carbon_host_port_tcp
        self.carbon_host_port_udp = carbon_host_port_udp
        self.link_host_port = link_host_port
        self.backlog = backlog
        self.disable_housework = disable_housework

        self.ready_to_merge = False
        self.flush_pids = set()
        self.merge_pid = None

        self.loop = Loop()

    def handle_carbon_tcp(self):
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_sock.bind(self.carbon_host_port_tcp)
        listen_sock.listen(self.backlog)
        listen_sock.setblocking(False)

        async def server_loop():
            while True:
                conn, _addr = await accept(listen_sock)
                conn.setblocking(False)
                await spawn(self.handle_carbon_tcp_client(conn))

        self.loop.spawn(server_loop())

    async def handle_carbon_tcp_client(self, conn):
        olddata = b''
        while True:
            data = await recv(conn, 4096)
            if not data:
                break
            olddata = self.process(olddata + data)

        if olddata:
            self.process(olddata, True)

        conn.close()

    def handle_carbon_udp(self):
        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_sock.bind(self.carbon_host_port_udp)
        listen_sock.setblocking(False)

        async def server_loop():
            read = listen_sock.recvfrom
            while True:
                data, _addr = await wait_io(listen_sock, WAIT_READ, read, 4096)
                if data:
                    self.process(data, end=True)

        self.loop.spawn(server_loop())

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

    async def handle_signals(self, conn):
        while True:
            data = await wait_io(conn, WAIT_READ, os.read, conn, 4096)
            if data[-1] in (signal.SIGINT, signal.SIGTERM):
                log.info('Cought exit signal')
                self.loop.stop()
                return

    def setup_signals(self):
        pipe_r, pipe_w = os.pipe()
        os.set_blocking(pipe_r, False)
        os.set_blocking(pipe_w, False)
        signal.set_wakeup_fd(pipe_w)

        def dummy(signal, frame):  # pragma: no cover
            pass

        signal.signal(signal.SIGINT, dummy)
        signal.signal(signal.SIGTERM, dummy)
        self.loop.spawn(self.handle_signals(pipe_r))

    def listen(self, signals=True):
        self.handle_carbon_tcp()

        if self.carbon_host_port_udp:
            self.handle_carbon_udp()

        if signals:
            self.setup_signals()

        if self.link_host_port:
            self.link_server = RpcServer(self.buf, *self.link_host_port)
            self.link_thread = threading.Thread(
                target=self.link_server.start, daemon=True)
            self.link_thread.start()

    def check_childs(self):
        if self.flush_pids or self.merge_pid:
            try:
                pid, _exit = wait_childs()
            except OSError as e:  # pragma: no cover
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

    def check_buffer(self, now=None):
        data, new_names = self.buf.tick(now=now)
        if data:
            self.flush_pids.add(run_in_fork(self.storage.new_block, *data).pid)
            self.ready_to_merge = False

        if new_names:
            self.flush_pids.add(run_in_fork(self.storage.new_names, new_names).pid)

        if (not self.disable_housework
                and self.ready_to_merge
                and not self.merge_pid):
            self.merge_pid = run_in_fork(self.storage.do_housework).pid
            self.ready_to_merge = False

    async def check_aux(self):
        while True:
            await sleep(3)
            self.check_childs()
            self.check_buffer()

    def run(self):
        self.loop.spawn(self.check_aux())
        self.loop.run()

        while self.check_childs():
            time.sleep(1)

        data, _new_names = self.buf.tick(force=True)
        if data:
            self.storage.new_block(*data)


class RpcServer:
    def __init__(self, buf, host, port):
        self.buf = buf
        self.host = host
        self.port = port

    async def handler(self, conn):
        data = []
        while True:
            buf = await recv(conn, 16384)
            if not buf:
                break
            data.append(buf)

        data = b''.join(data)

        if not data:  # pragma: no cover
            conn.close()
            return

        try:
            req = mloads(data)
            method = req.pop('method')
            resp = mdumps(getattr(self, 'rpc_{}'.format(method))(**req))
        except Exception as e:
            resp = mdumps({'error': str(e)})

        await sendall(conn, resp)
        conn.close()

    def rpc_fetch(self, keys):
        return self.buf.get_data(keys)

    def start(self):
        loop = Loop()

        listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listen_sock.bind((self.host, self.port))
        listen_sock.listen(100)
        listen_sock.setblocking(False)

        async def server_loop():
            while True:
                conn, _addr = await accept(listen_sock)
                loop.spawn(self.handler(conn))

        loop.run(server_loop())


class RpcClient:
    def __init__(self, host_port=('127.0.0.1', 8002), connect_timeout=5, timeout=5):
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
