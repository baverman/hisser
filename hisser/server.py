import os
import time
import socket
import signal
import logging
import threading

from nanoio import spawn, Loop, recv, accept, wait_io, WAIT_READ, sendall, sleep

from .utils import mloads, mdumps
from . import tasks, profile

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

        self.tm = tasks.TaskManager()
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
            self.link_server = RpcServer(self, *self.link_host_port)
            self.link_thread = threading.Thread(
                target=self.link_server.start, daemon=True)
            self.link_thread.start()

    def check_buffer(self, now=None):
        data, new_names = self.buf.tick(now=now)
        if new_names:
            self.tm.add('names', self.storage.new_names, new_names)

        if data:
            self.tm.add('data', self.storage.new_block, *data)
            if not self.disable_housework:
                self.tm.add('housework', self.storage.do_housework)

    async def check_aux(self):
        while True:
            await sleep(3)
            if self.link_server:
                self.buf.add(time.time(), b'hisser.link.accepted', self.link_server.accepted_requests)
            if not self.tm.check():
                self.check_buffer()

    def run(self):
        self.loop.spawn(self.check_aux())
        self.loop.run()

        while self.tm.check():
            time.sleep(1)

        data, _new_names = self.buf.tick(force=True)
        if data:
            self.storage.new_block(*data)


class RpcServer:
    def __init__(self, server, host, port):
        self.server = server
        self.host = host
        self.port = port
        self.last_ts = None
        self.accepted_requests = 0

    async def handler(self, conn):
        with profile.slowlog(0.05, log.debug, 'SLOW RPC HANDLER') as ps:
            with conn:
                with ps('read'):
                    data = []
                    while True:
                        buf = await recv(conn, 16384)
                        if not buf:
                            break
                        data.append(buf)

                    data = b''.join(data)

                if not data:  # pragma: no cover
                    return

                with ps('call'):
                    try:
                        req = mloads(data)
                        method = req.pop('method')
                        resp = mdumps(getattr(self, 'rpc_{}'.format(method))(**req))
                    except Exception as e:
                        resp = mdumps({'error': str(e)})

                with ps('send'):
                    await sendall(conn, resp)

    def rpc_fetch(self, keys):
        return self.server.buf.get_data(keys)

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
                conn.setblocking(False)
                self.accepted_requests += 1
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
