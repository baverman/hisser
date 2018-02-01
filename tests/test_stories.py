import time
import socket
from threading import Thread

from hisser import config


def get_config(data_dir, **opts):
    opts['DATA_DIR'] = data_dir
    opts['CARBON_BIND'] = '127.0.0.1:14000'
    opts['CARBON_BIND_UDP'] = '127.0.0.1:14001'
    opts['LINK_BIND'] = '127.0.0.1:14002'
    return config.get_config(opts)


def run_server(cfg):
    cfg.ensure_dirs()
    server = cfg.server
    server.listen(False)
    server.run()


def send_tcp(data):
    s = socket.create_connection(('127.0.0.1', 14000), 3)
    s.sendall(''.join('{} {} {}\n'.format(*r) for r in data).encode())
    s.close()


def test_simple(tmpdir):
    cfg = get_config(str(tmpdir))
    cfg.ensure_dirs()

    t = Thread(target=run_server, args=(cfg,))
    t.start()

    send_tcp([('m1', 10, time.time())])
    assert cfg.rpc_client.call('fetch', keys=[b'm1'])
    cfg.server.time_to_exit = True
