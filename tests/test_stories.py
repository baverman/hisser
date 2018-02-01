import os
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
    s.sendall('\n'.join('{} {} {}'.format(*r) for r in data).encode())
    s.close()


def send_udp(data):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto('\n'.join('{} {} {}'.format(*r) for r in data).encode(), ('127.0.0.1', 14001))


def test_simple(tmpdir):
    os.environ['DJANGO_SETTINGS_MODULE'] = 'graphite_local_settings'
    import django
    django.setup()

    from hisser import graphite

    cfg = get_config(str(tmpdir))
    cfg.ensure_dirs()

    t = Thread(target=run_server, args=(cfg,))
    t.daemon = True
    t.start()

    start = time.time()
    time.sleep(0.1)
    send_tcp([('m1', 10, start)])
    send_udp([('m2', 10, start)])
    time.sleep(0.1)
    result = cfg.rpc_client.call('fetch', keys=[b'm1', b'm2'])
    assert set(result['result']) == {b'm1', b'm2'}

    result = cfg.rpc_client.call('fetch')
    assert 'missing' in result['error']

    cfg.server.check_buffer(start + 60)
    time.sleep(0.2)

    f = graphite.Finder(cfg)

    class q:
        pattern = '*'
    result = [(r.path, r.is_leaf) for r in f.find_nodes(q)]
    assert result == [('hisser', False), ('m1', True), ('m2', True)]

    result, = f.fetch(['m1'], start - 60, start + 60)
    assert result['path'] == 'm1'

    cfg.server.time_to_exit = True
    time.sleep(3)
