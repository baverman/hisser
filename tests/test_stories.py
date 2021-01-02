import os
import time
import socket
from threading import Thread

import numpy as np

from hisser import config
from .helpers import assert_naneq


def get_config(data_dir, **opts):
    opts['LOGGING_LEVEL'] = 'DEBUG'
    opts['DATA_DIR'] = data_dir
    opts['CARBON_BIND'] = '127.0.0.1:14000'
    opts['CARBON_BIND_UDP'] = '127.0.0.1:14001'
    opts['LINK_BIND'] = '127.0.0.1:14002'
    return config.get_config(opts)


def send_tcp(data):
    s = socket.create_connection(('127.0.0.1', 14000), 3)
    s.sendall('\n'.join('{} {} {}'.format(*r) for r in data).encode())
    s.close()


def send_udp(data):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto('\n'.join('{} {} {}'.format(*r) for r in data).encode(), ('127.0.0.1', 14001))


def test_simple(tmpdir):
    from hisser import evaluator, graphite

    cfg = get_config(str(tmpdir))
    cfg.ensure_dirs()

    cfg.server.listen(True)

    t = Thread(target=cfg.server.run)
    t.daemon = True
    t.start()

    start = int(time.time())
    send_tcp([('m1', 10, start)])
    send_udp([('m2', 10, start)])
    send_tcp([('m3;tag=value', 10, start)])
    time.sleep(0.1)
    result = cfg.rpc_client.call('fetch', keys=[b'm1', b'm2'])
    assert set(result['result']) == {b'm1', b'm2'}

    result = cfg.rpc_client.call('fetch')
    assert 'missing' in result['error']

    cfg.server.check_buffer(start + 60)

    while cfg.server.tm.check():
        time.sleep(0.1)

    f = graphite.Finder(cfg)

    class q:
        pattern = '*'
    result = [(r.path, r.is_leaf) for r in f.find_nodes(q)]
    assert result == [('hisser', False), ('m1', True), ('m2', True)]

    ds, = f.fetch(['m1'], start - 60, start + 60)
    assert ds.names == [('m1', 0)]

    ds, = f.fetch(["seriesByTag('tag=value')"], start - 60, start + 60)
    assert ds.names == [('m3;tag=value', 0)]

    result = f.auto_complete_tags([], 't')
    assert result == ['tag']

    result = f.auto_complete_values([], 'tag', 'v')
    assert result == ['value']

    result = cfg.reader.fetch([b'm1', b'm0'], start - 60, start + 60)
    assert_naneq(result[1], [np.nan, 10.0, np.nan])

    r, w = os.pipe()
    os.write(w, bytes([2]))
    cfg.server.loop.spawn(cfg.server.handle_signals(r))
    time.sleep(5)

    while cfg.server.tm.check():
        time.sleep(0.1)
