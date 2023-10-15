from hisser import config


def test_simple(tmpdir):
    cfg = config.get_config({'DATA_DIR': str(tmpdir)})
    cfg.ensure_dirs()

    server = cfg.server
    server.buf.ts = 1000

    for ts in range(1000, 1700, 60):
        data = 'm1 {0} 10\nm2 {0} 10\n'.format(ts).encode()
        server.process(data[:15])
        server.process(data[15:], True)
        server.check_buffer(ts)
