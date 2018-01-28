from hisser import config

OPTS = {
    'RETENTIONS': '10s:1d,100s:1d',
    'BUFFER_SIZE': '15',
    'BUFFER_FLUSH_SIZE': '5',
    'BUFFER_PAST_SIZE': '1',
    'BUFFER_MAX_POINTS': '50000',
    'MERGE_MAX_SIZE': '500',
    'MERGE_MAX_GAP_SIZE': 50,
    'MERGE_RATIO': 2,
    'DOWNSAMPLE_MAX_SIZE': 100,
    'DOWNSAMPLE_MIN_SIZE': 5,
    'AGG_DEFAULT_METHOD': 'sum',
    'LINK_BIND': None,
}


def get_config(data_dir, **opts):
    opts = OPTS.copy()
    opts['DATA_DIR'] = data_dir
    opts.update(opts)
    return config.Config(opts)


def _test_simple(tmpdir):
    cfg = get_config(str(tmpdir))
    cfg.ensure_dirs()
    buf = cfg.buffer
    buf.set_ts(100)
    for val, ts in enumerate(range(100, 1000, 10)):
        buf.add(ts, b'm1', val)
        buf.add(ts, b'm2', val*2)
        data, new_names = buf.tick(now=ts)
        if data:
            cfg.storage.new_block(*data)
        if new_names:
            cfg.storage.new_names(new_names)
        cfg.storage.do_housework(ts)

    data, new_names = buf.tick(force=True, now=1000)
    if data:
        cfg.storage.new_block(*data)
    if new_names:
        cfg.storage.new_names(new_names)
    cfg.storage.do_housework(1000)
