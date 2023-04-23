import numpy as np
from ._cffi import ffi, lib


def init():
    import lmdb.cpython
    if lib.lmdb_scan_init(lmdb.cpython.__file__.encode()):
        raise Exception("Can't init lmdb_scan")  # pragma: no cover


def scan_tags(cursor, tag_ids, name_cursors=None):
    # print(tag_ids, flush=True)
    out = np.full((1000, 4), 0, 'B')
    state = ffi.new('LMDB_scan_state *')
    state.count = len(tag_ids)
    state.cname = ffi.NULL
    state.tag_states = tag_states = ffi.new('LMDB_tags_state[]', len(tag_ids))
    refs = []
    for i, it in enumerate(tag_ids):
        ts = tag_states[i]
        ts.count = len(it)
        ts.min_names_filled = 0;
        # ts.min_names_buf = ff.new('uint8_t[]', 4*len(it))
        ts.min_names = mn = ffi.new('uint8_t *[]', len(it))
        ts.tag_ids = ti = ffi.new('uint8_t *[]', [ffi.from_buffer(x) for x in it])
        refs.append((mn, ti))

    fetch_names = False
    if name_cursors:
        fetch_names = True
        pyobj_nt = ffi.cast('void *', id(name_cursors[0]))
        pyobj_ti = ffi.cast('void *', id(name_cursors[1]))
        scan_cache_state = ffi.new('hash_item *[]', [ffi.NULL])

    try:
        py_obj = ffi.cast('void *', id(cursor))
        out_buf = ffi.from_buffer(out)
        result = []
        while True:
            r = lib.lmdb_scan_tags(py_obj, state, out_buf, len(out))
            if r and fetch_names:
                tmpres = []
                lib.lmdb_scan_names(pyobj_nt, pyobj_ti, out_buf, r,
                                    ffi.cast('void *', id(tmpres)), scan_cache_state)
                result.extend(tmpres)
            else:  # pragma: no cover
                result.extend(bytes(it.data) for it in out[:r])

            if r < len(out):
                break
    finally:
        if fetch_names:
            lib.lmdb_scan_free_cache(scan_cache_state)

    return result
