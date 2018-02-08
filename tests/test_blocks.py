from hisser import blocks


def make_block(ts, resolution, size):
    return blocks.Block.make(ts, size, resolution, 'fake')


def test_block_slices():
    block = make_block(180, 60, 6)
    bslice = blocks.Slice.make(block)
    assert bslice[:4] == (180, 540, 0, 6)

    assert bslice.slice(240)[:4] == (240, 540, 1, 5)
    assert bslice.slice(None, 240)[:4] == (180, 240, 0, 1)
    assert bslice.slice(240, 420)[:4] == (240, 420, 1, 3)

    assert bslice.slice(540) is None
    assert bslice.slice(None, 180) is None

    assert bslice.slice(180) == bslice
    assert bslice.slice(None, 540) == bslice

    assert bslice.split(180) == (None, bslice)
    assert block.split(540) == (bslice, None)

    assert bslice.split(300) == ((180, 300, 0, 2, 60, 'fake', 180),
                                 (300, 540, 2, 4, 60, 'fake', 180))
    assert bslice.slice(240, 420).split(300) == ((240, 300, 1, 1, 60, 'fake', 180),
                                                 (300, 420, 2, 2, 60, 'fake', 180))


def test_block_list(tmpdir):
    bl = blocks.BlockList(str(tmpdir))

    assert bl.blocks(10) == []
    assert bl.blocks(10) == []

    tmpdir.join('10').ensure('1000.10.hdb')
    tmpdir.join('10').ensure('1000.10.hdb.tmp')
    tmpdir.join('10').ensure('1000.boo.hdb')
    blocks.notify_blocks_changed(bl.data_dir, 10)

    b, = bl.blocks(10)
    assert b.start == 1000
    assert b.end == 1100
    assert b.idx == 0
    assert b.resolution == 10

    tmpdir.join('10').ensure('500.10.hdb')
    b, *rest = bl.blocks(10, refresh=True)
    assert b.start == 500
    assert b.end == 600
