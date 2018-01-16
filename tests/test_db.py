from hisser import db


def make_block(ts, resolution, size):
    return db.BlockInfo.make(ts, size, resolution, 'fake')


def get_segments(result):
    return [[start, stop] + [(b.start, b.end) for b in s]
            for s, start, stop in result]


def test_block_slices():
    block = make_block(180, 60, 6)
    bslice = db.BlockSlice.make(block)
    assert bslice[:4] == (180, 540, 0, 6)

    assert bslice.slice(240)[:4] == (240, 540, 1, 5)
    assert bslice.slice(None, 240)[:4] == (180, 240, 0, 1)
    assert bslice.slice(240, 420)[:4] == (240, 420, 1, 3)

    assert bslice.slice(540) == None
    assert bslice.slice(None, 180) == None

    assert bslice.slice(180) == bslice
    assert bslice.slice(None, 540) == bslice

    assert bslice.split(180) == (None, bslice)
    assert bslice.split(540) == (bslice, None)

    assert bslice.split(300) == ((180, 300, 0, 2, 60, 'fake', 180), (300, 540, 2, 4, 60, 'fake', 180))
    assert bslice.slice(240, 420).split(300) == ((240, 300, 1, 1, 60, 'fake', 180), (300, 420, 2, 2, 60, 'fake', 180))


def test_find_downsample_simple():
    blocks = [make_block(300, 10, 100), make_block(1300, 10, 100)]
    result = db.find_blocks_to_downsample(10, blocks, 100, 30, 10, 1000, 300)
    assert get_segments(result) == [[300, 2300, (300, 1300), (1300, 2300)]]


def test_find_downsample_gap():
    blocks = [make_block(300, 10, 100), make_block(5000, 10, 100)]
    result = db.find_blocks_to_downsample(10, blocks, 100, 30, 10, 1000, 300)
    assert get_segments(result) == [[300, 1300, (300, 1300)], [5000, 6000, (5000, 6000)]]


def test_find_downsample_min_size():
    blocks = [make_block(300, 10, 40), make_block(700, 10, 40)]
    result = db.find_blocks_to_downsample(10, blocks, 100, 30, 100, 100, 300)
    assert get_segments(result) == []


def test_find_downsample_start():
    blocks = [make_block(300, 10, 40), make_block(700, 10, 40)]
    result = db.find_blocks_to_downsample(10, blocks, 100, 30, 1, 100, 700)
    assert get_segments(result) == [[700, 1100, (700, 1100)]]


def test_find_downsample_max_size():
    blocks = [make_block(100, 10, 100)]
    result = db.find_blocks_to_downsample(10, blocks, 100, 30, 1, 40, 100)
    assert get_segments(result) == [[100, 500, (100, 500)], [500, 900, (500, 900)], [900, 1100, (900, 1100)]]


def test_find_downsample_mixed_shifts():
    blocks = [make_block(4, 1, 7)]
    result = db.find_blocks_to_downsample(1, blocks, 3, 30, 1, 40, 7)
    assert get_segments(result) == [[6, 12, (6, 11)]]
