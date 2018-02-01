import os.path
import array

from hisser import db, blocks, metrics, agg


def make_block(ts, resolution, size):
    return blocks.Block.make(ts, size, resolution, 'path{}'.format(ts))


def get_segments(result):
    return [[start, stop] + [(b.start, b.end) for b in s]
            for s, start, stop in result]


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


def test_find_blocks_to_merge_simple():
    blocks = [make_block(1000, 10, 10), make_block(1100, 10, 20), make_block(1300, 10, 10)]
    result = db.find_blocks_to_merge(10, blocks, max_size=100, max_gap_size=10, ratio=1.1)
    assert not result

    result = db.find_blocks_to_merge(10, blocks, max_size=100, max_gap_size=10, ratio=2.1)
    assert result == [['path1100', 'path1300']]


def test_find_blocks_to_merge_gaps():
    blocks = [make_block(1000, 10, 10), make_block(1300, 10, 10), make_block(1600, 10, 10)]
    result = db.find_blocks_to_merge(10, blocks, max_size=100, max_gap_size=10, ratio=1.1)
    assert not result


def test_find_blocks_to_merge_max_size():
    blocks = [make_block(1000, 10, 50), make_block(1500, 10, 50)]
    result = db.find_blocks_to_merge(10, blocks, max_size=99, max_gap_size=10, ratio=1.1)
    assert not result

    result = db.find_blocks_to_merge(10, blocks, max_size=100, max_gap_size=10, ratio=1.1)
    assert result == [['path1000', 'path1500']]


def test_storage_read_write(tmpdir):
    class RpcClient:
        @staticmethod
        def call(cmd, keys):
            return {'result': {b'm1': [4]},
                    'start': 1030,
                    'size': 1,
                    'resolution': 10}

    class BrokenRpcClient:
        @staticmethod
        def call(cmd, keys):
            raise Exception('Boo')

    data_dir = str(tmpdir)
    bl = blocks.BlockList(data_dir)
    bl.blocks('10')

    mi = metrics.MetricIndex(os.path.join(data_dir, 'metric.index'))

    reader = db.Reader(bl, [(10, 10)], None)
    info, data = reader.fetch([b'm1'], 500, 1500)
    assert info == (500, 500, 10)
    assert data == {}

    data = [(b'm1', array.array('d', [1, 2, 3]))]
    storage = db.Storage(data_dir, None, None, None, None, mi)
    storage.new_block(data, 1000, 10, 3, [b'm2'])

    info, data = reader.fetch([b'm1'], 500, 1500)
    assert info == (1000, 1030, 10)
    assert data == {b'm1': [1, 2, 3]}

    info, data = reader.fetch([b'm1'], 500, 1020)
    assert info == (1000, 1030, 10)
    assert data == {b'm1': [1, 2, 3]}

    reader = db.Reader(bl, [(10, 10)], RpcClient)
    info, data = reader.fetch([b'm1'], 500, 1030)
    assert info == (1000, 1040, 10)
    assert data == {b'm1': [1, 2, 3, 4]}

    reader = db.Reader(bl, [(10, 10)], BrokenRpcClient)
    info, data = reader.fetch([b'm1'], 500, 1030)
    assert info == (1000, 1030, 10)
    assert data == {b'm1': [1, 2, 3]}
    # assert False


def test_new_data_in_buffer(tmpdir):
    class EmptyRpcClient:
        @staticmethod
        def call(cmd, keys):
            return {'result': {b'm2': [4]},
                    'start': 1030,
                    'size': 1,
                    'resolution': 10}

    data_dir = str(tmpdir)
    bl = blocks.BlockList(data_dir)
    bl.blocks('10')

    data = [(b'm1', [1, 2, 3])]
    db.new_block(data_dir, data, 1000, 10, 3, append=True)

    reader = db.Reader(bl, [(10, 10)], EmptyRpcClient)
    info, data = reader.fetch([b'm1', b'm2'], 500, 1030)
    assert info == (1000, 1040, 10)
    assert data == {b'm1': [1, 2, 3, None],
                    b'm2': [None, None, None, 4]}


def test_storage_house_work(tmpdir):
    data_dir = str(tmpdir)
    mi = metrics.MetricIndex(os.path.join(data_dir, 'metric.index'))
    agg_rules = agg.AggRules({})

    def merge_finder(resolution, blocks):
        return db.find_blocks_to_merge(resolution, blocks, max_size=200,
                                       max_gap_size=10, ratio=1.4)

    def downsample_finder(resolution, blocks, new_resolution, start=0):
        return db.find_blocks_to_downsample(
            resolution, blocks, new_resolution,
            max_size=200, min_size=10,
            max_gap_size=10, start=start
        )

    retentions = [(10, 150), (20, 300)]
    blocks.ensure_block_dirs(data_dir, retentions)
    bl = blocks.BlockList(data_dir)

    storage = db.Storage(data_dir, retentions, merge_finder, downsample_finder, agg_rules, mi)
    storage.do_housework()

    data = [(b'm1', [1, 2, 3, 4, 5])]
    db.new_block(data_dir, data, 1000, 10, 5, append=True)
    db.new_block(data_dir, data, 1050, 10, 5, append=True)
    db.new_block(data_dir, data, 1100, 10, 5, append=True)
    db.new_block(data_dir, data, 1150, 10, 5, append=True)

    storage.do_housework(1200)

    b1, b2, b3 = bl.blocks(10)
    assert b1.start == 1000
    assert b2.start == 1050
    assert b3.start == 1100

    b1, = bl.blocks(20)
    assert b1.start == 1000
    assert b1.end == 1200
    assert b1.size == 10

    storage.do_housework(1450)
    assert not bl.blocks(10, refresh=True)

    b1, = bl.blocks(20, refresh=True)


def test_iter_dump(tmpdir):
    data_dir = str(tmpdir)
    data = [(b'm%06d' % r, [1, 2, 3, 4, 5]) for r in range(1000)]
    blocks.ensure_block_dirs(data_dir, [(10, 10)])
    db.new_block(data_dir, data, 1000, 10, 5, append=True)

    for k, idx, v in db.iter_dump(os.path.join(data_dir, '10', '1000.5.hdb'), 1, 300):
        pass

    assert k == b'm000999'
