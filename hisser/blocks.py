import os.path
import pathlib
from collections import namedtuple

try:
    from os import scandir
except ImportError:  # pragma: nocover
    from scandir import scandir


class Block(namedtuple('Block', 'start end idx size resolution path')):
    @staticmethod
    def make(start, size, resolution, path):
        return Block(start, start + size * resolution, 0, size, resolution, path)

    def split(self, ts):
        return Slice.make(self).split(ts)

    def slice(self, start, stop=None):
        return Slice.make(self).slice(start, stop)


class Slice(namedtuple('Slice', 'start end idx size resolution path bstart')):
    @staticmethod
    def make(block):
        return Slice(block.start, block.end, 0, block.size,
                     block.resolution, block.path, block.start)

    def split(self, ts):
        if ts <= self.start:
            return None, self
        elif ts >= self.end:
            return self, None

        return self.slice_to(ts), self.slice_from(ts)

    def slice(self, start, stop=None):
        result = self
        if start is not None:
            result = result.slice_from(start)
        if stop is not None:
            result = result and result.slice_to(stop)
        return result

    def slice_from(self, ts):
        if ts <= self.start:
            return self

        if ts >= self.end:
            return None

        start = ts
        end = self.end
        return self._replace(start=start, end=end,
                             idx=(start - self.bstart) // self.resolution,
                             size=(end - start) // self.resolution)

    def slice_to(self, ts):
        if ts <= self.start:
            return None

        if ts >= self.end:
            return self

        start = self.start
        end = ts
        return self._replace(start=start, end=end,
                             idx=(start - self.bstart) // self.resolution,
                             size=(end - start) // self.resolution)


class BlockList:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self._last_state = {}
        self._blocks = {}

    def check(self, resolution, refresh):
        if refresh or resolution not in self._last_state:
            self.rescan(resolution)
            self._last_state[resolution] = 0
            return

        try:
            new_state = os.path.getmtime(block_state_name(self.data_dir, resolution))
        except OSError:
            new_state = 0

        if self._last_state[resolution] < new_state:
            self._last_state[resolution] = new_state
            self.rescan(resolution)

    def blocks(self, resolution, refresh=False):
        self.check(resolution, refresh)
        return self._blocks[resolution]

    def rescan(self, resolution):
        blocks = self._blocks[resolution] = []
        data_path = os.path.join(self.data_dir, str(resolution))

        try:
            entries = scandir(data_path)
        except FileNotFoundError:
            os.makedirs(data_path, exist_ok=True)
            entries = []

        for e in entries:
            if e.name.endswith('.hdb') and e.is_file():
                try:
                    info = get_info(e.path, resolution)
                except ValueError:
                    pass
                else:
                    blocks.append(info)

        blocks.sort()


def get_info(path, res=0):
    ts, size, *rest = os.path.basename(path).split('.')
    ts, size = int(ts), int(size)
    return Block.make(ts, size, res, path)


def block_state_name(data_dir, resolution):
    return os.path.join(data_dir, str(resolution), 'blocks.state')


def notify_blocks_changed(data_dir, resolution):
    pathlib.Path(block_state_name(data_dir, resolution)).touch(exist_ok=True)


def ensure_block_dirs(data_dir, retentions):
    for r, _ in retentions:
        os.makedirs(os.path.join(data_dir, str(r)), exist_ok=True)
