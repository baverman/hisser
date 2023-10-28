from ._cffi import ffi, lib


class HashTrie:
    item_size = ffi.sizeof('struct HashTrieNode')
    item_align = ffi.alignof('struct HashTrieNode')

    def __init__(self, size=None, buffer=None):
        self._hashtrie = ffi.new('struct HashTrie *')
        if buffer is None:
            assert size is not None
            self._buffer = ffi.new('struct HashTrieNode[]', size)
        else:
            self._buffer = ffi.from_buffer('struct HashTrieNode[]', buffer)
        lib.hashtrie_init(self._hashtrie, self._buffer)

    def __setitem__(self, key, value):
        lib.hashtrie_set(self._hashtrie, key, value)

    def __getitem__(self, key):
        return lib.hashtrie_get(self._hashtrie, key)
