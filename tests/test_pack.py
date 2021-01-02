import array
from hisser import pack


def test_simple():
    data = array.array('d', [1, 2, 3, 4, 5])
    result = array.array('d', [0, 0, 0])
    pack.unpack_into(result, pack.pack(data))
    assert list(result) == [1, 2, 3]

    mv = memoryview(result)
    pack.unpack_into(mv[1:3], pack.pack(data))
    assert list(result) == [1, 1, 2]
