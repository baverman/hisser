cimport cython
from cpython cimport array
import array
from libc.string cimport memcpy
from libc.stdint cimport uint32_t
from libc.math cimport isnan


cpdef array_is_empty(array.array data):
    return _array_is_empty(data.data.as_doubles, len(data))


@cython.boundscheck(False)
@cython.wraparound(False)
cdef int _array_is_empty(double* data, size_t count) nogil:
    cdef size_t i
    for i in range(count):
        if not isnan(data[i]):
            return False
    return True


cpdef unpack(data, count):
    cdef array.array result = array.array('d', bytes(count*8))
    cdef array.array buf = array.array('B', data)
    _decode(buf.data.as_uchars, len(data), result.data.as_uchars)
    return result


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cdef void _decode(unsigned char *data, size_t data_len, unsigned char *result) nogil:
    cdef size_t c = 0
    cdef size_t rc = 0
    cdef unsigned int num = 0
    cdef int t = 0
    while c < data_len:
        t = data[c] & 0xc0
        if t == 0 or t == 64:
            num = data[c]
            c += 1
        elif t == 0x80:
            num = ((data[c] << 8) + data[c+1]) & 0x3fff
            c += 2
        elif t == 0xc0:
            num = ((data[c] << 24) + (data[c+1] << 16) + (data[c+2] << 8) + data[c+3]) & 0x3fffffff
            c += 4

        t = num % 2
        num = num >> 1
        # print('decode', t, num, c)

        if t:
            for _ in range(num):
                memcpy(result + rc, data + c, 8)
                rc += 8
            c += 8
        else:
            memcpy(result + rc, data + c, 8*num)
            rc += 8 * num
            c += 8 * num


cdef inline int encode_varint(unsigned char *buf, int offset, uint32_t num) nogil:
    if num < 0x80:
        buf[offset] = num
        return offset + 1
    elif num < 0x4000:
        num = num | 0x8000
        buf[offset+1] = num & 0xff
        buf[offset] = num >> 8
        return offset + 2
    elif num < 0x40000000ul:
        num = num | 0xc0000000ul
        buf[offset+3] = num & 0xff
        buf[offset+2] = (num >> 8) & 0xff
        buf[offset+1] = (num >> 16) & 0xff
        buf[offset] = num >> 24
        return offset + 4
    return 0


cpdef pack(array.array data):
    cdef array.array result = array.array('B', bytes(len(data) * 8 * 2))
    cdef array.array buf = array.array('Q', bytes(len(data) * 8 * 2))
    cdef size_t offset = _encode(data.data.as_ulonglongs, len(data), result.data.as_uchars, buf.data.as_ulonglongs)
    array.resize(result, offset)
    return result


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cdef size_t _encode(unsigned long long *data, size_t count,
                    unsigned char *result,
                    unsigned long long *buf) nogil:
    cdef size_t i = 0
    cdef size_t buf_count = 0
    cdef size_t rcount = 0
    cdef size_t offset = 0
    cdef unsigned long long prev = 0
    cdef unsigned long long val
    for i in range(count):
        val = data[i]
        if not rcount:
            prev = val
            rcount += 1
            continue

        if prev == val:
            rcount += 1
        else:
            if rcount > 1:
                if buf_count:
                    # print('encode', 0, buf_count, offset)
                    offset = encode_varint(result, offset, buf_count << 1)
                    memcpy(result + offset, <char *>buf, buf_count * 8)
                    offset += 8 * buf_count
                    buf_count = 0
                # print('encode', 1, rcount, prev, offset)
                offset = encode_varint(result, offset, (rcount << 1) + 1)
                (<unsigned long long *>(result + offset))[0] = prev
                offset += 8
                prev = val
                rcount = 1
            else:
                buf[buf_count] = prev
                buf_count += 1
                prev = val
                rcount = 1

    # print('final', buf_count, rcount, prev)
    if buf_count:
        if rcount == 1:
            # print('encode', 0, buf_count + 1, offset)
            offset = encode_varint(result, offset, (buf_count + 1) << 1)
            memcpy(result + offset, <char *>buf, buf_count * 8)
            offset += 8 * buf_count
            (<unsigned long long *>(result + offset))[0] = prev
            offset += 8
            rcount = 0
        else:
            # print('encode', 0, buf_count, offset)
            offset = encode_varint(result, offset, buf_count << 1)
            memcpy(result + offset, <char *>buf, buf_count * 8)
            offset += 8 * buf_count

    if rcount:
        # print('encode', 1, rcount, prev, offset)
        offset = encode_varint(result, offset, (rcount << 1) + 1)
        (<unsigned long long *>(result + offset))[0] = prev
        offset += 8

    return offset
