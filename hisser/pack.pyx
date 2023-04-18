# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
cimport cython
from cpython cimport array
import array
from libc.string cimport memcpy
from libc.stdint cimport uint32_t
from libc.math cimport isnan


cpdef array_is_empty(array.array data):
    return _array_is_empty(data.data.as_doubles, len(data))


cdef int _array_is_empty(double* data, size_t count) nogil:
    cdef size_t i
    for i in range(count):
        if not isnan(data[i]):
            return False
    return True


cpdef unpack(data, count):
    cdef array.array result = array.array('d', bytes(count*8))
    cdef array.array buf = array.array('B', data)
    _decode(buf.data.as_uchars, len(data), result.data.as_uchars, count*8)
    return result


def unpack_into(double [::1] view, const unsigned char [::1] data):
    _decode(&data[0], data.shape[0], <unsigned char*>&view[0], view.shape[0]*8)


cdef void _decode(const unsigned char *data, ssize_t data_len, unsigned char *result, ssize_t result_len) nogil:
    cdef ssize_t c = 0
    cdef ssize_t rc = 0
    cdef unsigned int num = 0
    cdef int t = 0
    while c < data_len and rc < result_len:
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
            for _ in range(min(num, (result_len - rc) // 8)):
                memcpy(result + rc, data + c, 8)
                rc += 8
            c += 8
        else:
            memcpy(result + rc, data + c, min(8*num, result_len - rc))
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


cpdef pack(double [::1] view):
    cdef array.array result = array.array('B', bytes(view.shape[0] * 8 * 2))
    cdef array.array buf = array.array('Q', bytes(view.shape[0] * 8 * 2))
    cdef size_t offset = _encode(<unsigned long long*>&view[0], view.shape[0], result.data.as_uchars, buf.data.as_ulonglongs)
    array.resize(result, offset)
    return result


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
