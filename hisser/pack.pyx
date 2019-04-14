# cython: language_level=3
cimport cython
from cpython cimport array
from cpython.ref cimport PyObject
import array
from libc.string cimport memcpy
from libc.stdint cimport uint32_t
from libc.math cimport isnan

cdef extern from *:
    PyObject* PyList_GET_ITEM(PyObject* p, Py_ssize_t pos) nogil
    double PyFloat_AsDouble(PyObject *pyfloat) nogil
    double PyFloat_AS_DOUBLE(PyObject *pyfloat) nogil
    PyObject* Py_None
    PyObject* PySequence_Fast(PyObject *o, const char *m) nogil
    PyObject* PySequence_Fast_GET_ITEM(PyObject *o, Py_ssize_t i) nogil
    PyObject** PySequence_Fast_ITEMS(PyObject *o) nogil
    void Py_DECREF(PyObject *o)


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


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef moving_average(seq, size_t size, list result):
    cdef size_t valcnt = 0
    cdef size_t wlen = 0
    cdef size_t bcount = 0
    cdef double value = 0
    cdef double dit = 0

    for it in seq:
        if it is not None:
            dit = it
            value = (value * wlen + dit) / (wlen + 1.0)
            wlen += 1

        valcnt += 1
        if valcnt == size:
            if wlen:
                result[bcount] = value
            wlen = 0
            value = 0
            valcnt = 0
            bcount += 1

    if wlen:
        result[bcount] = value


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef moving_sum(seq, size_t size, list result):
    cdef size_t valcnt = 0
    cdef size_t wlen = 0
    cdef size_t bcount = 0
    cdef double value = 0
    cdef double dit = 0

    for it in seq:
        if it is not None:
            dit = it
            value += dit
            wlen += 1

        valcnt += 1
        if valcnt == size:
            if wlen:
                result[bcount] = value
            wlen = 0
            value = 0
            valcnt = 0
            bcount += 1

    if wlen:
        result[bcount] = value


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef moving_min(seq, size_t size, list result):
    cdef size_t valcnt = 0
    cdef size_t wlen = 0
    cdef size_t bcount = 0
    cdef double value = 0
    cdef double dit = 0

    for it in seq:
        if it is not None:
            dit = it
            if wlen:
                if dit < value:
                    value = dit
            else:
                value = dit
            wlen += 1

        valcnt += 1
        if valcnt == size:
            if wlen:
                result[bcount] = value
            wlen = 0
            value = 0
            valcnt = 0
            bcount += 1

    if wlen:
        result[bcount] = value


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef moving_max(seq, size_t size, list result):
    cdef size_t valcnt = 0
    cdef size_t wlen = 0
    cdef size_t bcount = 0
    cdef double value = 0
    cdef double dit = 0

    for it in seq:
        if it is not None:
            dit = it
            if wlen:
                if dit > value:
                    value = dit
            else:
                value = dit
            wlen += 1

        valcnt += 1
        if valcnt == size:
            if wlen:
                result[bcount] = value
            wlen = 0
            value = 0
            valcnt = 0
            bcount += 1

    if wlen:
        result[bcount] = value


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef moving_first(seq, size_t size, list result):
    cdef size_t valcnt = 0
    cdef size_t wlen = 0
    cdef size_t bcount = 0
    cdef double value = 0

    for it in seq:
        if it is not None:
            if not wlen:
                value = it
            wlen += 1

        valcnt += 1
        if valcnt == size:
            if wlen:
                result[bcount] = value
            wlen = 0
            value = 0
            valcnt = 0
            bcount += 1

    if wlen:
        result[bcount] = value


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef moving_last(seq, size_t size, list result):
    cdef size_t valcnt = 0
    cdef size_t wlen = 0
    cdef size_t bcount = 0
    cdef double value = 0

    for it in seq:
        if it is not None:
            value = it
            wlen += 1

        valcnt += 1
        if valcnt == size:
            if wlen:
                result[bcount] = value
            wlen = 0
            value = 0
            valcnt = 0
            bcount += 1

    if wlen:
        result[bcount] = value


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef list replace_nans(list result):
    cdef size_t i = 0
    cdef double it
    for it in result:
        if isnan(it):
            result[i] = None
        i += 1
    return result


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef object array_mean(array.array values):
    cdef size_t i
    cdef size_t length = len(values)
    cdef double v
    cdef size_t non_empty = 0
    cdef double total = 0
    cdef double* vals = values.data.as_doubles

    for i in range(length):
        v = vals[i]
        if not isnan(v):
            total += v
            non_empty += 1

    if non_empty > 0:
        return total / non_empty
    else:
        return float('nan')


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef object safe_average(object values):
    cdef size_t i
    cdef size_t length = len(values)
    cdef PyObject* vo
    cdef size_t non_empty = 0
    cdef double total = 0

    cdef PyObject* seq = PySequence_Fast(<PyObject*>values, 'invalid object')

    for i in range(length):
        vo = PySequence_Fast_GET_ITEM(seq, i)
        if vo != Py_None:
            total += PyFloat_AS_DOUBLE(vo)
            non_empty += 1

    Py_DECREF(seq)

    if non_empty > 0:
        return total / non_empty
    else:
        return None


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef object safe_sum(object values):
    cdef size_t i
    cdef size_t length = len(values)
    cdef PyObject* vo
    cdef size_t non_empty = 0
    cdef double total = 0

    cdef PyObject* seq = PySequence_Fast(<PyObject*>values, 'invalid object')

    for i in range(length):
        vo = PySequence_Fast_GET_ITEM(seq, i)
        if vo != Py_None:
            total += PyFloat_AS_DOUBLE(vo)
            non_empty += 1

    Py_DECREF(seq)

    if non_empty > 0:
        return total
    else:
        return None


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef object safe_count(object values):
    cdef size_t i
    cdef size_t length = len(values)
    cdef PyObject* vo
    cdef size_t non_empty = 0

    cdef PyObject* seq = PySequence_Fast(<PyObject*>values, 'invalid object')

    for i in range(length):
        vo = PySequence_Fast_GET_ITEM(seq, i)
        if vo != Py_None:
            non_empty += 1

    Py_DECREF(seq)
    return non_empty


@cython.boundscheck(False)
@cython.wraparound(False)
@cython.cdivision(True)
cpdef object transposed_average(object slist):
    cdef size_t i, j
    cdef size_t slength = len(slist)
    cdef size_t length = len(slist[0])
    cdef PyObject* vo
    cdef size_t non_empty = 0
    cdef double total = 0

    cdef list result = []

    cdef PyObject* seq = PySequence_Fast(<PyObject*>slist, 'invalid object')
    cdef PyObject** items = PySequence_Fast_ITEMS(seq)

    for i in range(length):
        non_empty = 0
        total = 0
        for j in range(slength):
            vo = PyList_GET_ITEM(items[j], i)
            if vo != Py_None:
                total += PyFloat_AsDouble(vo)
                non_empty += 1

        if non_empty > 0:
            result.append(total / non_empty)
        else:
            result.append(None)

    return result
