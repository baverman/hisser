# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
import numpy as np
from libc.math cimport isnan


cdef double NAN = float('nan')

cdef void sum_idx_t_impl(const double* data, size_t data_cols, const long* idx, size_t idx_count, double* result) nogil:
    cdef size_t i, j, cnt0, cnt1, cnt2, cnt3, base
    cdef double tmp0, tmp1, tmp2, tmp3
    j = 0
    for j in range(0, (data_cols >> 2) << 2, 4):
        tmp0 = 0.0
        cnt0 = 0
        tmp1 = 0.0
        cnt1 = 0
        tmp2 = 0.0
        cnt2 = 0
        tmp3 = 0.0
        cnt3 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base + 0]):
                cnt0 += 1
                tmp0 += data[base + 0]
            if not isnan(data[base + 1]):
                cnt1 += 1
                tmp1 += data[base + 1]
            if not isnan(data[base + 2]):
                cnt2 += 1
                tmp2 += data[base + 2]
            if not isnan(data[base + 3]):
                cnt3 += 1
                tmp3 += data[base + 3]

        result[j + 0] = tmp0 if cnt0 else NAN
        result[j + 1] = tmp1 if cnt1 else NAN
        result[j + 2] = tmp2 if cnt2 else NAN
        result[j + 3] = tmp3 if cnt3 else NAN

    for j in range(j, data_cols):
        tmp0 = 0.0
        cnt0 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base]):
                cnt0 += 1
                tmp0 += data[base]

        result[j] = tmp0 if cnt0 else NAN


cdef void sum_window_impl(const double* data, size_t count, size_t wsize, size_t wstart, double* result) nogil:
    cdef size_t i, wi, ri
    cdef double acc = 0
    cdef size_t cnt = 0
    wi = wstart
    ri = 0
    i = 0
    while i < count:
        if not isnan(data[i]):
            cnt += 1
            acc += data[i]
        wi += 1
        i += 1
        if wi == wsize:
            result[ri] = acc if cnt else NAN
            ri += 1
            wi = 0
            acc = 0
            cnt = 0

    if wi:
        result[ri] = acc if cnt else NAN


cdef void count_idx_t_impl(const double* data, size_t data_cols, const long* idx, size_t idx_count, double* result) nogil:
    cdef size_t i, j, cnt0, cnt1, cnt2, cnt3, base
    cdef double tmp0, tmp1, tmp2, tmp3
    j = 0
    for j in range(0, (data_cols >> 2) << 2, 4):
        tmp0 = 0.0
        cnt0 = 0
        tmp1 = 0.0
        cnt1 = 0
        tmp2 = 0.0
        cnt2 = 0
        tmp3 = 0.0
        cnt3 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base + 0]):
                cnt0 += 1
                
            if not isnan(data[base + 1]):
                cnt1 += 1
                
            if not isnan(data[base + 2]):
                cnt2 += 1
                
            if not isnan(data[base + 3]):
                cnt3 += 1
                

        result[j + 0] = cnt0 if cnt0 else NAN
        result[j + 1] = cnt1 if cnt1 else NAN
        result[j + 2] = cnt2 if cnt2 else NAN
        result[j + 3] = cnt3 if cnt3 else NAN

    for j in range(j, data_cols):
        tmp0 = 0.0
        cnt0 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base]):
                cnt0 += 1
                

        result[j] = cnt0 if cnt0 else NAN


cdef void count_window_impl(const double* data, size_t count, size_t wsize, size_t wstart, double* result) nogil:
    cdef size_t i, wi, ri
    cdef double acc = 0
    cdef size_t cnt = 0
    wi = wstart
    ri = 0
    i = 0
    while i < count:
        if not isnan(data[i]):
            cnt += 1
            
        wi += 1
        i += 1
        if wi == wsize:
            result[ri] = cnt if cnt else NAN
            ri += 1
            wi = 0
            acc = 0
            cnt = 0

    if wi:
        result[ri] = cnt if cnt else NAN


cdef void mean_idx_t_impl(const double* data, size_t data_cols, const long* idx, size_t idx_count, double* result) nogil:
    cdef size_t i, j, cnt0, cnt1, cnt2, cnt3, base
    cdef double tmp0, tmp1, tmp2, tmp3
    j = 0
    for j in range(0, (data_cols >> 2) << 2, 4):
        tmp0 = 0.0
        cnt0 = 0
        tmp1 = 0.0
        cnt1 = 0
        tmp2 = 0.0
        cnt2 = 0
        tmp3 = 0.0
        cnt3 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base + 0]):
                cnt0 += 1
                tmp0 += data[base + 0]
            if not isnan(data[base + 1]):
                cnt1 += 1
                tmp1 += data[base + 1]
            if not isnan(data[base + 2]):
                cnt2 += 1
                tmp2 += data[base + 2]
            if not isnan(data[base + 3]):
                cnt3 += 1
                tmp3 += data[base + 3]

        result[j + 0] = tmp0 / cnt0 if cnt0 else NAN
        result[j + 1] = tmp1 / cnt1 if cnt1 else NAN
        result[j + 2] = tmp2 / cnt2 if cnt2 else NAN
        result[j + 3] = tmp3 / cnt3 if cnt3 else NAN

    for j in range(j, data_cols):
        tmp0 = 0.0
        cnt0 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base]):
                cnt0 += 1
                tmp0 += data[base]

        result[j] = tmp0 / cnt0 if cnt0 else NAN


cdef void mean_window_impl(const double* data, size_t count, size_t wsize, size_t wstart, double* result) nogil:
    cdef size_t i, wi, ri
    cdef double acc = 0
    cdef size_t cnt = 0
    wi = wstart
    ri = 0
    i = 0
    while i < count:
        if not isnan(data[i]):
            cnt += 1
            acc += data[i]
        wi += 1
        i += 1
        if wi == wsize:
            result[ri] = acc / cnt if cnt else NAN
            ri += 1
            wi = 0
            acc = 0
            cnt = 0

    if wi:
        result[ri] = acc / cnt if cnt else NAN


cdef void first_idx_t_impl(const double* data, size_t data_cols, const long* idx, size_t idx_count, double* result) nogil:
    cdef size_t i, j, cnt0, cnt1, cnt2, cnt3, base
    cdef double tmp0, tmp1, tmp2, tmp3
    j = 0
    for j in range(0, (data_cols >> 2) << 2, 4):
        tmp0 = 0.0
        cnt0 = 0
        tmp1 = 0.0
        cnt1 = 0
        tmp2 = 0.0
        cnt2 = 0
        tmp3 = 0.0
        cnt3 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base + 0]):
                cnt0 += 1
                if cnt0 == 1: tmp0 = data[base + 0]
            if not isnan(data[base + 1]):
                cnt1 += 1
                if cnt1 == 1: tmp1 = data[base + 1]
            if not isnan(data[base + 2]):
                cnt2 += 1
                if cnt2 == 1: tmp2 = data[base + 2]
            if not isnan(data[base + 3]):
                cnt3 += 1
                if cnt3 == 1: tmp3 = data[base + 3]

        result[j + 0] = tmp0 if cnt0 else NAN
        result[j + 1] = tmp1 if cnt1 else NAN
        result[j + 2] = tmp2 if cnt2 else NAN
        result[j + 3] = tmp3 if cnt3 else NAN

    for j in range(j, data_cols):
        tmp0 = 0.0
        cnt0 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base]):
                cnt0 += 1
                if cnt0 == 1: tmp0 = data[base]

        result[j] = tmp0 if cnt0 else NAN


cdef void first_window_impl(const double* data, size_t count, size_t wsize, size_t wstart, double* result) nogil:
    cdef size_t i, wi, ri
    cdef double acc = 0
    cdef size_t cnt = 0
    wi = wstart
    ri = 0
    i = 0
    while i < count:
        if not isnan(data[i]):
            cnt += 1
            if cnt == 1: acc = data[i]
        wi += 1
        i += 1
        if wi == wsize:
            result[ri] = acc if cnt else NAN
            ri += 1
            wi = 0
            acc = 0
            cnt = 0

    if wi:
        result[ri] = acc if cnt else NAN


cdef void last_idx_t_impl(const double* data, size_t data_cols, const long* idx, size_t idx_count, double* result) nogil:
    cdef size_t i, j, cnt0, cnt1, cnt2, cnt3, base
    cdef double tmp0, tmp1, tmp2, tmp3
    j = 0
    for j in range(0, (data_cols >> 2) << 2, 4):
        tmp0 = 0.0
        cnt0 = 0
        tmp1 = 0.0
        cnt1 = 0
        tmp2 = 0.0
        cnt2 = 0
        tmp3 = 0.0
        cnt3 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base + 0]):
                cnt0 += 1
                tmp0 = data[base + 0]
            if not isnan(data[base + 1]):
                cnt1 += 1
                tmp1 = data[base + 1]
            if not isnan(data[base + 2]):
                cnt2 += 1
                tmp2 = data[base + 2]
            if not isnan(data[base + 3]):
                cnt3 += 1
                tmp3 = data[base + 3]

        result[j + 0] = tmp0 if cnt0 else NAN
        result[j + 1] = tmp1 if cnt1 else NAN
        result[j + 2] = tmp2 if cnt2 else NAN
        result[j + 3] = tmp3 if cnt3 else NAN

    for j in range(j, data_cols):
        tmp0 = 0.0
        cnt0 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base]):
                cnt0 += 1
                tmp0 = data[base]

        result[j] = tmp0 if cnt0 else NAN


cdef void last_window_impl(const double* data, size_t count, size_t wsize, size_t wstart, double* result) nogil:
    cdef size_t i, wi, ri
    cdef double acc = 0
    cdef size_t cnt = 0
    wi = wstart
    ri = 0
    i = 0
    while i < count:
        if not isnan(data[i]):
            cnt += 1
            acc = data[i]
        wi += 1
        i += 1
        if wi == wsize:
            result[ri] = acc if cnt else NAN
            ri += 1
            wi = 0
            acc = 0
            cnt = 0

    if wi:
        result[ri] = acc if cnt else NAN


cdef void min_idx_t_impl(const double* data, size_t data_cols, const long* idx, size_t idx_count, double* result) nogil:
    cdef size_t i, j, cnt0, cnt1, cnt2, cnt3, base
    cdef double tmp0, tmp1, tmp2, tmp3
    j = 0
    for j in range(0, (data_cols >> 2) << 2, 4):
        tmp0 = 0.0
        cnt0 = 0
        tmp1 = 0.0
        cnt1 = 0
        tmp2 = 0.0
        cnt2 = 0
        tmp3 = 0.0
        cnt3 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base + 0]):
                cnt0 += 1
                if cnt0 == 1 or data[base + 0] < tmp0: tmp0 = data[base + 0]
            if not isnan(data[base + 1]):
                cnt1 += 1
                if cnt1 == 1 or data[base + 1] < tmp1: tmp1 = data[base + 1]
            if not isnan(data[base + 2]):
                cnt2 += 1
                if cnt2 == 1 or data[base + 2] < tmp2: tmp2 = data[base + 2]
            if not isnan(data[base + 3]):
                cnt3 += 1
                if cnt3 == 1 or data[base + 3] < tmp3: tmp3 = data[base + 3]

        result[j + 0] = tmp0 if cnt0 else NAN
        result[j + 1] = tmp1 if cnt1 else NAN
        result[j + 2] = tmp2 if cnt2 else NAN
        result[j + 3] = tmp3 if cnt3 else NAN

    for j in range(j, data_cols):
        tmp0 = 0.0
        cnt0 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base]):
                cnt0 += 1
                if cnt0 == 1 or data[base] < tmp0: tmp0 = data[base]

        result[j] = tmp0 if cnt0 else NAN


cdef void min_window_impl(const double* data, size_t count, size_t wsize, size_t wstart, double* result) nogil:
    cdef size_t i, wi, ri
    cdef double acc = 0
    cdef size_t cnt = 0
    wi = wstart
    ri = 0
    i = 0
    while i < count:
        if not isnan(data[i]):
            cnt += 1
            if cnt == 1 or data[i] < acc: acc = data[i]
        wi += 1
        i += 1
        if wi == wsize:
            result[ri] = acc if cnt else NAN
            ri += 1
            wi = 0
            acc = 0
            cnt = 0

    if wi:
        result[ri] = acc if cnt else NAN


cdef void max_idx_t_impl(const double* data, size_t data_cols, const long* idx, size_t idx_count, double* result) nogil:
    cdef size_t i, j, cnt0, cnt1, cnt2, cnt3, base
    cdef double tmp0, tmp1, tmp2, tmp3
    j = 0
    for j in range(0, (data_cols >> 2) << 2, 4):
        tmp0 = 0.0
        cnt0 = 0
        tmp1 = 0.0
        cnt1 = 0
        tmp2 = 0.0
        cnt2 = 0
        tmp3 = 0.0
        cnt3 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base + 0]):
                cnt0 += 1
                if cnt0 == 1 or data[base + 0] > tmp0: tmp0 = data[base + 0]
            if not isnan(data[base + 1]):
                cnt1 += 1
                if cnt1 == 1 or data[base + 1] > tmp1: tmp1 = data[base + 1]
            if not isnan(data[base + 2]):
                cnt2 += 1
                if cnt2 == 1 or data[base + 2] > tmp2: tmp2 = data[base + 2]
            if not isnan(data[base + 3]):
                cnt3 += 1
                if cnt3 == 1 or data[base + 3] > tmp3: tmp3 = data[base + 3]

        result[j + 0] = tmp0 if cnt0 else NAN
        result[j + 1] = tmp1 if cnt1 else NAN
        result[j + 2] = tmp2 if cnt2 else NAN
        result[j + 3] = tmp3 if cnt3 else NAN

    for j in range(j, data_cols):
        tmp0 = 0.0
        cnt0 = 0
        for i in range(idx_count):
            base = idx[i]*data_cols + j
            if not isnan(data[base]):
                cnt0 += 1
                if cnt0 == 1 or data[base] > tmp0: tmp0 = data[base]

        result[j] = tmp0 if cnt0 else NAN


cdef void max_window_impl(const double* data, size_t count, size_t wsize, size_t wstart, double* result) nogil:
    cdef size_t i, wi, ri
    cdef double acc = 0
    cdef size_t cnt = 0
    wi = wstart
    ri = 0
    i = 0
    while i < count:
        if not isnan(data[i]):
            cnt += 1
            if cnt == 1 or data[i] > acc: acc = data[i]
        wi += 1
        i += 1
        if wi == wsize:
            result[ri] = acc if cnt else NAN
            ri += 1
            wi = 0
            acc = 0
            cnt = 0

    if wi:
        result[ri] = acc if cnt else NAN




def op_idx_t(str op, const double[:,::1] data, const long[::1] idx, double[::1] result):
    cdef size_t idx_count = idx.shape[0]
    cdef size_t data_cols = data.shape[1]
    if result is None:
        result = np.empty(data_cols, dtype='d')
    cdef double[:] mv = result
    if op == 'sum':
        sum_idx_t_impl(&data[0,0], data_cols, &idx[0], idx_count, &mv[0])
    elif op == 'count':
        count_idx_t_impl(&data[0,0], data_cols, &idx[0], idx_count, &mv[0])
    elif op == 'mean':
        mean_idx_t_impl(&data[0,0], data_cols, &idx[0], idx_count, &mv[0])
    elif op == 'first':
        first_idx_t_impl(&data[0,0], data_cols, &idx[0], idx_count, &mv[0])
    elif op == 'last':
        last_idx_t_impl(&data[0,0], data_cols, &idx[0], idx_count, &mv[0])
    elif op == 'min':
        min_idx_t_impl(&data[0,0], data_cols, &idx[0], idx_count, &mv[0])
    elif op == 'max':
        max_idx_t_impl(&data[0,0], data_cols, &idx[0], idx_count, &mv[0])

    return result


def op_window(str op, const double[::1] data, size_t wsize, size_t offset):
    cdef size_t count = data.shape[0]
    cdef size_t rcount = (count - offset + wsize - 1) // wsize + (offset + wsize - 1) // wsize
    result = np.empty(rcount, dtype='d')
    cdef double[:] mv = result
    cdef size_t wstart = (wsize - offset) % wsize
    if op == 'sum':
        sum_window_impl(&data[0], count, wsize, wstart, &mv[0])
    elif op == 'count':
        count_window_impl(&data[0], count, wsize, wstart, &mv[0])
    elif op == 'mean':
        mean_window_impl(&data[0], count, wsize, wstart, &mv[0])
    elif op == 'first':
        first_window_impl(&data[0], count, wsize, wstart, &mv[0])
    elif op == 'last':
        last_window_impl(&data[0], count, wsize, wstart, &mv[0])
    elif op == 'min':
        min_window_impl(&data[0], count, wsize, wstart, &mv[0])
    elif op == 'max':
        max_window_impl(&data[0], count, wsize, wstart, &mv[0])

    return result


def op_idx_window(str op, const double[:,:] data, const long[::1] idx, size_t wsize, size_t offset):
    cdef size_t i
    cdef size_t idx_count = idx.shape[0]
    cdef size_t count = data.shape[1]
    cdef size_t rcount = (count + offset + wsize - 1) // wsize
    result = np.empty((idx_count, rcount), dtype='d')
    cdef double[:] mv
    for i in range(idx_count):
        mv = result[i]
        if op == 'sum':
            sum_window_impl(&data[idx[i]][0], count, wsize, offset, &mv[0])
        elif op == 'count':
            count_window_impl(&data[idx[i]][0], count, wsize, offset, &mv[0])
        elif op == 'mean':
            mean_window_impl(&data[idx[i]][0], count, wsize, offset, &mv[0])
        elif op == 'first':
            first_window_impl(&data[idx[i]][0], count, wsize, offset, &mv[0])
        elif op == 'last':
            last_window_impl(&data[idx[i]][0], count, wsize, offset, &mv[0])
        elif op == 'min':
            min_window_impl(&data[idx[i]][0], count, wsize, offset, &mv[0])
        elif op == 'max':
            max_window_impl(&data[idx[i]][0], count, wsize, offset, &mv[0])

    return result


def transposed_any(const double[:,::1] data, const long[::1] idx):
    cdef size_t i_max = idx.shape[0]
    cdef size_t j_max = data.shape[1]
    result = np.empty(j_max, dtype='l')
    cdef long[:] mv = result
    cdef size_t i, j, ii, cnt
    for j in range(j_max):
        cnt = 0
        for ii in range(i_max):
            i = idx[ii]
            if not isnan(data[i, j]):
                cnt += 1
                break
        mv[j] = cnt

    return result
