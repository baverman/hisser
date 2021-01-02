# cython: boundscheck=False
# cython: wraparound=False
# cython: cdivision=True
import numpy as np
from libc.math cimport isnan

{% from "hisser/aggop.macro" import make_funcs %}

cdef double NAN = float('nan')

{{ make_funcs('sum', '{acc} += {it}', '{acc} if {cnt} else NAN') }}

{{ make_funcs('count', '', '{cnt} if {cnt} else NAN') }}

{{ make_funcs('mean', '{acc} += {it}', '{acc} / {cnt} if {cnt} else NAN') }}

{{ make_funcs('first', 'if {cnt} == 1: {acc} = {it}', '{acc} if {cnt} else NAN') }}

{{ make_funcs('last', '{acc} = {it}', '{acc} if {cnt} else NAN') }}

{{ make_funcs('min', 'if {cnt} == 1 or {it} < {acc}: {acc} = {it}', '{acc} if {cnt} else NAN') }}

{{ make_funcs('max', 'if {cnt} == 1 or {it} > {acc}: {acc} = {it}', '{acc} if {cnt} else NAN') }}


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
