import os
from contextlib import contextmanager
from time import perf_counter, process_time


_state = [None]
_vals = {}


def profile_func(fn_or_name):
    def decorator(fn):
        return fn

    if callable(fn_or_name):
        return decorator(fn_or_name)

    return decorator


@contextmanager
def profile(name):
    yield


@contextmanager
def real_profile(name):
    real = perf_counter()
    cpu = process_time()

    is_root = not _state[0]
    if is_root:
        name = name,
    else:
        name = _state[0][0] + (name,)

    current = (name, [])
    pstate = _state[0]
    _state[0] = current

    try:
        yield
    finally:
        real_duration = perf_counter() - real
        cpu_duration = process_time() - cpu
        cnt, old_real, old_cpu = _vals.get(current[0], (0, 0, 0))
        _vals[current[0]] = cnt + 1, real_duration + old_real, cpu_duration + old_cpu
        if is_root:
            print_state(_state)
            _state[0] = None
            _vals.clear()
        else:
            _state[0] = pstate
            if not cnt:
                _state[0][-1].append(current)


def real_profile_func(fn_or_name):
    def decorator(fn):
        def inner(*args, **kwargs):
            with real_profile(name):
                return fn(*args, **kwargs)
        return inner

    if callable(fn_or_name):
        name = fn_or_name.__name__
        return decorator(fn_or_name)

    name = fn_or_name
    return decorator


def fmt(d):
    scales = 's', 'ms', 'us', 'ns', 'ps', 'fs'
    i = 0
    while d < 1:
        i += 1
        d *= 1000
    return '{:.3g}{}'.format(d, scales[i])


def print_state(state, level=0):
    for name, children in state:
        cnt, r, c = _vals[name]
        print('##' + '  ' * level, '{}({}) wall: {}, cpu: {}'.format(name[-1], cnt, fmt(r), fmt(c)), flush=True)
        print_state(children, level+1)


if os.environ.get('HISSER_PROFILE'):
    profile = real_profile
    profile_func = real_profile_func
