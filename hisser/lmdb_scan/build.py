import os.path
from cffi import FFI
ffibuilder = FFI()

src_dir = os.path.dirname(__file__)
ffibuilder.cdef(open(os.path.join(src_dir, "lmdb_scan.h")).read())

ffibuilder.set_source("hisser.lmdb_scan._cffi",
"""
#include "lmdb_scan.c"
""", include_dirs=[os.path.join(src_dir)])

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
