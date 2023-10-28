import os.path
from cffi import FFI
ffibuilder = FFI()

src_dir = os.path.dirname(__file__)
ffibuilder.cdef(open(os.path.join(src_dir, "hashtrie.h")).read())

ffibuilder.set_source("hisser.hashtrie._cffi",
"""
#include "hashtrie.c"
""", include_dirs=[os.path.join(src_dir)])

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
