import ctypes

malloc_trim = ctypes.CDLL("libc.so.6").malloc_trim
