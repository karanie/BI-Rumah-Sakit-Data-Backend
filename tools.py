import time

def measure_time(func):
    time_start = time.time()
    res = func()
    time_end = time.time()
    return time_end - time_start, res
