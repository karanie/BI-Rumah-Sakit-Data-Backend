import time
import config
from datastore.rdbms import DatastoreDB
import sqlalchemy

ds = DatastoreDB(backend="polars")

def measure_time(func):
    time_start = time.time()
    res = func()
    time_end = time.time()
    return time_end - time_start, res

def get_last_waktu_regis():
    try:
        res_current_time = ds.execute(sqlalchemy.text("SELECT waktu_registrasi FROM dataset ORDER BY Waktu_registrasi DESC LIMIT 1;")).first()
    except Exception as e:
        return None
    current_datetime = res_current_time[0].strftime("%Y-%m-%d %H:%M:%S")
    return current_datetime
