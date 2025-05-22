import time
import config

def measure_time(func):
    time_start = time.time()
    res = func()
    time_end = time.time()
    return time_end - time_start, res

def get_last_waktu_regis():
    import sqlalchemy
    # Get latest waktu_registrasi in table
    engine = sqlalchemy.create_engine(config.DB_CONNECTION)
    with engine.connect() as conn:
        try:
            res_current_time = conn.execute(sqlalchemy.text("SELECT waktu_registrasi FROM dataset ORDER BY Waktu_registrasi DESC LIMIT 1;")).first()
            current_date = res_current_time[0].strftime("%Y-%m-%d %H:%M:%S")
        except Exception as e:
            return None
    return current_date
