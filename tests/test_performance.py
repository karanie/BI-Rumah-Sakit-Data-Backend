import random
from tools import measure_time
import sources.file
import datastore.rdbms
import datastore.parquet
import config

def t_test_read_time(tmp_path, dataset_files):
    s_pl = sources.file.SourceFile(backend="polars")
    s_pd = sources.file.SourceFile(backend="pandas")
    t_read_csv_pl, _ = measure_time(lambda: s_pl.read_dataset(str(ds_csv_file)))
    t_read_csv_pd, _ = measure_time(lambda: s_pd.read_dataset(str(ds_csv_file)))
    print(f"# CSV Read time")
    print(f"t_read_csv_pd: {t_read_csv_pd}")
    print(f"t_read_csv_pl: {t_read_csv_pl}")
    print()

    dt_pd = datastore.parquet.DatastoreParquet(backend="pandas")
    dt_pl = datastore.parquet.DatastoreParquet(backend="polars")
    t_read_parquet_pd, _ = measure_time(lambda: dt_pd.read_parquet(ds_parquet_file))
    t_read_parquet_pl, _ = measure_time(lambda: dt_pl.read_parquet(str(ds_parquet_file)))
    print(f"# Parquet Read time")
    print(f"t_read_parquet_pd: {t_read_parquet_pd}")
    print(f"t_read_parquet_pl: {t_read_parquet_pl}")
    print()

    dt_pd = datastore.rdbms.DatastoreDB(backend="pandas", connection=config.DB_CONNECTION)
    dt_pl = datastore.rdbms.DatastoreDB(backend="polars", connection=config.DB_CONNECTION)
    t_read_sql_pd, _ = measure_time(lambda: dt_pd.read_database("SELECT * FROM dataset"))
    t_read_sql_pl_sqlalchemy, _ = measure_time(lambda: dt_pl.read_database("SELECT * FROM dataset"))
    t_read_sql_pl_connectorx, _ = measure_time(lambda: dt_pl._pl_read_database("SELECT * FROM dataset", engine="connectorx"))
    print(f"# SQL Read time - All data")
    print(f"t_read_sql_pd: {t_read_sql_pd}")
    print(f"t_read_sql_pl_sqlalchemy: {t_read_sql_pl_sqlalchemy}")
    print(f"t_read_sql_pl_connectorx: {t_read_sql_pl_connectorx}")
    print()

    t_read_sql_pd, _ = measure_time(lambda: dt_pd._pd_read_database("SELECT waktu_registrasi FROM dataset", dtype=None, parse_dates=None))
    t_read_sql_pl_sqlalchemy, _ = measure_time(lambda: dt_pl.read_database("SELECT waktu_registrasi FROM dataset"))
    t_read_sql_pl_connectorx, _ = measure_time(lambda: dt_pl._pl_read_database("SELECT waktu_registrasi FROM dataset", engine="connectorx"))
    print(f"# SQL Read time - 1 col only")
    print(f"t_read_sql_pd: {t_read_sql_pd}")
    print(f"t_read_sql_pl_sqlalchemy: {t_read_sql_pl_sqlalchemy}")
    print(f"t_read_sql_pl_connectorx: {t_read_sql_pl_connectorx}")
    print()

    t_read_sql_pd, _ = measure_time(lambda: dt_pd.read_database("SELECT * FROM dataset LIMIT 1000"))
    t_read_sql_pl_sqlalchemy, _ = measure_time(lambda: dt_pl.read_database("SELECT * FROM dataset LIMIT 1000"))
    t_read_sql_pl_connectorx, _ = measure_time(lambda: dt_pl._pl_read_database("SELECT * FROM dataset LIMIT 1000", engine="connectorx"))
    print(f"# SQL Read time - All cols, 1000 Rows")
    print(f"t_read_sql_pd: {t_read_sql_pd}")
    print(f"t_read_sql_pl_sqlalchemy: {t_read_sql_pl_sqlalchemy}")
    print(f"t_read_sql_pl_connectorx: {t_read_sql_pl_connectorx}")
    print()

    t_read_sql_pd, _ = measure_time(lambda: dt_pd._pd_read_database("SELECT waktu_registrasi FROM dataset LIMIT 1000", dtype=None, parse_dates=None))
    t_read_sql_pl_sqlalchemy, _ = measure_time(lambda: dt_pl.read_database("SELECT waktu_registrasi FROM dataset LIMIT 1000"))
    t_read_sql_pl_connectorx, _ = measure_time(lambda: dt_pl._pl_read_database("SELECT waktu_registrasi FROM dataset LIMIT 1000", engine="connectorx"))
    print(f"# SQL Read time - All 1 col, 100 Rows")
    print(f"t_read_sql_pd: {t_read_sql_pd}")
    print(f"t_read_sql_pl_sqlalchemy: {t_read_sql_pl_sqlalchemy}")
    print(f"t_read_sql_pl_connectorx: {t_read_sql_pl_connectorx}")
    print()
