import pickle
import gzip
import pytest
import polars as pl
import pandas as pd
import datastore.rdbms
import datastore.parquet
import datastore.file

def test_datastore_read_parquet_polars(dataset_file_parquet):
    dt = datastore.parquet.DatastoreParquet(backend="polars")
    df = dt.read_parquet(dataset_file_parquet)
    assert type(df) == pl.DataFrame

def test_datastore_read_parquet_pandas(dataset_file_parquet):
    dt = datastore.parquet.DatastoreParquet(backend="pandas")
    df = dt.read_parquet(dataset_file_parquet)
    assert type(df) == pd.DataFrame

def test_datastore_write_parquet_polars(tmp_path):
    df = pl.DataFrame({"a": [1,2,3], "b": ['x','y','z']})
    path = tmp_path / "test_write_parquet_polars.parquet"
    dt = datastore.parquet.DatastoreParquet(backend="polars")
    dt.write_parquet(df, path)

def test_datastore_write_parquet_pandas(tmp_path):
    df = pd.DataFrame({"a": [1,2,3], "b": ['x','y','z']})
    path = tmp_path / "test_write_parquet_pandas.parquet"
    dt = datastore.parquet.DatastoreParquet(backend="pandas")
    dt.write_parquet(df, path)

def test_datastore_file_pickle(dataset_file_pickle):
    df = datastore.file.read_pickle(dataset_file_pickle)
    assert type(df) == pd.DataFrame

def test_datastore_file_pickle_gzip(dataset_file_pickle_gzip):
    df = datastore.file.read_pickle(dataset_file_pickle_gzip)
    assert type(df) == pd.DataFrame

def test_datastore_file_save_as_pickle_compressed(tmp_path):
    df = { "foo": "bar" }
    filename = tmp_path / "foobar.pkl.gz"
    datastore.file.save_dataset_as_pickle(df, str(filename))

    with gzip.open(filename, "rb") as f:
        read_obj = pickle.load(f)
        assert type(read_obj) == dict
        assert list(df.keys()) == ["foo"]
        assert list(df.values()) == ["bar"]

def test_datastore_file_save_as_pickle(tmp_path):
    df = { "foo": "bar" }
    filename = tmp_path / "foobar.pkl"
    datastore.file.save_dataset_as_pickle(df, str(filename), compress=False)

    with open(filename, "rb") as f:
        read_obj = pickle.load(f)
        assert type(read_obj) == dict
        assert list(df.keys()) == ["foo"]
        assert list(df.values()) == ["bar"]

def test_datastore_rdbms_read_polars_sqlalchemy(dataset_sqlite_conn):
    ds = datastore.rdbms.DatastoreDB(backend="polars", connection=dataset_sqlite_conn)
    df = ds.read_database("select * from dataset limit 1")
    assert type(df) == pl.DataFrame

def test_datastore_rdbms_read_polars_connectorx(dataset_sqlite_conn):
    ds = datastore.rdbms.DatastoreDB(backend="polars", connection=dataset_sqlite_conn)
    df = ds.read_database("select * from dataset limit 1", engine="connectorx")
    assert type(df) == pl.DataFrame

def test_datastore_rdbms_write_adbc(dataset_sqlite_conn):
    from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

    engine = create_engine(dataset_sqlite_conn)
    meta = MetaData()
    dataset_test_write = Table(
       'dataset_test_write_adbc', meta,
       Column('a', Integer, primary_key = True),
       Column('b', String),
    )
    meta.create_all(engine)

    df = pl.DataFrame({"a": [1,2,3], "b": ['x','y','z']})
    ds = datastore.rdbms.DatastoreDB(backend="polars", connection=dataset_sqlite_conn)
    ds.write_database(df, db_table="dataset_test_write_adbc")

def test_datastore_rdbms_write_sqlalchemy(dataset_sqlite_conn):
    from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData

    engine = create_engine(dataset_sqlite_conn)
    meta = MetaData()
    dataset_test_write = Table(
       'dataset_test_write_sqlalchemy', meta,
       Column('a', Integer, primary_key = True),
       Column('b', String),
    )
    meta.create_all(engine)

    df = pl.DataFrame({"a": [1,2,3], "b": ['x','y','z']})
    ds = datastore.rdbms.DatastoreDB(backend="polars", connection=dataset_sqlite_conn)
    ds.write_database(df, db_table="dataset_test_write_sqlalchemy", engine="sqlalchemy")
