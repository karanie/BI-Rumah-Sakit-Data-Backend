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
