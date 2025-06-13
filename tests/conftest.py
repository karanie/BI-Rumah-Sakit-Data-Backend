import pytest
import dummyapi
import datastore.rdbms

@pytest.fixture(scope="session")
def datasets(tmp_path_factory):
    print("Generating test dataset")

    generated_ds = dummyapi.generate_dataset("2025-01-01 00:00:00", 10000)
    import pandas as pd
    import polars as pl
    ds = pd.DataFrame(generated_ds)
    ds_pl = pl.from_pandas(ds)

    tmp_root = tmp_path_factory.mktemp("data")

    ds_parquet_file = tmp_root / "dataset.parquet"
    ds_pl.write_parquet(ds_parquet_file)
    ds_csv_file = tmp_root / "dataset.csv"
    ds_pl.write_csv(ds_csv_file)

    sqlite_conn = "sqlite:///" + str(tmp_root / "dataset.db")
    ds_pl.write_database("dataset", sqlite_conn)

    print("Test dataset generated")

    return [ str(ds_parquet_file), str(ds_csv_file), sqlite_conn ]

@pytest.fixture(scope="session")
def dataset_file_parquet(datasets):
    ds_parquet_file, ds_csv_file, sqlite_conn = datasets
    return ds_parquet_file

@pytest.fixture(scope="session")
def dataset_file_csv(datasets):
    ds_parquet_file, ds_csv_file, sqlite_conn = datasets
    return ds_csv_file

@pytest.fixture(scope="session")
def dataset_sqlite_conn(datasets):
    ds_parquet_file, ds_csv_file, sqlite_conn = datasets
    return sqlite_conn

@pytest.fixture(scope="session")
def dummyapi_server():
    import time
    import uvicorn
    import subprocess
    server = subprocess.Popen(
            ["python", "-m", "uvicorn", "dummyapi:app", "--port", "8089"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
    )
    time.sleep(2)
    # Check it started successfully
    assert not server.poll(), server.stdout.read().decode("utf-8")
    yield server
    server.terminate()
