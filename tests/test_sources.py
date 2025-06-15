import pytest
import sources
import polars as pl
import pandas as pd

def execute(backend, dataset_file):
    s = sources.file.SourceFile(backend=backend)
    df = s.read_dataset(dataset_file)
    if backend == "pandas":
        assert type(df) == pd.DataFrame
    elif backend == "polars":
        assert type(df) == pl.DataFrame

def test_sources_polars_file_csv(dataset_file_csv):
    execute("polars", dataset_file_csv)

def test_sources_pandas_file_csv(dataset_file_csv):
    execute("pandas", dataset_file_csv)

def test_sources_api_object(dummyapi_server):
    import sources.api
    s = sources.api.SourceAPI(backend="object")
    res = s.fetch("http://localhost:8089/api/data/dummy?datetime_start=2000-01-01+00%3A00%3A00")
    assert type(res) == list

def test_sources_api_polars(dummyapi_server):
    import sources.api
    import polars as pl
    s = sources.api.SourceAPI(backend="polars")
    res = s.fetch("http://localhost:8089/api/data/dummy?datetime_start=2000-01-01+00%3A00%3A00")
    assert type(res) == pl.DataFrame
