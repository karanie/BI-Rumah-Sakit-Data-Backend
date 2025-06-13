import pytest
import sources

def execute(backend, dataset_file):
    s = sources.file.SourceFile(backend=backend)
    df = s.read_dataset(dataset_file)
    print(df)
    if backend == "pandas":
        print(df.iloc[0])
    elif backend == "polars":
        print(df.row(0))

def test_sources_polars_file_csv(dataset_file_csv):
    print(dataset_file_csv)
    execute("polars", dataset_file_csv)

def test_sources_pandas_file_csv(dataset_file_csv):
    execute("pandas", dataset_file_csv)

def test_sources_api_object(dummyapi_server):
    import sources.api
    s = sources.api.SourceAPI(backend="object")
    s.fetch("http://localhost:8089/api/data/dummy?datetime_start=2000-01-01+00%3A00%3A00")

def test_sources_api_polars(dummyapi_server):
    import sources.api
    s = sources.api.SourceAPI(backend="polars")
    s.fetch("http://localhost:8089/api/data/dummy?datetime_start=2000-01-01+00%3A00%3A00")
