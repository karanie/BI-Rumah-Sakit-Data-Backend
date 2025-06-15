from typing import Literal
import pandas as pd
import polars as pl
import config

class DatastoreParquet():
    def __init__(self, backend: Literal["polars", "pandas"]="polars"):
        self.backend = backend

    def _pd_read_parquet(self, path) -> pd.DataFrame:
        return pd.read_parquet(path)

    def _pl_read_parquet(self, path) -> pl.DataFrame:
        return pl.read_parquet(path)

    def _pd_write_parquet(self, df: pd.DataFrame, path):
        df.to_parquet(path)

    def _pl_write_parquet(self, df: pl.DataFrame, path):
        df.write_parquet(path)

    def read_parquet(self, path):
        if self.backend == "pandas":
            return self._pd_read_parquet(path)
        if self.backend == "polars":
            return self._pl_read_parquet(path)

    def write_parquet(self, df, path):
        if self.backend == "pandas":
            return self._pd_write_parquet(df, path)
        if self.backend == "polars":
            return self._pl_write_parquet(df, path)
