import requests
import json
import polars as pl

class SourceAPI():
    def __init__(self, backend = "polars"):
        self._backend = backend

    def fetch(self, url, params=None):
        res = requests.get(url, params=params)
        if self._backend == "object":
            return json.loads(res.content.decode())
        if self._backend == "polars":
            return pl.read_json(res.content)

