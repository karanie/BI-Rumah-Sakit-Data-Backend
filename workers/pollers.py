import time
import datetime
import requests
from sources.api import SourceAPI
from computes.preprocess import PreprocessPolars
from tools import get_last_waktu_regis, ds

class Pollers():
    def __init__(self, callback, timing: int = 5):
        self.timing = timing
        self._callback = callback

    def run(self):
        while True:
            time.sleep(self.timing)
            self._callback()

class APISourcePollers(Pollers):
    def __init__(self, url: str, timing: int = 5):
        self._callback = self._create_callback(url)
        super().__init__(callback=self._callback, timing=timing)

    def _create_callback(self, url: str):
        def callback():
            try:
                s = SourceAPI()
                last_w = get_last_waktu_regis() or datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df = s.fetch(url, params={"datetime_start": last_w})
                if not df.is_empty():
                    pre = PreprocessPolars()
                    df = pre.preprocess_dataset(df)
                    df.write_parquet("dataset.parquet")
                    df = pre.convert_dtypes(df)
                    ds.write_database(df, engine="adbc")
            except requests.exceptions.ConnectionError as e:
                print(f"Connection Error on {url}")
                print("Retrying in 30s..")
                time.sleep(30)
        return callback
