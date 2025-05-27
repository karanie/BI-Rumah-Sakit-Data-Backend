import datetime
import os
import time
from fastapi import APIRouter, Query
from typing import Optional, Literal
from tools import ds
import config
import sqlalchemy
import polars as pl

router = APIRouter()

@router.get("/api/last-update")
async def last_update():
    res = {}
    res["mtime"] = os.path.getmtime(config.DATASTORE_FILE_PATH)
    res["mtimeLocaltime"] = time.ctime(os.path.getmtime(config.DATASTORE_FILE_PATH))

    # Get latest waktu_registrasi in table
    res_current_time = ds.execute(sqlalchemy.text("SELECT waktu_registrasi FROM dataset ORDER BY Waktu_registrasi DESC LIMIT 1;")).first()
    current_date = res_current_time[0].strftime("%Y-%m-%d")

    res["waktuRegistrasiTerakhir"] = current_date
    return res

@router.get("/api/filter-options")
async def data_filter_options():
    res = {}
    temp_df_kab = ds.read_database("SELECT DISTINCT kabupaten FROM dataset")
    temp_df_waktu = ds.read_database("SELECT MIN(waktu_registrasi) as mint, MAX(waktu_registrasi) as maxt FROM dataset",)
    res["kabupaten"] = temp_df_kab["kabupaten"].to_list()
    print()
    res["tahun"] = list(range(temp_df_waktu["mint"].to_list()[0].year, temp_df_waktu["maxt"].to_list()[0].year + 1))
    res["bulan"] = list(range(1, 13))
    return res
