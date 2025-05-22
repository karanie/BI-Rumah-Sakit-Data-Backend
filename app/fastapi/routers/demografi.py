from typing import Annotated
import fastapi
from datastore.rdbms import DatastoreDB

router = fastapi.APIRouter()

@router.get("/api/demografi")
async def data_demografi(
    tahun: Annotated[int, fastapi.Query()] = None,
    bulan: Annotated[int | None, fastapi.Query()] = None,
):
    dt = DatastoreDB(backend="polars")
    if tahun and bulan:
        df = dt.read_database(
            f"SELECT kabupaten, count(kabupaten) FROM dataset GROUP BY kabupaten WHERE date_part('year', waktu_registrasi) = {tahun} AND date_part('month', waktu_registrasi) = {bulan}",
        )
    elif tahun:
        df = dt.read_database(
            f"SELECT kabupaten, count(kabupaten) FROM dataset WHERE date_part('year', waktu_registrasi) = {tahun} GROUP BY kabupaten",
        )
    else:
        df = dt.read_database(
            f"SELECT kabupaten, count(kabupaten) FROM dataset GROUP BY kabupaten",
        )

    df = df["kabupaten"].value_counts()
    res = {}
    res["index"] = df["kabupaten"].to_list()
    res["values"] = df["count"].to_list()
    return res
