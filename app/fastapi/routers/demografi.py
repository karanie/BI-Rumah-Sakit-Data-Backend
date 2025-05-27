from typing import Annotated
import fastapi
from tools import ds

router = fastapi.APIRouter()

@router.get("/api/demografi")
async def data_demografi(
    tahun: Annotated[int, fastapi.Query()] = None,
    bulan: Annotated[int | None, fastapi.Query()] = None,
):
    if tahun and bulan:
        df = ds.read_database(
            f"SELECT kabupaten, count(kabupaten) FROM dataset GROUP BY kabupaten WHERE date_part('year', waktu_registrasi) = {tahun} AND date_part('month', waktu_registrasi) = {bulan}",
        )
    elif tahun:
        df = ds.read_database(
            f"SELECT kabupaten, count(kabupaten) FROM dataset WHERE date_part('year', waktu_registrasi) = {tahun} GROUP BY kabupaten",
        )
    else:
        df = ds.read_database(
            f"SELECT kabupaten, count(kabupaten) FROM dataset GROUP BY kabupaten",
        )

    df = df["kabupaten"].value_counts()
    res = {}
    res["index"] = df["kabupaten"].to_list()
    res["values"] = df["count"].to_list()
    return res
