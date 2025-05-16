from typing import Annotated
import fastapi
from datastore.rdbms import pl_read_database

router = fastapi.APIRouter()

@router.get("/api/demografi")
async def data_demografi(
    tahun: Annotated[int, fastapi.Query()] = None,
    bulan: Annotated[int | None, fastapi.Query()] = None,
):
    if tahun and bulan:
        df = pl_read_database(
            f"SELECT kabupaten FROM dataset WHERE date_part('year', waktu_registrasi) = {tahun} AND date_part('month', waktu_registrasi) = {bulan}",
        )
    elif tahun:
        df = pl_read_database(
            f"SELECT kabupaten FROM dataset WHERE date_part('year', waktu_registrasi) = {tahun}",
        )
    else:
        df = pl_read_database(
            f"SELECT kabupaten FROM dataset",
        )

    df = df["kabupaten"].value_counts()
    res = {}
    res["index"] = df["kabupaten"].to_list()
    res["values"] = df["count"].to_list()
    return res
