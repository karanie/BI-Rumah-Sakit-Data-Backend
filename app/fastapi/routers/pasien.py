from fastapi import APIRouter, Query
from typing import Optional
from tools import ds
import polars as pl

router = APIRouter()

def build_base_query(
    tahun: Optional[int] = None,
    bulan: Optional[int] = None,
    kabupaten: Optional[str] = None
) -> tuple[str, dict]:
    """Build base query with filters"""
    conditions = []
    params = {}

    if kabupaten:
        conditions.append("kabupaten = :kabupaten")
        params["kabupaten"] = kabupaten
    if tahun:
        conditions.append("EXTRACT(YEAR FROM waktu_registrasi) = :tahun")
        params["tahun"] = tahun
    if bulan and tahun:
        conditions.append("EXTRACT(MONTH FROM waktu_registrasi) = :bulan")
        params["bulan"] = bulan

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return where_clause, params

@router.get("/api/pasien")
async def get_pasien(
    tipe_data: str = Query(...),
    tahun: Optional[int] = Query(None),
    bulan: Optional[int] = Query(None),
    kabupaten: Optional[str] = Query(None),
):
    res = {}
    where_clause, params = build_base_query(tahun, bulan, kabupaten)

    if tipe_data == "jumlahJenisKelamin":
        if not tahun:
            # Get current month data if no year specified
            query = f"""
                WITH latest_month AS (
                    SELECT
                        EXTRACT(YEAR FROM MAX(waktu_registrasi)) AS year,
                        EXTRACT(MONTH FROM MAX(waktu_registrasi)) AS month
                    FROM dataset
                )
                SELECT DISTINCT ON (id_pasien, jenis_kelamin)
                    id_pasien,
                    jenis_kelamin
                FROM dataset, latest_month
                WHERE EXTRACT(YEAR FROM waktu_registrasi) = latest_month.year
                AND EXTRACT(MONTH FROM waktu_registrasi) = latest_month.month
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
            """
        else:
            query = f"""
                SELECT DISTINCT ON (id_pasien, jenis_kelamin)
                    id_pasien,
                    jenis_kelamin
                FROM dataset
                {where_clause}
            """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        counts = temp_df.group_by("jenis_kelamin").agg(pl.count().alias("count")).sort("jenis_kelamin")

        res["index"] = counts["jenis_kelamin"].to_list()
        res["values"] = counts["count"].to_list()

    elif tipe_data == "timeseriesJenisKelamin":
        # Determine resample period
        if not tahun and not bulan:
            time_expr = "DATE_TRUNC('year', waktu_registrasi)"
        elif tahun and not bulan:
            time_expr = "DATE_TRUNC('month', waktu_registrasi)"
        else:
            time_expr = "DATE_TRUNC('day', waktu_registrasi)"

        query = f"""
            WITH first_visits AS (
                SELECT DISTINCT ON (id_pasien, jenis_kelamin, {time_expr})
                    {time_expr} AS time_period,
                    jenis_kelamin
                FROM dataset
                {where_clause}
                ORDER BY id_pasien, jenis_kelamin, {time_expr}, waktu_registrasi
            )
            SELECT
                time_period,
                jenis_kelamin,
                COUNT(*) AS count
            FROM first_visits
            GROUP BY time_period, jenis_kelamin
            ORDER BY time_period
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        pivot_df = temp_df.pivot(
            index="time_period",
            columns="jenis_kelamin",
            values="count",
            aggregate_function="sum"
        )

        res["index"] = pivot_df["time_period"].dt.strftime("%Y-%m-%d").to_list()
        res["columns"] = [col for col in pivot_df.columns if col != "time_period"]
        res["values"] = [pivot_df[col].to_list() for col in res["columns"]]

    elif tipe_data == "pekerjaan":
        query = f"""
            SELECT
                pekerjaan,
                COUNT(*) AS count
            FROM dataset
            {where_clause}
            GROUP BY pekerjaan
            ORDER BY count DESC
            LIMIT 10
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["index"] = temp_df["pekerjaan"].to_list()
        res["values"] = temp_df["count"].to_list()

    elif tipe_data == "jumlahPasien":
        if not tahun:
            # Get current month count
            query = f"""
                WITH latest_month AS (
                    SELECT
                        EXTRACT(YEAR FROM MAX(waktu_registrasi)) AS year,
                        EXTRACT(MONTH FROM MAX(waktu_registrasi)) AS month
                    FROM dataset
                )
                SELECT COUNT(DISTINCT id_pasien) AS count
                FROM dataset, latest_month
                WHERE EXTRACT(YEAR FROM waktu_registrasi) = latest_month.year
                AND EXTRACT(MONTH FROM waktu_registrasi) = latest_month.month
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
            """
        else:
            query = f"""
                SELECT COUNT(DISTINCT id_pasien) AS count
                FROM dataset
                {where_clause}
            """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["value"] = temp_df["count"][0]

    elif tipe_data == "jumlahPasienPertahun":
        query = f"""
            WITH first_visits AS (
                SELECT DISTINCT ON (id_pasien)
                    id_pasien,
                    EXTRACT(YEAR FROM waktu_registrasi) AS tahun
                FROM dataset
                ORDER BY id_pasien, waktu_registrasi
            )
            SELECT
                tahun,
                COUNT(*) AS count
            FROM first_visits
            GROUP BY tahun
            ORDER BY tahun
        """

        temp_df = ds.read_database(query)
        res["index"] = temp_df["tahun"].to_list()
        res["values"] = temp_df["count"].to_list()

    elif tipe_data == "pasienLamaBaru":
        if not tahun:
            # Get current month data
            query = f"""
                WITH latest_month AS (
                    SELECT
                        EXTRACT(YEAR FROM MAX(waktu_registrasi)) AS year,
                        EXTRACT(MONTH FROM MAX(waktu_registrasi)) AS month
                    FROM dataset
                )
                SELECT
                    fix_pasien_baru,
                    COUNT(DISTINCT id_pasien) AS count
                FROM dataset, latest_month
                WHERE EXTRACT(YEAR FROM waktu_registrasi) = latest_month.year
                AND EXTRACT(MONTH FROM waktu_registrasi) = latest_month.month
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
                GROUP BY fix_pasien_baru
            """
        else:
            query = f"""
                SELECT
                    fix_pasien_baru,
                    COUNT(DISTINCT id_pasien) AS count
                FROM dataset
                {where_clause}
                GROUP BY fix_pasien_baru
            """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        # Convert to the required format
        new_patients = temp_df.filter(pl.col("fix_pasien_baru") == "t")["count"].to_list()
        returning_patients = temp_df.filter(pl.col("fix_pasien_baru") == "f")["count"].to_list()

        res["index"] = ["Pasien Baru", "Pasien Lama"]
        res["values"] = [
            new_patients[0] if new_patients else 0,
            returning_patients[0] if returning_patients else 0
        ]

    elif tipe_data == "usia":
        query = f"""
            SELECT DISTINCT ON (id_pasien, kategori_usia)
                kategori_usia
            FROM dataset
            {where_clause}
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        counts = temp_df.group_by("kategori_usia").agg(pl.count().alias("count")).sort("kategori_usia")

        res["index"] = counts["kategori_usia"].to_list()
        res["values"] = counts["count"].to_list()

    return res
