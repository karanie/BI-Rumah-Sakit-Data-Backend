from fastapi import APIRouter, Query
from typing import Optional, Literal
from datastore.rdbms import DatastoreDB
import config
import sqlalchemy
import polars as pl
import datetime

router = APIRouter()
dt = DatastoreDB(backend="polars")

@router.get("/api/kunjungan")
async def get_kunjungan(
    tahun: Optional[int] = Query(None),
    bulan: Optional[int] = Query(None),
    kabupaten: Optional[str] = Query(None),
    tipe_data: Optional[str] = Query(None),
    forecast: bool = Query(False),
    timeseries: bool = Query(False),
    diagnosa: Optional[str] = Query(None),
    jenis_registrasi: Optional[str] = Query(None),
    departemen: Optional[str] = Query(None)
):
    res = {}

    # Base conditions for all queries
    conditions = []
    params = {}

    resample_option = "year"
    if kabupaten:
        conditions.append("kabupaten = :kabupaten")
        params["kabupaten"] = kabupaten
    if tahun:
        conditions.append("EXTRACT(YEAR FROM waktu_registrasi) = :tahun")
        params["tahun"] = tahun
        resample_option = "month"
    if bulan and tahun:
        conditions.append("EXTRACT(MONTH FROM waktu_registrasi) = :bulan")
        params["bulan"] = bulan
        resample_option = "day"

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # Get latest waktu_registrasi in table
    engine = sqlalchemy.create_engine(config.DB_CONNECTION)
    with engine.connect() as conn:
        res_current_time = conn.execute(sqlalchemy.text("SELECT waktu_registrasi FROM dataset ORDER BY Waktu_registrasi DESC LIMIT 1;")).first()
        current_date = res_current_time[0].strftime("%Y-%m-%d")

    # Handle each tipe_data case with optimized SQL
    if tipe_data == "pertumbuhan":
        if timeseries:
            if not tahun:
                # Get last 6 months data
                query = f"""
                    SELECT
                        DATE_TRUNC('day', waktu_registrasi) AS day,
                        COUNT(*) AS jumlah_kunjungan
                    FROM dataset
                    WHERE waktu_registrasi >= DATE('{current_date}') - INTERVAL '6 months'
                    {where_clause.replace("WHERE", "AND") if where_clause else ""}
                    GROUP BY day
                    ORDER BY day
                """
            else:
                query = f"""
                    SELECT
                        DATE_TRUNC('{resample_option}', waktu_registrasi) AS time_period,
                        COUNT(*) AS jumlah_kunjungan
                    FROM dataset
                    {where_clause}
                    GROUP BY time_period
                    ORDER BY time_period
                """

            temp_df = dt.read_database(query, execute_options={"parameters": params})

            if temp_df.is_empty():
                return []
            res["index"] = temp_df[temp_df.columns[0]].dt.strftime("%Y-%m-%d").to_list()
            res["columns"] = ["Jumlah Kunjungan"]
            res["values"] = [temp_df["jumlah_kunjungan"].to_list()]

    elif tipe_data == "pertumbuhanPertahun":
        # Why do we even need this
        query = """
            SELECT
                DATE_TRUNC('year', waktu_registrasi) AS time_period,
                COUNT(*) AS jumlah_kunjungan
            FROM dataset
            GROUP BY time_period
            ORDER BY time_period
        """
        temp_df = dt.read_database(query)

        res["index"] = temp_df["time_period"].dt.strftime("%Y-%m-%d").to_list()
        res["columns"] = ["Jumlah Kunjungan"]
        res["values"] = temp_df["jumlah_kunjungan"].to_list()

    elif tipe_data == "rujukan":
        query = f"""
            SELECT
                DATE_TRUNC('{resample_option}', waktu_registrasi) AS time_period,
                rujukan,
                COUNT(*) AS count
            FROM dataset
            {where_clause}
            GROUP BY time_period, rujukan
            ORDER BY time_period
        """

        temp_df = dt.read_database(query, execute_options={"parameters": params})
        pivot_df = temp_df.pivot(
            index="time_period",
            columns="rujukan",
            values="count",
            aggregate_function="sum"
        )

        res["index"] = pivot_df["time_period"].dt.strftime("%Y-%m-%d").to_list()
        res["columns"] = [col for col in pivot_df.columns if col != "month"]
        res["values"] = [pivot_df[col].to_list() for col in res["columns"]]

    elif tipe_data == "usia":
        query = f"""
            SELECT
                DATE_TRUNC('{resample_option}', waktu_registrasi) AS time_period,
                kategori_usia,
                COUNT(*) AS count
            FROM dataset
            {where_clause}
            GROUP BY time_period, kategori_usia
            ORDER BY time_period
        """

        temp_df = dt.read_database(query, execute_options={"parameters": params})
        pivot_df = temp_df.pivot(
            index="time_period",
            columns="kategori_usia",
            values="count",
            aggregate_function="sum"
        )

        res["index"] = pivot_df["time_period"].dt.strftime("%Y-%m-%d").to_list()
        res["columns"] = [col for col in pivot_df.columns if col != "time_period"]
        res["values"] = [pivot_df[col].to_list() for col in res["columns"]]

    elif tipe_data == "jenis_registrasi":
        if not bulan and not tahun:
            resample_option = "day"
            last_six_month = (res_current_time[0] - datetime.timedelta(days=30*6)).strftime("%Y-%m-%d")
            conditions.append(f"waktu_registrasi >= '{last_six_month}'")
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        if not forecast:
            query = f"""
                SELECT
                    DATE_TRUNC('{resample_option}', waktu_registrasi) AS time_period,
                    jenis_registrasi,
                    COUNT(*) AS count
                FROM dataset
                {where_clause}
                GROUP BY time_period, jenis_registrasi
                ORDER BY time_period
            """

            temp_df = dt.read_database(query, execute_options={"parameters": params})
            pivot_df = temp_df.pivot(
                index="time_period",
                columns="jenis_registrasi",
                values="count",
                aggregate_function="sum"
            ).fill_null(0)

            res["index"] = pivot_df["time_period"].dt.strftime("%Y-%m-%d").to_list()
            res["columns"] = [col for col in pivot_df.columns if col != "time_period"]
            res["values"] = [pivot_df[col].to_list() for col in res["columns"]]
        else:
            # Forecasting logic remains similar as it uses pickle files
            pass

    elif tipe_data == "jumlahJenis_registrasi":
        query = """
            SELECT
                jenis_registrasi,
                COUNT(*) AS count
            FROM dataset
            {where_clause}
            GROUP BY jenis_registrasi
        """.format(where_clause=where_clause)

        temp_df = dt.read_database(query, execute_options={"parameters": params})

        res["index"] = temp_df["jenis_registrasi"].to_list()
        res["values"] = temp_df["count"].to_list()

    elif tipe_data == "poliklinik":
        query = """
            SELECT
                nama_departemen,
                COUNT(*) AS count
            FROM dataset
            WHERE jenis_registrasi = 'Rawat Jalan'
            {additional_where}
            GROUP BY nama_departemen
            ORDER BY count DESC
            LIMIT 10
        """.format(
            additional_where=where_clause.replace("WHERE", "AND") if where_clause else ""
        )

        temp_df = dt.read_database(query, execute_options={"parameters": params})

        res["index"] = temp_df["nama_departemen"].to_list()
        res["values"] = temp_df["count"].to_list()

    elif tipe_data == "gejala":
        query = """
            SELECT
                diagnosa_primer,
                COUNT(*) AS count
            FROM dataset
            {where_clause}
            GROUP BY diagnosa_primer
            ORDER BY count DESC
            LIMIT 10
        """.format(where_clause=where_clause)

        temp_df = dt.read_database(query, execute_options={"parameters": params})

        res["index"] = temp_df["diagnosa_primer"].to_list()
        res["values"] = temp_df["count"].to_list()

    elif tipe_data == "jumlahKunjungan":
        if not tahun:
            query = """
                SELECT COUNT(DISTINCT no_registrasi) AS count
                FROM dataset
                WHERE waktu_registrasi >= DATE_TRUNC('month', DATE('{current_date}'))
                {additional_where}
            """.format(
                additional_where=where_clause.replace("WHERE", "AND") if where_clause else "",
                current_date=current_date
            )
        else:
            query = """
                SELECT COUNT(DISTINCT no_registrasi) AS count
                FROM dataset
                {where_clause}
            """.format(where_clause=where_clause)

        temp_df = dt.read_database(query, execute_options={"parameters": params})
        res["value"] = temp_df["count"][0]

    elif tipe_data == "penjamin":
        query = """
            SELECT
                jenis_penjamin,
                COUNT(*) AS count
            FROM dataset
            {where_clause}
            GROUP BY jenis_penjamin
        """.format(where_clause=where_clause)

        temp_df = dt.read_database(query, execute_options={"parameters": params})

        res["index"] = temp_df["jenis_penjamin"].to_list()
        res["values"] = temp_df["count"].to_list()

    elif tipe_data == "regis-byRujukan":
        query = """
            SELECT
                jenis_registrasi,
                rujukan,
                COUNT(*) AS kunjungan
            FROM dataset
            {where_clause}
            GROUP BY jenis_registrasi, rujukan
        """.format(where_clause=where_clause)

        temp_df = dt.read_database(query, execute_options={"parameters": params})

        res = {}
        for group in temp_df.partition_by("jenis_registrasi"):
            regis = group["jenis_registrasi"][0]
            res[regis] = group.select(["rujukan", "kunjungan"]).to_dicts()

    elif tipe_data == "diagnosa":
        if jenis_registrasi:
            params["jenis_registrasi"] = jenis_registrasi

            if not diagnosa:
                query = """
                    SELECT
                        diagnosa_primer,
                        COUNT(*) AS count
                    FROM dataset
                    WHERE jenis_registrasi = :jenis_registrasi
                    {additional_where}
                    GROUP BY diagnosa_primer
                    ORDER BY count DESC
                """.format(
                    additional_where=where_clause.replace("WHERE", "AND") if where_clause else ""
                )

                temp_df = dt.read_database(query, execute_options={"parameters": params})
                temp_df = temp_df.drop_nulls()

                res["index"] = temp_df["diagnosa_primer"].to_list()
                res["values"] = temp_df["count"].to_list()
            else:
                params["diagnosa"] = diagnosa

                if not timeseries:
                    query = """
                        SELECT COUNT(*) AS count
                        FROM dataset
                        WHERE jenis_registrasi = :jenis_registrasi
                        AND diagnosa_primer = :diagnosa
                        {additional_where}
                    """.format(
                        additional_where=where_clause.replace("WHERE", "AND") if where_clause else ""
                    )

                    temp_df = dt.read_database(query, execute_options={"parameters": params})

                    res["index"] = [diagnosa]
                    res["values"] = [temp_df["count"][0]]
                else:
                    if not tahun and not bulan:
                        time_expr = "DATE_TRUNC('year', waktu_registrasi)"
                    elif tahun and not bulan:
                        time_expr = "DATE_TRUNC('month', waktu_registrasi)"
                    else:
                        time_expr = "DATE_TRUNC('day', waktu_registrasi)"

                    query = f"""
                        SELECT
                            {time_expr} AS time_period,
                            COUNT(*) AS count,
                            MODE() WITHIN GROUP (ORDER BY kategori_usia) AS dominant_age
                        FROM dataset
                        WHERE jenis_registrasi = :jenis_registrasi
                        AND diagnosa_primer = :diagnosa
                        {where_clause.replace("WHERE", "AND") if where_clause else ""}
                        GROUP BY time_period
                        ORDER BY time_period
                    """

                    temp_df = dt.read_database(query, execute_options={"parameters": params})

                    res["index"] = temp_df["time_period"].dt.strftime("%Y-%m-%d").to_list()
                    res["columns"] = [diagnosa]
                    res["values"] = temp_df["count"].to_list()
                    res["dominant_age_category"] = temp_df["dominant_age"].to_list()
                    res["dominant_age_category_summary"] = temp_df["dominant_age"].mode()[0]

    elif tipe_data == "departemen":
        if jenis_registrasi:
            params["jenis_registrasi"] = jenis_registrasi

        if not departemen:
            query = """
                SELECT
                    nama_departemen,
                    COUNT(*) AS count
                FROM dataset
                {where_clause}
                {additional_where}
                GROUP BY nama_departemen
                ORDER BY count DESC
            """.format(
                where_clause=where_clause,
                additional_where=f"{"AND" if where_clause else "WHERE"} jenis_registrasi = :jenis_registrasi" if jenis_registrasi else ""
            )

            temp_df = dt.read_database(query, execute_options={"parameters": params})

            res["index"] = temp_df["nama_departemen"].to_list()
            res["values"] = temp_df["count"].to_list()
        else:
            params["departemen"] = departemen

            if not timeseries:
                query = """
                    SELECT
                        COUNT(*) AS count,
                        (
                            SELECT json_agg(json_build_object('diagnosa', diagnosa_primer, 'count', cnt))
                            FROM (
                                SELECT diagnosa_primer, COUNT(*) AS cnt
                                FROM dataset
                                WHERE nama_departemen = :departemen
                                {additional_where}
                                GROUP BY diagnosa_primer
                                ORDER BY cnt DESC
                            ) t
                        ) AS diagnosa_counts
                    FROM dataset
                    WHERE nama_departemen = :departemen
                    {where_clause}
                """.format(
                    where_clause=where_clause.replace("WHERE", "AND") if where_clause else "",
                    additional_where=where_clause.replace("WHERE", "AND") if where_clause else ""
                )

                temp_df = dt.read_database(query, execute_options={"parameters": params})

                res["index"] = [departemen]
                res["values"] = [temp_df["count"][0]]
                res["columns"] = "Jumlah"
                res["indexDiagnosa"] = [d["diagnosa"] for d in temp_df["diagnosa_counts"][0]]
                res["valuesDiagnosa"] = [d["count"] for d in temp_df["diagnosa_counts"][0]]
            else:
                if not tahun and not bulan:
                    time_expr = "DATE_TRUNC('year', waktu_registrasi)"
                elif tahun and not bulan:
                    time_expr = "DATE_TRUNC('month', waktu_registrasi)"
                else:
                    time_expr = "DATE_TRUNC('day', waktu_registrasi)"

                query = f"""
                    SELECT
                        {time_expr} AS time_period,
                        COUNT(*) AS count,
                        MODE() WITHIN GROUP (ORDER BY kategori_usia) AS dominant_age
                    FROM dataset
                    WHERE nama_departemen = :departemen
                    {where_clause.replace("WHERE", "AND") if where_clause else ""}
                    GROUP BY time_period
                    ORDER BY time_period
                """

                temp_df = dt.read_database(query, execute_options={"parameters": params})

                res["index"] = temp_df["time_period"].dt.strftime("%Y-%m-%d").to_list()
                res["columns"] = [departemen]
                res["values"] = temp_df["count"].to_list()
                res["dominant_age_category"] = temp_df["dominant_age"].to_list()
                res["dominant_age_category_summary"] = temp_df["dominant_age"].mode()[0]

    return res
