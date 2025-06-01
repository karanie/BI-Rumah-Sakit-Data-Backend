from fastapi import APIRouter, Query
from typing import Optional
from datastore.rdbms import DatastoreDB
import polars as pl
import os
import pickle
from tools import ds

router = APIRouter()

def build_base_query(
    tahun: Optional[int] = None,
    bulan: Optional[int] = None,
    kabupaten: Optional[str] = None,
    jenis_registrasi: Optional[str] = None,
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
    if jenis_registrasi:
        conditions.append("jenis_registrasi = :jenis_registrasi")
        params["jenis_registrasi"] = jenis_registrasi

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return where_clause, params

def get_resample_option(tahun: Optional[int], bulan: Optional[int]) -> str:
    """Determine the appropriate time grouping"""
    if tahun is None and bulan is None:
        return "year"
    elif tahun is not None and bulan is None:
        return "month"
    return "day"

@router.get("/api/pendapatan")
async def get_pendapatan(
    tipe_data: Optional[str] = Query(None),
    tahun: Optional[int] = Query(None),
    bulan: Optional[int] = Query(None),
    kabupaten: Optional[str] = Query(None),
    forecast: bool = Query(False),
    jenis_registrasi: Optional[str] = Query(None),
    poli: Optional[str] = Query(None),
    diagnosa: Optional[str] = Query(None),
    sort: str = Query("pendapatan"),
):
    res = {}
    where_clause, params = build_base_query(tahun, bulan, kabupaten, jenis_registrasi)
    resample_option = get_resample_option(tahun, bulan)
    if sort != "pengeluaran" or sort != "pendapatan":
        sort = "pendapatan"

    if tipe_data == "jenis_registrasi":
        if not forecast:
            if tahun is None and bulan is None:
                resample_option = "day"

            if not tahun:
                # Get last 6 months data
                query = f"""
                    WITH last_date AS (
                        SELECT MAX(waktu_registrasi) AS max_date FROM dataset
                    )
                    SELECT
                        DATE_TRUNC('{resample_option}', waktu_registrasi) AS time_period,
                        jenis_registrasi,
                        SUM(total_tagihan) AS total
                    FROM dataset, last_date
                    WHERE waktu_registrasi >= last_date.max_date - INTERVAL '6 months'
                    {where_clause.replace("WHERE", "AND") if where_clause else ""}
                    GROUP BY time_period, jenis_registrasi
                    ORDER BY time_period
                """
            else:
                query = f"""
                    SELECT
                        DATE_TRUNC('{resample_option}', waktu_registrasi) AS time_period,
                        jenis_registrasi,
                        SUM(total_tagihan) AS total
                    FROM dataset
                    {where_clause}
                    GROUP BY time_period, jenis_registrasi
                    ORDER BY time_period
                """

            temp_df = ds.read_database(query, execute_options={"parameters": params})
            pivot_df = temp_df.pivot(
                index="time_period",
                columns="jenis_registrasi",
                values="total",
                aggregate_function="sum"
            )

            res["index"] = pivot_df["time_period"].dt.strftime("%Y-%m-%d").to_list()
            res["columns"] = [col for col in pivot_df.columns if col != "time_period"]
            res["values"] = [pivot_df[col].fill_null(0).to_list() for col in res["columns"]]
        else:
            # Forecasting logic
            models_path = [
                "data/models/pendapatan/prophet_pendapatan_IGD.pkl",
                "data/models/pendapatan/prophet_pendapatan_OTC.pkl",
                "data/models/pendapatan/prophet_pendapatan_Rawat Jalan.pkl",
                "data/models/pendapatan/prophet_pendapatan_Rawat Inap.pkl",
            ]

            res = []
            forecast_start_date = ds.read_database(
                "SELECT MAX(waktu_registrasi) FROM dataset"
            )["MAX(waktu_registrasi)"][0]

            for path in models_path:
                if os.path.isfile(path):
                    with open(path, "rb") as file:
                        model = pickle.load(file)

                    future = model.make_future_dataframe(periods=30)
                    forecast = model.predict(future)
                    forecast = forecast[forecast['ds'] >= forecast_start_date]

                    res.append({
                        "index": forecast['ds'].dt.strftime("%Y-%m-%d").to_list(),
                        "columns": ["Prediksi Jumlah Pendapatan"],
                        "values": [forecast['yhat'].to_list()]
                    })

    elif tipe_data == "pendapatanPenjamin":
        query = f"""
            SELECT
                jenis_penjamin,
                SUM(total_tagihan) AS total
            FROM dataset
            {where_clause}
            GROUP BY jenis_penjamin
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["index"] = temp_df["jenis_penjamin"].to_list()
        res["values"] = temp_df["total"].to_list()

    elif tipe_data == "pengeluaranPenjamin":
        query = f"""
            SELECT
                jenis_penjamin,
                SUM(total_semua_hpp) AS total
            FROM dataset
            {where_clause}
            GROUP BY jenis_penjamin
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["index"] = temp_df["jenis_penjamin"].to_list()
        res["values"] = temp_df["total"].to_list()

    elif tipe_data == "penjamin":
        query = f"""
            SELECT
                jenis_penjamin,
                SUM(total_tagihan) AS pendapatan,
                SUM(total_semua_hpp) AS pengeluaran
            FROM dataset
            {where_clause}
            GROUP BY jenis_penjamin
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["index"] = temp_df["jenis_penjamin"].to_list()
        res["pendapatan"] = temp_df["pendapatan"].to_list()
        res["pengeluaran"] = temp_df["pengeluaran"].to_list()

    elif tipe_data == "pendapatanLastDay":
        query = f"""
            SELECT
                SUM(total_tagihan) AS pendapatan,
                SUM(total_semua_hpp) AS pengeluaran
            FROM dataset
            WHERE waktu_registrasi >= (SELECT MAX(waktu_registrasi) FROM dataset) - INTERVAL '1 day'
            {where_clause.replace("WHERE", "AND") if where_clause else ""}
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["pendapatanLastDay"] = temp_df["pendapatan"][0]
        res["pengeluaranLastDay"] = temp_df["pengeluaran"][0]

    elif tipe_data == "totalPendapatan":
        if not tahun:
            query = f"""
                WITH last_month AS (
                    SELECT
                        EXTRACT(YEAR FROM MAX(waktu_registrasi)) AS year,
                        EXTRACT(MONTH FROM MAX(waktu_registrasi)) AS month
                    FROM dataset
                )
                SELECT SUM(total_tagihan) AS total
                FROM dataset, last_month
                WHERE EXTRACT(YEAR FROM waktu_registrasi) = last_month.year
                AND EXTRACT(MONTH FROM waktu_registrasi) = last_month.month
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
            """
        else:
            query = f"""
                SELECT SUM(total_tagihan) AS total
                FROM dataset
                {where_clause}
            """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["value"] = temp_df["total"][0]

    elif tipe_data == "totalPengeluaran":
        if not tahun:
            query = f"""
                WITH last_month AS (
                    SELECT
                        EXTRACT(YEAR FROM MAX(waktu_registrasi)) AS year,
                        EXTRACT(MONTH FROM MAX(waktu_registrasi)) AS month
                    FROM dataset
                )
                SELECT SUM(total_semua_hpp) AS total
                FROM dataset, last_month
                WHERE EXTRACT(YEAR FROM waktu_registrasi) = last_month.year
                AND EXTRACT(MONTH FROM waktu_registrasi) = last_month.month
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
            """
        else:
            query = f"""
                SELECT SUM(total_semua_hpp) AS total
                FROM dataset
                {where_clause}
            """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["value"] = temp_df["total"][0]

    elif tipe_data == "total":
        query = f"""
            SELECT
                SUM(total_tagihan) AS pendapatan,
                SUM(total_semua_hpp) AS pengeluaran
            FROM dataset
            {where_clause}
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["pendapatan"] = temp_df["pendapatan"][0]
        res["pengeluaran"] = temp_df["pengeluaran"][0]

    elif tipe_data == "pendapatanGejala":
        query = f"""
            SELECT
                diagnosa_primer,
                SUM(total_tagihan) AS pendapatan
            FROM dataset
            {where_clause}
            GROUP BY diagnosa_primer
            ORDER BY pendapatan DESC
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["index"] = temp_df["diagnosa_primer"].to_list()
        res["values"] = temp_df["pendapatan"].to_list()

    elif tipe_data == "pengeluaranGejala":
        query = f"""
            SELECT
                diagnosa_primer,
                SUM(total_semua_hpp) AS pengeluaran
            FROM dataset
            {where_clause}
            GROUP BY diagnosa_primer
            ORDER BY pengeluaran DESC
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["index"] = temp_df["diagnosa_primer"].to_list()
        res["values"] = temp_df["pengeluaran"].to_list()

    elif tipe_data == "diagnosa":
        if diagnosa:
            # Query for specific diagnosis
            params["diagnosa"] = diagnosa
            query_penjamin = f"""
                SELECT
                    jenis_penjamin,
                    SUM(total_tagihan) AS pendapatan,
                    SUM(total_semua_hpp) AS pengeluaran
                FROM dataset
                WHERE diagnosa_primer = :diagnosa
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
                GROUP BY jenis_penjamin
            """

            temp_df = ds.read_database(query_penjamin, execute_options={"parameters": params})

            res["index"] = [diagnosa]
            res["jenis_penjamin"] = temp_df["jenis_penjamin"].to_list()
            res["pendapatan"] = temp_df["pendapatan"].to_list()
            res["pengeluaran"] = temp_df["pengeluaran"].to_list()

            if jenis_registrasi == "Rawat Inap":
                query_kelas = f"""
                    SELECT
                        kelas_hak,
                        SUM(total_tagihan) AS pendapatan,
                        SUM(total_semua_hpp) AS pengeluaran,
                        AVG(los_rawatan) AS avg_los
                    FROM dataset
                    WHERE diagnosa_primer = :diagnosa
                    {where_clause.replace("WHERE", "AND") if where_clause else ""}
                    GROUP BY kelas_hak
                """

                temp_df = ds.read_database(query_kelas, execute_options={"parameters": params})
                res["kelas_hak"] = temp_df["kelas_hak"].to_list()
                res["pendapatanKelas"] = temp_df["pendapatan"].to_list()
                res["pengeluaranKelas"] = temp_df["pengeluaran"].to_list()
                res["avgLosRawatan"] = round(temp_df["avg_los"][0]) if temp_df["avg_los"][0] else 0
        else:
            # Query for all diagnoses sorted
            query = f"""
                SELECT
                    diagnosa_primer,
                    SUM(total_tagihan) AS pendapatan,
                    SUM(total_semua_hpp) AS pengeluaran
                FROM dataset
                {where_clause}
                GROUP BY diagnosa_primer
                ORDER BY {sort} DESC
            """

            temp_df = ds.read_database(query, execute_options={"parameters": params})
            temp_df = temp_df.drop_nulls()
            res["index"] = temp_df["diagnosa_primer"].to_list()
            res["pendapatan"] = temp_df["pendapatan"].to_list()
            res["pengeluaran"] = temp_df["pengeluaran"].to_list()

    elif tipe_data == "poliklinik":
        if poli:
            params["poli"] = poli
            # Query for specific polyclinic
            query_diagnosa = f"""
                SELECT
                    diagnosa_primer,
                    SUM(total_tagihan) AS pendapatan,
                    SUM(total_semua_hpp) AS pengeluaran
                FROM dataset
                WHERE nama_departemen = :poli
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
                GROUP BY diagnosa_primer
                ORDER BY {sort} DESC
            """

            query_penjamin = f"""
                SELECT
                    jenis_penjamin,
                    SUM(total_tagihan) AS pendapatan,
                    SUM(total_semua_hpp) AS pengeluaran
                FROM dataset
                WHERE nama_departemen = :poli
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
                GROUP BY jenis_penjamin
            """

            temp_diagnosa = ds.read_database(query_diagnosa, execute_options={"parameters": params})
            temp_penjamin = ds.read_database(query_penjamin, execute_options={"parameters": params})

            res["index"] = [poli]
            res["diagnosa"] = temp_diagnosa["diagnosa_primer"].to_list()
            res["pendapatanDiagnosa"] = temp_diagnosa["pendapatan"].to_list()
            res["pengeluaranDiagnosa"] = temp_diagnosa["pengeluaran"].to_list()
            res["jenis_penjamin"] = temp_penjamin["jenis_penjamin"].to_list()
            res["pendapatan"] = temp_penjamin["pendapatan"].to_list()
            res["pengeluaran"] = temp_penjamin["pengeluaran"].to_list()
        else:
            # Query for all polyclinics sorted
            query = f"""
                SELECT
                    nama_departemen,
                    SUM(total_tagihan) AS pendapatan,
                    SUM(total_semua_hpp) AS pengeluaran
                FROM dataset
                {where_clause}
                GROUP BY nama_departemen
                ORDER BY {sort} DESC
            """

            temp_df = ds.read_database(query, execute_options={"parameters": params})
            temp_df = temp_df.drop_nulls()
            res["index"] = temp_df["nama_departemen"].to_list()
            res["pendapatan"] = temp_df["pendapatan"].to_list()
            res["pengeluaran"] = temp_df["pengeluaran"].to_list()

    elif tipe_data in ["poliklinikSortByPendapatan", "poliklinikSortByPengeluaran"]:
        sort_col = "pendapatan" if tipe_data == "poliklinikSortByPendapatan" else "pengeluaran"
        query = f"""
            SELECT
                nama_departemen,
                SUM(total_tagihan) AS pendapatan,
                SUM(total_semua_hpp) AS pengeluaran
            FROM dataset
            {where_clause}
            GROUP BY nama_departemen
            ORDER BY {sort_col} DESC
            LIMIT 10
        """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["index"] = temp_df["nama_departemen"].to_list()
        res["pendapatan"] = temp_df["pendapatan"].to_list()
        res["pengeluaran"] = temp_df["pengeluaran"].to_list()

    elif tipe_data == "profit":
        if not tahun:
            query = f"""
                WITH last_month AS (
                    SELECT
                        EXTRACT(YEAR FROM MAX(waktu_registrasi)) AS year,
                        EXTRACT(MONTH FROM MAX(waktu_registrasi)) AS month
                    FROM dataset
                )
                SELECT
                    SUM(total_tagihan) - SUM(total_semua_hpp) AS profit
                FROM dataset, last_month
                WHERE EXTRACT(YEAR FROM waktu_registrasi) = last_month.year
                AND EXTRACT(MONTH FROM waktu_registrasi) = last_month.month
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
            """
        else:
            query = f"""
                SELECT
                    SUM(total_tagihan) - SUM(total_semua_hpp) AS profit
                FROM dataset
                {where_clause}
            """

        temp_df = ds.read_database(query, execute_options={"parameters": params})
        res["value"] = temp_df["profit"][0]

    else:
        if tahun is None and bulan is None:
            resample_option = "day"

        if not tahun:
            query = f"""
                WITH last_date AS (
                    SELECT MAX(waktu_registrasi) AS max_date FROM dataset
                )
                SELECT
                    DATE_TRUNC('{resample_option}', waktu_registrasi) AS time_period,
                    SUM(total_tagihan) AS total_tagihan,
                    SUM(total_semua_hpp) AS total_semua_hpp
                FROM dataset, last_date
                WHERE waktu_registrasi >= last_date.max_date - INTERVAL '6 months'
                {where_clause.replace("WHERE", "AND") if where_clause else ""}
                GROUP BY time_period
                ORDER BY time_period
            """
        else:
            query = f"""
                SELECT
                    DATE_TRUNC('{resample_option}', waktu_registrasi) AS time_period,
                    SUM(total_tagihan) AS total_tagihan,
                    SUM(total_semua_hpp) AS total_semua_hpp
                FROM dataset
                {where_clause}
                GROUP BY time_period
                ORDER BY time_period
            """
        temp_df = ds.read_database(query, execute_options={"parameters": params})
        if not forecast:
            res["index"] = temp_df["time_period"].dt.strftime("%Y-%m-%d").to_list()
            res["columns"] = ["total_tagihan", "total_semua_hpp"]
            res["values"] = [
                temp_df["total_tagihan"].to_list(),
                temp_df["total_semua_hpp"].to_list()
            ]
        else:
            from computes.prophet import predict
            res = []
            for jenis_reg in temp_df["jenis_registrasi"].unique():
                t_df = temp_df.filter(pl.col("jenis_registrasi") == jenis_reg)
                prediction = predict(t_df, periods=30, ds_col="time_period", y_col="count")
                prediction = prediction[prediction["ds"] >= t_df["time_period"].max()]
                res.append({
                    "index": prediction.ds.dt.strftime("%Y-%m-%d").tolist(),
                    "columns": [f"Prediksi {jenis_reg}"],
                    "values": [prediction.yhat.tolist()]
                })
            return res
    return res
