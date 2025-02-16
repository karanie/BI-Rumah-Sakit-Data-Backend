import os, pickle
from flask import Blueprint, request
import pandas as pd
from utils.filterdf import filter_in_year, filter_in_year_month,filter_last
import data as d

routes_pendapatan = Blueprint("routes_pendapatan", __name__)
@routes_pendapatan.route("/api/pendapatan", methods=["GET"])
def routes():
    res = {}
    tahun = request.args.get("tahun", type=int)
    bulan = request.args.get("bulan", type=int)
    kabupaten = request.args.get("kabupaten", type=str)
    forecast = request.args.get("forecast", type=bool)
    tipe_data = request.args.get("tipe_data")

    #modified by chintya
    jenis_registrasi = request.args.get("jenisregistrasi", type=str)
    poli = request.args.get("poli", type=str)
    diagnosa = request.args.get("diagnosa", type=str)
    sort_arg = request.args.get("sort", type=str)

    if sort_arg is None :
        sort_arg = "pendapatan"

    temp_df = d.dataset

    if jenis_registrasi is not None:
        temp_df = temp_df[temp_df["jenis_registrasi"] == jenis_registrasi]

    if kabupaten is not None:
        temp_df = temp_df[temp_df["kabupaten"] == kabupaten]
    if tahun is not None:
        temp_df =  filter_in_year(temp_df,"waktu_registrasi",tahun)
    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df,"waktu_registrasi",tahun,bulan)


    if tahun is None and bulan is None:
        resample_option = "YE"
    elif tahun is not None and bulan is None:
        resample_option = "M"
    else:
        resample_option = "D"

    # Mengelompokkan data berdasarkan 'diagnosa_primer' dan menghitung jumlah total pendapatan dan pengeluaran
    grouped_data = temp_df.groupby('diagnosa_primer').agg(
        pendapatan=('total_tagihan', 'sum'),
        pengeluaran=('total_semua_hpp', 'sum')
    ).reset_index()

    #  Grouping poliklinik
    # filtered_data = temp_df[temp_df["jenis_registrasi"] == "Rawat Jalan"]
    grouped_dataPoli = temp_df.groupby('nama_departemen').agg(
        pendapatan=('total_tagihan', 'sum'),
        pengeluaran=('total_semua_hpp', 'sum')
    ).reset_index()

    if tipe_data == "jenis_registrasi":
        if not forecast:
            if tahun is None:
                temp_df = filter_last(temp_df, "waktu_registrasi", from_last_data = "True", months = 6)

            if tahun is None and bulan is None:
                resample_option = "D"

            temp_df = temp_df[["waktu_registrasi","jenis_registrasi","total_tagihan"]]
            # Grouping berdasarkan jenisregis
            temp_df = temp_df.groupby(['jenis_registrasi', pd.Grouper(key='waktu_registrasi', freq=resample_option)])['total_tagihan'].sum().reset_index()
            temp_df = temp_df.pivot_table(index='waktu_registrasi', columns='jenis_registrasi', values='total_tagihan', fill_value=0)


            res["index"] = temp_df.index.strftime(f"%Y-%m{'-%d' if resample_option == 'D' else '' }").tolist()
            res["columns"] = temp_df.columns.tolist()
            res["values"] = temp_df.values.transpose().tolist()
            return res

        else:
            # Preprocess
            d.dataset['waktu_registrasi'] = pd.to_datetime(d.dataset['waktu_registrasi'], format= "%Y/%m/%d")
            df = d.dataset[['waktu_registrasi', 'jenis_registrasi', 'id_registrasi']]
            agg_df = df.groupby(['waktu_registrasi','jenis_registrasi']).agg({'id_registrasi':'count'}).reset_index().sort_values(['jenis_registrasi','waktu_registrasi'])
            total_sales_df = agg_df.pivot(index='waktu_registrasi',columns='jenis_registrasi', values='id_registrasi')

            # Modelling
            prediction_days = request.args.get("days", type=int)
            forecast_start_date = max(total_sales_df.index)

            models_path = [
                "data/models/pendapatan/prophet_pendapatan_IGD.pkl",
                "data/models/pendapatan/prophet_pendapatan_OTC.pkl",
                "data/models/pendapatan/prophet_pendapatan_Rawat Jalan.pkl",
                "data/models/pendapatan/prophet_pendapatan_Rawat Inap.pkl",
            ]
            models = []
            res = []

            for i in models_path:
                if os.path.isfile(i):
                    with open(i, "rb") as file:
                        models.append(pickle.load(file))

            for model in models:
                future = model.make_future_dataframe(periods=30)
                fcst_prophet_train = model.predict(future)

                forecasted_df = fcst_prophet_train[fcst_prophet_train['ds']>=forecast_start_date]
                forecasted_df = forecasted_df[['ds', 'yhat']]

                res.append({
                    "index": forecasted_df.ds.dt.strftime("%Y-%m-%d").tolist(),
                    "columns": ["Prediksi Jumlah Pendapatan"],
                    "values": [forecasted_df.yhat.tolist()]
                })
            return res

    elif tipe_data == "pendapatanPenjamin":
        temp_df = temp_df[["jenis_penjamin", "total_tagihan"]]
        temp_df = temp_df.set_index("jenis_penjamin")
        temp_df = temp_df.groupby(["jenis_penjamin"]).sum()

        res["index"] = temp_df.index.values.tolist()
        res["values"] = temp_df["total_tagihan"].values.tolist()
        return res

    elif tipe_data == "pengeluaranPenjamin":
        temp_df = temp_df[["jenis_penjamin", "total_semua_hpp"]]
        temp_df = temp_df.set_index("jenis_penjamin")
        temp_df = temp_df.groupby(["jenis_penjamin"]).sum()

        res["index"] = temp_df.index.tolist()
        res["values"] = temp_df["total_semua_hpp"].values.tolist()
        return res

    elif tipe_data == "penjamin":
        temp_df = temp_df[["jenis_penjamin", "total_semua_hpp", "total_tagihan"]]
        temp_df = temp_df.set_index("jenis_penjamin")
        temp_df = temp_df.groupby(["jenis_penjamin"]).sum()

        res["index"] = temp_df.index.tolist()
        res["pengeluaran"] = temp_df["total_semua_hpp"].values.tolist()
        res["pendapatan"] = temp_df["total_tagihan"].values.tolist()

        return res

    elif tipe_data == "pendapatanLastDay":
        temp_df = temp_df[["waktu_registrasi", "total_tagihan", "total_semua_hpp"]]
        temp_df = filter_last(temp_df, "waktu_registrasi", from_last_data=True, days = 1)
        res["pendapatanLastDay"] = float(temp_df["total_tagihan"].sum())
        res["pengeluaranLastDay"] = float(temp_df["total_semua_hpp"].sum())
        return res

    elif tipe_data == "totalPendapatan":
        if tahun is None :
            temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)
        res["value"] = temp_df["total_tagihan"].sum()
        return res

    elif tipe_data == "totalPengeluaran":
        if tahun is None :
            temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)
        res["value"] = temp_df["total_semua_hpp"].sum()
        return res

    elif tipe_data == "total":
        res["pendapatan"] = temp_df["total_tagihan"].sum()
        res["pengeluaran"] = temp_df["total_semua_hpp"].sum()
        return res

    elif tipe_data == "pendapatanGejala":
        grouped_data_sorted = grouped_data.sort_values(by='pendapatan', ascending=False)

        res["index"] = grouped_data_sorted["diagnosa_primer"].tolist()
        res["values"] = grouped_data_sorted["pendapatan"].tolist()
        return res

    elif tipe_data == "pengeluaranGejala":
        grouped_data_sorted = grouped_data.sort_values(by='pengeluaran', ascending=False)

        res["index"] = grouped_data_sorted["diagnosa_primer"].tolist()
        res["values"] = grouped_data_sorted["pengeluaran"].tolist()
        return res

    # modified by chintya
    elif tipe_data == 'diagnosa' :
        if diagnosa is not None :
            grouped_data = temp_df.groupby(['diagnosa_primer','jenis_penjamin']).agg(
                pendapatan=('total_tagihan', 'sum'),
                pengeluaran=('total_semua_hpp', 'sum')
            ).reset_index()

            res['index'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['diagnosa_primer'].unique().tolist()
            res['jenis_penjamin'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['jenis_penjamin'].tolist()
            res['pendapatan'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['pendapatan'].tolist()
            res['pengeluaran'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['pengeluaran'].tolist()

            if jenis_registrasi == 'Rawat Inap':
                grouped_data = temp_df.groupby(['diagnosa_primer','kelas_hak']).agg(
                    pendapatan=('total_tagihan', 'sum'),
                    pengeluaran=('total_semua_hpp', 'sum')
                ).reset_index()

                res['kelas_hak'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['kelas_hak'].tolist()
                res['pendapatanKelas'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['pendapatan'].tolist()
                res['pengeluaranKelas'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['pengeluaran'].tolist()
                res['avgLosRawatan'] = round(temp_df[temp_df['diagnosa_primer']==diagnosa]['los_rawatan'].mean())

            return res

        grouped_data_sorted = grouped_data.sort_values(by=sort_arg, ascending=False)

        res["index"] = grouped_data_sorted["diagnosa_primer"].tolist()
        res["pendapatan"] = grouped_data_sorted["pendapatan"].tolist()
        res["pengeluaran"] = grouped_data_sorted["pengeluaran"].tolist()
        return res

    # modified by chintya
    elif tipe_data == 'poliklinik':
        if poli is not None :
            grouped_dataPoliDiagnosa = temp_df.groupby(['nama_departemen','diagnosa_primer']).agg(
                pendapatan=('total_tagihan', 'sum'),
                pengeluaran=('total_semua_hpp', 'sum')
            ).reset_index()

            grouped_dataPoliDiagnosa = grouped_dataPoliDiagnosa[grouped_dataPoliDiagnosa['nama_departemen']==poli]
            grouped_dataPoli_sorted = grouped_dataPoliDiagnosa.sort_values(by=sort_arg, ascending=False)

            res["index"] = grouped_dataPoli_sorted["nama_departemen"].unique().tolist()
            res["diagnosa"] = grouped_dataPoli_sorted["diagnosa_primer"].tolist()
            res["pendapatanDiagnosa"] = grouped_dataPoli_sorted["pendapatan"].tolist()
            res["pengeluaranDiagnosa"] = grouped_dataPoli_sorted["pengeluaran"].tolist()

            grouped_dataPoliPenjamin = temp_df.groupby(['nama_departemen','jenis_penjamin']).agg(
                pendapatan=('total_tagihan', 'sum'),
                pengeluaran=('total_semua_hpp', 'sum')
            ).reset_index()
            grouped_dataPoliPenjamin = grouped_dataPoliPenjamin[grouped_dataPoliPenjamin['nama_departemen']==poli]

            res["jenis_penjamin"] = grouped_dataPoliPenjamin["jenis_penjamin"].tolist()
            res["pendapatan"] = grouped_dataPoliPenjamin["pendapatan"].tolist()
            res["pengeluaran"] = grouped_dataPoliPenjamin["pengeluaran"].tolist()

        else :
            grouped_dataPoli_sorted = grouped_dataPoli.sort_values(by=sort_arg, ascending=False)

            res["index"] = grouped_dataPoli_sorted["nama_departemen"].tolist()
            res["pendapatan"] = grouped_dataPoli_sorted["pendapatan"].tolist()
            res["pengeluaran"] = grouped_dataPoli_sorted["pengeluaran"].tolist()

        return res

    elif tipe_data == "poliklinikSortByPendapatan":
        grouped_dataPoli_sorted = grouped_dataPoli.sort_values(by='pendapatan', ascending=False).iloc[:10]

        res["index"] = grouped_dataPoli_sorted["nama_departemen"].tolist()
        res["pendapatan"] = grouped_dataPoli_sorted["pendapatan"].tolist()
        res["pengeluaran"] = grouped_dataPoli_sorted["pengeluaran"].tolist()
        return data

    elif tipe_data == "poliklinikSortByPengeluaran":
        grouped_dataPoli_sorted = grouped_dataPoli.sort_values(by='pengeluaran', ascending=False).iloc[:10]

        res["index"] = grouped_dataPoli_sorted["nama_departemen"].tolist()
        res["pendapatan"] = grouped_dataPoli_sorted["pendapatan"].tolist()
        res["pengeluaran"] = grouped_dataPoli_sorted["pengeluaran"].tolist()
        return res

    elif tipe_data == "profit":
        if tahun is None :
             temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)

        pendapatan = temp_df['total_tagihan'].sum()
        pengeluaran = temp_df['total_semua_hpp'].sum()
        res["value"] = pendapatan - pengeluaran
        return res

    else:
        if not forecast:
            if tahun is None:
                temp_df = filter_last(temp_df, "waktu_registrasi", from_last_data = "True", months = 6)

            if tahun is None and bulan is None:
                resample_option = "D"

            temp_df = temp_df[["waktu_registrasi", "total_tagihan", "total_semua_hpp"]]
            temp_df = temp_df.set_index("waktu_registrasi")
            temp_df = temp_df.resample(resample_option).sum()
            res["index"] = temp_df.index.strftime(f"%Y-%m{'-%d' if resample_option == 'D' else '' }").tolist()
            res["columns"] = temp_df.columns.tolist()
            res["values"] = temp_df.values.transpose().tolist()
            return res

        else:
             # Preprocess
            d.dataset['waktu_registrasi'] = pd.to_datetime(d.dataset['waktu_registrasi'], format= "%Y/%m/%d")
            df = d.dataset[['waktu_registrasi', 'jenis_registrasi', 'id_registrasi']]
            agg_df = df.groupby(['waktu_registrasi','jenis_registrasi']).agg({'id_registrasi':'count'}).reset_index().sort_values(['jenis_registrasi','waktu_registrasi'])
            total_sales_df = agg_df.pivot(index='waktu_registrasi',columns='jenis_registrasi', values='id_registrasi')

            # Modelling
            prediction_days = request.args.get("days", type=int)
            forecast_start_date = max(total_sales_df.index)

            models_path = [
                "data/models/pendapatan/prophet_pendapatan_Pendapatan.pkl",
                "data/models/pendapatan/prophet_pendapatan_Pengeluaran.pkl",
            ]
            column_names = [
                "total_tagihan",
                "total_semua_hpp"
            ]
            models = []
            res = []

            for i in models_path:
                if os.path.isfile(i):
                    with open(i, "rb") as file:
                        models.append(pickle.load(file))

            for i, model in enumerate(models):
                future = model.make_future_dataframe(periods=30)
                fcst_prophet_train = model.predict(future)

                forecasted_df = fcst_prophet_train[fcst_prophet_train['ds']>=forecast_start_date]
                forecasted_df = forecasted_df[['ds', 'yhat']]

                res.append({
                    "index": forecasted_df.ds.dt.strftime("%Y-%m-%d").tolist(),
                    "columns": [column_names[i]],
                    "values": [forecasted_df.yhat.tolist()]
                })
            return res

