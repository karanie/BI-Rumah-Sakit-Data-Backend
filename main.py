import json
import os.path, time
import pickle
from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.utils import secure_filename
import pandas as pd
from tools import read_dataset_pickle, save_dataset_as_pickle, read_dataset
from preprocess import preprocess_dataset
from filterdf import filter_in_year, filter_in_year_month,filter_last,resample_opt,default_filter
from darts.timeseries import TimeSeries
from darts.models import ExponentialSmoothing

ALLOWED_EXTENSIONS = { "csv", "xlsx" }
UPLOAD_FOLDER = "dataset/"

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
CORS(app)

dc1 = read_dataset_pickle(["dataset/DC1"])[0]
dc1 = preprocess_dataset(dc1)

@app.route("/api/pendapatan", methods=["GET"])
def data_pendapatan():
    data = {}
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

    temp_df = dc1

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


            data["index"] = temp_df.index.strftime(f"%Y-%m{'-%d' if resample_option == 'D' else '' }").tolist()
            data["columns"] = temp_df.columns.tolist()
            data["values"] = temp_df.values.transpose().tolist()
            return data

        else:
            # Preprocess
            dc1['waktu_registrasi'] = pd.to_datetime(dc1['waktu_registrasi'], format= "%Y/%m/%d")
            df = dc1[['waktu_registrasi', 'jenis_registrasi', 'id_registrasi']]
            agg_df = df.groupby(['waktu_registrasi','jenis_registrasi']).agg({'id_registrasi':'count'}).reset_index().sort_values(['jenis_registrasi','waktu_registrasi'])
            total_sales_df = agg_df.pivot(index='waktu_registrasi',columns='jenis_registrasi', values='id_registrasi')

            # Modelling
            prediction_days = request.args.get("days", type=int)
            forecast_start_date = max(total_sales_df.index)

            models_path = [
                "models/pendapatan/prophet_pendapatan_model_IGD.pkl",
                "models/pendapatan/prophet_pendapatan_model_OTC.pkl",
                "models/pendapatan/prophet_pendapatan_model_Rawat Jalan.pkl",
                "models/pendapatan/prophet_pendapatan_model_Rawat Inap.pkl",
            ]
            models = []
            data = []

            for i in models_path:
                if os.path.isfile(i):
                    with open(i, "rb") as file:
                        models.append(pickle.load(file))

            for model in models:
                future = model.make_future_dataframe(periods=30)
                fcst_prophet_train = model.predict(future)

                forecasted_df = fcst_prophet_train[fcst_prophet_train['ds']>=forecast_start_date]
                forecasted_df = forecasted_df[['ds', 'yhat']]

                data.append({
                    "index": forecasted_df.ds.dt.strftime("%Y-%m-%d").tolist(),
                    "columns": ["Prediksi Jumlah Pendapatan"],
                    "values": [forecasted_df.yhat.tolist()]
                })
            return data

    elif tipe_data == "pendapatanPenjamin":
        temp_df = temp_df[["jenis_penjamin", "total_tagihan"]]
        temp_df = temp_df.set_index("jenis_penjamin")
        temp_df = temp_df.groupby(["jenis_penjamin"]).sum()

        data["index"] = temp_df.index.values.tolist()
        data["values"] = temp_df["total_tagihan"].values.tolist()
        return data

    elif tipe_data == "pengeluaranPenjamin":
        temp_df = temp_df[["jenis_penjamin", "total_semua_hpp"]]
        temp_df = temp_df.set_index("jenis_penjamin")
        temp_df = temp_df.groupby(["jenis_penjamin"]).sum()

        data["index"] = temp_df.index.tolist()
        data["values"] = temp_df["total_semua_hpp"].values.tolist()
        return data

    elif tipe_data == "penjamin":
        temp_df = temp_df[["jenis_penjamin", "total_semua_hpp", "total_tagihan"]]
        temp_df = temp_df.set_index("jenis_penjamin")
        temp_df = temp_df.groupby(["jenis_penjamin"]).sum()

        data["index"] = temp_df.index.tolist()
        data["pengeluaran"] = temp_df["total_semua_hpp"].values.tolist()
        data["pendapatan"] = temp_df["total_tagihan"].values.tolist()

        return data

    elif tipe_data == "pendapatanLastDay":
        temp_df = temp_df[["waktu_registrasi", "total_tagihan", "total_semua_hpp"]]
        temp_df = filter_last(temp_df, "waktu_registrasi", from_last_data=True, days = 1)
        data["pendapatanLastDay"] = float(temp_df["total_tagihan"].sum())
        data["pengeluaranLastDay"] = float(temp_df["total_semua_hpp"].sum())
        return data

    elif tipe_data == "totalPendapatanBulanIni":
        if tahun is None :
            temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)
        data["value"] = temp_df["total_tagihan"].sum()
        return data

    elif tipe_data == "totalPengeluaranBulanIni":
        if tahun is None :
            temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)
        data["value"] = temp_df["total_semua_hpp"].sum()
        return data

    elif tipe_data == "total":
        data["pendapatan"] = temp_df["total_tagihan"].sum()
        data["pengeluaran"] = temp_df["total_semua_hpp"].sum()
        return data

    elif tipe_data == "pendapatanGejala":
        grouped_data_sorted = grouped_data.sort_values(by='pendapatan', ascending=False)

        data["index"] = grouped_data_sorted["diagnosa_primer"].tolist()
        data["values"] = grouped_data_sorted["pendapatan"].tolist()
        return data

    elif tipe_data == "pengeluaranGejala":
        grouped_data_sorted = grouped_data.sort_values(by='pengeluaran', ascending=False)

        data["index"] = grouped_data_sorted["diagnosa_primer"].tolist()
        data["values"] = grouped_data_sorted["pengeluaran"].tolist()
        return data

    # modified by chintya
    elif tipe_data == 'diagnosa' :
        if diagnosa is not None :
            grouped_data = temp_df.groupby(['diagnosa_primer','jenis_penjamin']).agg(
                pendapatan=('total_tagihan', 'sum'),
                pengeluaran=('total_semua_hpp', 'sum')
            ).reset_index()

            data['index'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['diagnosa_primer'].unique().tolist()
            data['jenis_penjamin'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['jenis_penjamin'].tolist()
            data['pendapatan'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['pendapatan'].tolist()
            data['pengeluaran'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['pengeluaran'].tolist()

            if jenis_registrasi == 'Rawat Inap':
                grouped_data = temp_df.groupby(['diagnosa_primer','kelas_hak']).agg(
                    pendapatan=('total_tagihan', 'sum'),
                    pengeluaran=('total_semua_hpp', 'sum')
                ).reset_index()

                data['kelas_hak'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['kelas_hak'].tolist()
                data['pendapatanKelas'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['pendapatan'].tolist()
                data['pengeluaranKelas'] = grouped_data[grouped_data['diagnosa_primer']==diagnosa]['pengeluaran'].tolist()
                data['avgLosRawatan'] = round(temp_df[temp_df['diagnosa_primer']==diagnosa]['los_rawatan'].mean())

            return data

        grouped_data_sorted = grouped_data.sort_values(by=sort_arg, ascending=False)

        data["index"] = grouped_data_sorted["diagnosa_primer"].tolist()
        data["pendapatan"] = grouped_data_sorted["pendapatan"].tolist()
        data["pengeluaran"] = grouped_data_sorted["pengeluaran"].tolist()
        return data

    # modified by chintya
    elif tipe_data == 'poliklinik':
        if poli is not None :
            grouped_dataPoliDiagnosa = temp_df.groupby(['nama_departemen','diagnosa_primer']).agg(
                pendapatan=('total_tagihan', 'sum'),
                pengeluaran=('total_semua_hpp', 'sum')
            ).reset_index()

            grouped_dataPoliDiagnosa = grouped_dataPoliDiagnosa[grouped_dataPoliDiagnosa['nama_departemen']==poli]
            grouped_dataPoli_sorted = grouped_dataPoliDiagnosa.sort_values(by=sort_arg, ascending=False)

            data["index"] = grouped_dataPoli_sorted["nama_departemen"].unique().tolist()
            data["diagnosa"] = grouped_dataPoli_sorted["diagnosa_primer"].tolist()
            data["pendapatanDiagnosa"] = grouped_dataPoli_sorted["pendapatan"].tolist()
            data["pengeluaranDiagnosa"] = grouped_dataPoli_sorted["pengeluaran"].tolist()

            grouped_dataPoliPenjamin = temp_df.groupby(['nama_departemen','jenis_penjamin']).agg(
                pendapatan=('total_tagihan', 'sum'),
                pengeluaran=('total_semua_hpp', 'sum')
            ).reset_index()
            grouped_dataPoliPenjamin = grouped_dataPoliPenjamin[grouped_dataPoliPenjamin['nama_departemen']==poli]

            data["jenis_penjamin"] = grouped_dataPoliPenjamin["jenis_penjamin"].tolist()
            data["pendapatan"] = grouped_dataPoliPenjamin["pendapatan"].tolist()
            data["pengeluaran"] = grouped_dataPoliPenjamin["pengeluaran"].tolist()

        else :
            grouped_dataPoli_sorted = grouped_dataPoli.sort_values(by=sort_arg, ascending=False)

            data["index"] = grouped_dataPoli_sorted["nama_departemen"].tolist()
            data["pendapatan"] = grouped_dataPoli_sorted["pendapatan"].tolist()
            data["pengeluaran"] = grouped_dataPoli_sorted["pengeluaran"].tolist()

        return data

    elif tipe_data == "poliklinikSortByPendapatan":
        grouped_dataPoli_sorted = grouped_dataPoli.sort_values(by='pendapatan', ascending=False).iloc[:10]

        data["index"] = grouped_dataPoli_sorted["nama_departemen"].tolist()
        data["pendapatan"] = grouped_dataPoli_sorted["pendapatan"].tolist()
        data["pengeluaran"] = grouped_dataPoli_sorted["pengeluaran"].tolist()
        return data

    elif tipe_data == "poliklinikSortByPengeluaran":
        grouped_dataPoli_sorted = grouped_dataPoli.sort_values(by='pengeluaran', ascending=False).iloc[:10]

        data["index"] = grouped_dataPoli_sorted["nama_departemen"].tolist()
        data["pendapatan"] = grouped_dataPoli_sorted["pendapatan"].tolist()
        data["pengeluaran"] = grouped_dataPoli_sorted["pengeluaran"].tolist()
        return data

    elif tipe_data == "profit":
        if tahun is None :
             temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)

        pendapatan = temp_df['total_tagihan'].sum()
        pengeluaran = temp_df['total_semua_hpp'].sum()
        data["value"] = pendapatan - pengeluaran
        return data

    else:
        if not forecast:
            if tahun is None:
                temp_df = filter_last(temp_df, "waktu_registrasi", from_last_data = "True", months = 6)

            if tahun is None and bulan is None:
                resample_option = "D"

            temp_df = temp_df[["waktu_registrasi", "total_tagihan", "total_semua_hpp"]]
            temp_df = temp_df.set_index("waktu_registrasi")
            temp_df = temp_df.resample(resample_option).sum()
            data["index"] = temp_df.index.strftime(f"%Y-%m{'-%d' if resample_option == 'D' else '' }").tolist()
            data["columns"] = temp_df.columns.tolist()
            data["values"] = temp_df.values.transpose().tolist()
            return data

        else:
            temp_df = temp_df[["waktu_registrasi", "total_tagihan", "total_semua_hpp"]]
            temp_df = temp_df.set_index("waktu_registrasi")
            temp_df = temp_df.resample("D").sum()

            data = []
            for i in temp_df.columns:
                ts = TimeSeries.from_dataframe(temp_df[[i]])
                train, val = ts[:-30], ts[-30:]

                model = ExponentialSmoothing()
                model.fit(train)

                forecast = model.predict(60)
                data.append({
                    "index": forecast.time_index.strftime("%Y-%m-%d").tolist(),
                    "columns": forecast.columns.tolist(),
                    "values": forecast.values().transpose().tolist(),
                    })
            return data


@app.route("/api/kunjungan", methods=["GET"])
def data_kunjungan():
    data = {}
    tahun = request.args.get("tahun", type=int)
    bulan = request.args.get("bulan", type=int)
    kabupaten = request.args.get("kabupaten", type=str)
    tipe_data = request.args.get("tipe_data")
    forecast = request.args.get("forecast", type=bool)

    # params detailKunjungan
    timeseries = request.args.get("timeseries", type=bool)
    diagnosa = request.args.get("diagnosa", type=str)
    jenis_registrasi = request.args.get("jenisregistrasi", type=str)

    temp_df = dc1

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

        temp_df = temp_df[["waktu_registrasi"]]
        temp_df = temp_df.set_index("waktu_registrasi")
        temp_df["Jumlah Kunjungan"] = 1
        temp_df = temp_df.resample(resample_option).sum()

        data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df.values.transpose().tolist()

        return data

    if tipe_data == "pertumbuhan":
        if tahun is None:
            temp_df = filter_last(temp_df, "waktu_registrasi", from_last_data = "True", months = 6)
        if tahun is None and bulan is None:
            resample_option = "D"

        temp_df = temp_df[["waktu_registrasi"]]
        temp_df = temp_df.set_index("waktu_registrasi")
        temp_df["Jumlah Kunjungan"] = 1
        temp_df = temp_df.resample(resample_option).sum()

        data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df.values.transpose().tolist()

        return data

    elif tipe_data == "pertumbuhanPertahun":

        temp_df = dc1[["waktu_registrasi"]]
        temp_df = temp_df.set_index("waktu_registrasi")
        temp_df["Jumlah Kunjungan"] = 1
        temp_df = temp_df.resample("Y").sum()

        data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df["Jumlah Kunjungan"].transpose().tolist()

        return data

    elif tipe_data == "rujukan":
        temp_df = temp_df[["waktu_registrasi", "rujukan"]]
        temp_df = pd.crosstab(temp_df["waktu_registrasi"], temp_df["rujukan"])
        temp_df = temp_df.resample(resample_option).sum()

        data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df.values.transpose().tolist()
        return data

    elif tipe_data == "usia":
        temp_df = temp_df[["waktu_registrasi", "kategori_usia"]]
        temp_df = pd.crosstab(temp_df["waktu_registrasi"], temp_df["kategori_usia"])
        temp_df = temp_df.resample(resample_option).sum()

        data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df.values.transpose().tolist()
        return data

    elif tipe_data == "jenis_registrasi":
        if not forecast:
            if tahun is None:
                temp_df = filter_last(temp_df, "waktu_registrasi", from_last_data = "True", months = 6)
            if tahun is None and bulan is None:
                resample_option = "D"

            temp_df = temp_df[["waktu_registrasi","jenis_registrasi"]]
            temp_df = pd.crosstab(temp_df["waktu_registrasi"],temp_df["jenis_registrasi"])
            temp_df = temp_df.resample(resample_option).sum()

            data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
            data["columns"] = temp_df.columns.tolist()
            data["values"] = temp_df.values.transpose().tolist()
            return data

        else:
            #Preprocessing
            dc1['waktu_registrasi'] = pd.to_datetime(dc1['waktu_registrasi'], format= "%Y/%m/%d")
            df = dc1[['waktu_registrasi', 'jenis_registrasi', 'id_registrasi']]
            agg_df = df.groupby(['waktu_registrasi','jenis_registrasi']).agg({'id_registrasi':'count'}).reset_index().sort_values(['jenis_registrasi','waktu_registrasi'])
            total_sales_df = agg_df.pivot(index='waktu_registrasi',columns='jenis_registrasi', values='id_registrasi')

            #Modeling
            prediction_days = request.args.get("days", type=int)
            forecast_start_date = max(total_sales_df.index)

            models_path = [
                "models/kunjungan/prophet_kunjungan_model_IGD.pkl",
                "models/kunjungan/prophet_kunjungan_model_OTC.pkl",
                "models/kunjungan/prophet_kunjungan_model_Rawat Jalan.pkl",
                "models/kunjungan/prophet_kunjungan_model_Rawat Inap.pkl",
            ]
            models = []
            data = []

            for i in models_path:
                if os.path.isfile(i):
                    with open(i, "rb") as file:
                        models.append(pickle.load(file))

            for model in models:
                future = model.make_future_dataframe(periods=30)
                fcst_prophet_train = model.predict(future)

                # fig1 = model.plot(fcst_prophet_train)
                # fig2 = model.plot_components(fcst_prophet_train)

                forecasted_df = fcst_prophet_train[fcst_prophet_train['ds']>=forecast_start_date]

                # forecasted_dfs.append(forecasted_df)

                forecasted_df = forecasted_df[['ds', 'yhat']]

                data.append({
                    "index": forecasted_df.ds.dt.strftime("%Y-%m-%d").tolist(),
                    "columns": ["Prediksi Jumlah Kunjungan"],
                    "values": [forecasted_df.yhat.tolist()]
                })
            return data

    elif tipe_data == "jumlahJenis_registrasi":
        temp_df = temp_df[["jenis_registrasi"]]
        temp_df["count"] = 1
        temp_df = temp_df.set_index("jenis_registrasi")
        temp_df = temp_df.groupby(["jenis_registrasi"]).sum()

        data["index"] = temp_df.index.values.tolist()
        data["values"] = temp_df["count"].tolist()
        return data

    elif tipe_data == "poliklinik":
        filtered_data = temp_df[temp_df["jenis_registrasi"] == "Rawat Jalan"]
        department_counts = filtered_data['nama_departemen'].value_counts()
        sorted_departments = department_counts.sort_values(ascending=False).iloc[:10]

        data["index"] = sorted_departments.index.values.tolist()
        data["values"] = sorted_departments.values.tolist()
        return data

    elif tipe_data == "gejala":
        top_10_penyakit = temp_df["diagnosa_primer"].value_counts().iloc[:10]

        data["index"] = top_10_penyakit.index.values.tolist()
        data["values"] = top_10_penyakit.values.tolist()
        return data

    elif tipe_data == "jumlahKunjungan":
        if tahun is None :
             temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)
        data["value"] = temp_df['no_registrasi'].nunique()
        return data

    elif tipe_data == "penjamin":
        temp_df = temp_df[["jenis_penjamin"]]
        temp_df["count"] = 1
        temp_df = temp_df.set_index("jenis_penjamin")
        temp_df = temp_df.groupby(["jenis_penjamin"]).sum()

        data["index"] = temp_df.index.values.tolist()
        data["values"] = temp_df["count"].values.tolist()
        return data

    elif tipe_data == "regis-byRujukan":
        df_regis_rujuk = temp_df.groupby(['jenis_registrasi','rujukan']).size().reset_index(name='kunjungan')
        grouped = df_regis_rujuk.groupby('jenis_registrasi')
        data = {regis: group.drop(columns='jenis_registrasi').to_dict(orient='records') for regis, group in grouped}
        return data

    elif tipe_data == "diagnosa":
        if jenis_registrasi is not None:
            temp_df = temp_df[temp_df["jenis_registrasi"] == jenis_registrasi]

            if diagnosa is None:
                data["index"] = temp_df["diagnosa_primer"].value_counts().index.values.tolist()
                data["values"] = temp_df["diagnosa_primer"].value_counts().values.tolist()
                return data

            if diagnosa is not None:
                temp_df = temp_df[temp_df["diagnosa_primer"] == diagnosa]

                if timeseries:
                    if tahun is None and bulan is None:
                        resample_option = "YE"
                    elif tahun is not None and bulan is None:
                        resample_option = "M"
                    else:
                        resample_option = "D"

                else:
                    data["index"] = temp_df["diagnosa_primer"].value_counts().index.values.tolist()
                    data["values"] = temp_df["diagnosa_primer"].value_counts().values.tolist()
                    return data

            df_diagnosa = temp_df[temp_df["diagnosa_primer"] == diagnosa]
            temp_df = pd.crosstab(df_diagnosa["waktu_registrasi"], df_diagnosa["diagnosa_primer"])
            temp_df = temp_df.resample(resample_option).sum()

            # Hitung kategori usia yang dominan untuk setiap waktu registrasi
            if resample_option == "YE":
                dominan_usia_kesimpulan = df_diagnosa.groupby(df_diagnosa["waktu_registrasi"].dt.year)["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None).mode()[0]
            elif resample_option == "M":
                dominan_usia_kesimpulan = df_diagnosa.groupby([df_diagnosa["waktu_registrasi"].dt.year, df_diagnosa["waktu_registrasi"].dt.month])["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None).mode()[0]
            elif resample_option == "D":
                dominan_usia_kesimpulan = df_diagnosa.groupby([df_diagnosa["waktu_registrasi"].dt.year, df_diagnosa["waktu_registrasi"].dt.month, df_diagnosa["waktu_registrasi"].dt.date])["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None).mode()[0]

            dominan_usia = df_diagnosa.groupby(pd.Grouper(key="waktu_registrasi", freq=resample_option))["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None)

            data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
            data["columns"] = temp_df.columns.tolist()
            # data["values"] = temp_df.values.transpose().tolist()
            data["values"] = temp_df.iloc[:, 0].tolist()


            data["dominant_age_category"] = dominan_usia.reindex(temp_df.index).tolist()
            data["dominant_age_category_summary"] = dominan_usia_kesimpulan

            return data

        else:
            return data


@app.route("/api/pasien", methods=["GET"])
def data_pasien():
    data = {}
    tipe_data = request.args.get("tipe_data")
    bulan = request.args.get("bulan", type=int)
    tahun = request.args.get("tahun", type=int)
    kabupaten = request.args.get("kabupaten", type=str)

    temp_df = dc1
    temp_df = default_filter(temp_df, kabupaten, tahun, bulan)

    if tipe_data == "jumlahJenisKelamin":
        # FIlter Pasien Unique
        temp_df = temp_df.loc[temp_df.groupby(['id_pasien', 'jenis_kelamin']).head(1).index, ['id_pasien', 'jenis_kelamin',"waktu_registrasi"]]
        data["index"] = temp_df["jenis_kelamin"].value_counts().sort_index().index.values.tolist()
        data["values"] = temp_df["jenis_kelamin"].value_counts().sort_index().values.tolist()
        return data

    elif tipe_data == "timeseriesJenisKelamin":
        # FIlter Pasien Unique
        temp_df = temp_df.loc[temp_df.groupby(['id_pasien', 'jenis_kelamin']).head(1).index, ['id_pasien', 'jenis_kelamin',"waktu_registrasi"]]

        resample_option = resample_opt(tahun,bulan)

        temp_df = pd.crosstab(temp_df["waktu_registrasi"], temp_df["jenis_kelamin"])
        temp_df = temp_df.resample(resample_option).sum()

        data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df.values.transpose().tolist()
        return data

    elif tipe_data == "pekerjaan":
        data["index"] = temp_df["pekerjaan"].value_counts().iloc[-10:].index.values.tolist()
        data["values"] = temp_df["pekerjaan"].value_counts().iloc[-10:].values.tolist()
        return data

    elif tipe_data == "jumlahPasien":
        if tahun is None :
             temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)
        data["value"] = temp_df['id_pasien'].nunique()
        return data

    elif tipe_data == "jumlahPasienPertahun":
        df_paling_awal = temp_df.loc[temp_df.groupby('id_pasien')['waktu_registrasi'].idxmin()]

        # Ekstrak tahun dari kolom waktu registrasi
        df_paling_awal['tahun'] = df_paling_awal['waktu_registrasi'].dt.year

        # Hitung jumlah pasien unik untuk setiap tahun
        jumlah_pasien_tahunan = df_paling_awal.groupby('tahun')['id_pasien'].nunique()

        data["index"] = jumlah_pasien_tahunan.index.values.tolist()
        data["values"] = jumlah_pasien_tahunan.values.tolist()

        return data

    elif tipe_data == "pasienLamaBaru":
        df_pasien_baru = temp_df[temp_df["fix_pasien_baru"] == "t"][["waktu_registrasi", "id_pasien"]]
        df_pasien_lama = temp_df[temp_df["fix_pasien_baru"] == "f"][["waktu_registrasi", "id_pasien"]]

        data["jumlahPasienBaru"] = int(df_pasien_baru['id_pasien'].count())
        data["jumlahPasienLama"] = int(df_pasien_lama['id_pasien'].count())

        return data

    elif tipe_data == "usia":
        temp_df = temp_df.loc[temp_df.groupby(['id_pasien', 'kategori_usia']).head(1).index, ['id_pasien', 'kategori_usia',"waktu_registrasi"]]
        data["index"] = temp_df["kategori_usia"].value_counts().sort_index().index.values.tolist()
        data["values"] = temp_df["kategori_usia"].value_counts().sort_index().values.tolist()
        return data

@app.route("/api/demografi", methods=["GET"])
def data_demografi():
    data = {}
    tipe_data = request.args.get("tipe_data")
    tahun = request.args.get("tahun", type=int)
    bulan = request.args.get("bulan", type=int)

    temp_df = dc1

    temp_df = temp_df.loc[temp_df["provinsi"] == "RIAU"]

    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df, "waktu_registrasi", tahun, bulan)
    if tahun is not None:
        temp_df = filter_in_year(temp_df, "waktu_registrasi", tahun)

    if tipe_data is None:
        data["index"] = temp_df["kabupaten"].value_counts().index.values.tolist()
        data["values"] = temp_df["kabupaten"].value_counts().values.tolist()
        return data
    elif tipe_data == "timeseries":
        if tahun is None and bulan is None:
            temp_df = temp_df[["waktu_registrasi", "kabupaten"]]
            temp_df = filter_in_year(temp_df, "waktu_registrasi", 2021)
        else:
            temp_df = temp_df[["waktu_registrasi", "kabupaten"]]

        temp_df = temp_df[["kabupaten", "waktu_registrasi"]]
        temp_df = pd.crosstab(temp_df["waktu_registrasi"], temp_df["kabupaten"])
        resample_option = "D"
        temp_df = temp_df.resample(resample_option).sum()

        data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df.values.transpose().tolist()
        return data
    elif tipe_data == "forecast":
        if tahun is None and bulan is None:
            temp_df = dc1[["waktu_registrasi", "kabupaten"]]
            temp_df = filter_in_year(temp_df, "waktu_registrasi", 2021)
        else:
            temp_df = temp_df[["waktu_registrasi", "kabupaten"]]

        temp_df = temp_df[["kabupaten", "waktu_registrasi"]]
        temp_df = pd.crosstab(temp_df["waktu_registrasi"], temp_df["kabupaten"])
        resample_option = "D"
        temp_df = temp_df.resample(resample_option).sum()

        data = []
        for i in temp_df.columns:
            ts = TimeSeries.from_dataframe(temp_df[[i]])
            train, val = ts[:-30], ts[-30:]

            model = ExponentialSmoothing()
            model.fit(train)

            forecast = model.predict(90)
            data.append({
                "index": forecast.time_index.strftime("%Y-%m-%d").tolist(),
                "columns": forecast.columns.tolist(),
                "values": forecast.values().transpose().tolist(),
                })
        return data

    return data

@app.route("/api/last-update", methods=["GET"])
def last_update():
    data = {}
    data["mtime"] = os.path.getmtime("dataset/DC1.pkl.gz")
    data["mtimeLocaltime"] = time.ctime(os.path.getmtime("dataset/DC1.pkl.gz"))
    data["waktuRegistrasiTerakhir"] = dc1.iloc[-1]["waktu_registrasi"]
    return data

@app.route("/api/filter-options", methods=["GET"])
def data_filter_options():
    data = {}
    data["kabupaten"] = sorted(dc1["kabupaten"].unique().tolist())
    data["tahun"] = sorted(dc1["waktu_registrasi"].dt.year.unique().tolist())
    data["bulan"] = sorted(dc1["waktu_registrasi"].dt.month.unique().tolist())
    return data

@app.route("/api/update-dataset", methods=["POST"])
def update_dataset():
    global dc1
    method = request.args.get("method", type=str)

    if method == "concatdf":
        if "dataset" not in request.files:
            return "No dataset file"
        dataset_file = request.files["dataset"]
        if dataset_file.filename == "":
            return "No dataset file"
        if dataset_file and dataset_file.filename.rsplit('.', 1)[1].lower() not in ALLOWED_EXTENSIONS:
            return "File extension not supported"

        filename = secure_filename(dataset_file.filename)
        dataset_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        dataset_file.save(dataset_file_path)

        preprocess_time_start = time.time()
        df_new = preprocess_dataset(read_dataset(dataset_file_path))
        preprocess_time_end = time.time()

        concat_time_start = time.time()
        #df = pd.concat([dc1, df_new]).reset_index().drop(columns=["index"])
        df = pd.concat([dc1, df_new], ignore_index=True)
        concat_time_end = time.time()

        save_time_start = time.time()
        # Compressing the pickle file with gzip takes around ~85 seconds on
        # my computer. This may be a concern if you want faster dataset update
        save_dataset_as_pickle(df, "dataset/DC1")
        save_time_end = time.time()

        dc1 = df
        return {
                "status": "Updated",
                "preprocess_time": preprocess_time_end - preprocess_time_start,
                "concat_time": concat_time_end - concat_time_start,
                "save_time": save_time_end - save_time_start,
        }

    return {
            "status": "Unsupported method"
    }

@app.route("/api/diagnosa", methods=["GET"])
def data_diagnosa():
    data = {}
    tipe_data = request.args.get("tipe_data")
    bulan = request.args.get("bulan", type=int)
    tahun = request.args.get("tahun", type=int)
    jenis_registrasi = request.args.get("jenisregistrasi", type=str)
    diagnosa = request.args.get("diagnosa", type=str)
    kabupaten = request.args.get("kabupaten", type=str)
    timeseries = request.args.get("timeseries", type=bool)

    temp_df = dc1

    temp_df = default_filter(temp_df, kabupaten, tahun, bulan)

    if jenis_registrasi is not None:
        temp_df = temp_df[temp_df["jenis_registrasi"] == jenis_registrasi]

    if diagnosa is None:
        data["index"] = temp_df["diagnosa_primer"].value_counts().index.values.tolist()
        data["values"] = temp_df["diagnosa_primer"].value_counts().values.tolist()
        return data

    if diagnosa is not None:
        temp_df = temp_df[temp_df["diagnosa_primer"] == diagnosa]

        if timeseries:
            if tahun is None and bulan is None:
                resample_option = "YE"
            elif tahun is not None and bulan is None:
                resample_option = "M"
            else:
                resample_option = "D"

        else:
            data["index"] = temp_df["diagnosa_primer"].value_counts().index.values.tolist()
            data["values"] = temp_df["diagnosa_primer"].value_counts().values.tolist()
            return data

    df_diagnosa = temp_df[temp_df["diagnosa_primer"] == diagnosa]
    temp_df = pd.crosstab(df_diagnosa["waktu_registrasi"], df_diagnosa["diagnosa_primer"])
    temp_df = temp_df.resample(resample_option).sum()

    # Hitung kategori usia yang dominan untuk setiap waktu registrasi
    if resample_option == "YE":
        dominan_usia_kesimpulan = df_diagnosa.groupby(df_diagnosa["waktu_registrasi"].dt.year)["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None).mode()[0]
    elif resample_option == "M":
        dominan_usia_kesimpulan = df_diagnosa.groupby([df_diagnosa["waktu_registrasi"].dt.year, df_diagnosa["waktu_registrasi"].dt.month])["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None).mode()[0]
    elif resample_option == "D":
        dominan_usia_kesimpulan = df_diagnosa.groupby([df_diagnosa["waktu_registrasi"].dt.year, df_diagnosa["waktu_registrasi"].dt.month, df_diagnosa["waktu_registrasi"].dt.date])["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None).mode()[0]

    dominan_usia = df_diagnosa.groupby(pd.Grouper(key="waktu_registrasi", freq=resample_option))["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None)

    data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
    data["columns"] = temp_df.columns.tolist()
    # data["values"] = temp_df.values.transpose().tolist()
    data["values"] = temp_df.iloc[:, 0].tolist()


    data["dominant_age_category"] = dominan_usia.reindex(temp_df.index).tolist()
    data["dominant_age_category_summary"] = dominan_usia_kesimpulan

    return data

@app.route("/api/departemen", methods=["GET"])
def data_departemen():
    data = {}
    tipe_data = request.args.get("tipe_data")
    bulan = request.args.get("bulan", type=int)
    tahun = request.args.get("tahun", type=int)
    jenis_registrasi = request.args.get("jenisregistrasi", type=str)
    departemen = request.args.get("departemen", type=str)
    kabupaten = request.args.get("kabupaten", type=str)
    timeseries = request.args.get("timeseries", type=bool)

    temp_df = dc1

    temp_df = default_filter(temp_df, kabupaten, tahun, bulan)

    if jenis_registrasi is not None:
        temp_df = temp_df[temp_df["jenis_registrasi"] == jenis_registrasi]

    if departemen is None:
        data["index"] = temp_df["nama_departemen"].value_counts().index.values.tolist()
        data["values"] = temp_df["nama_departemen"].value_counts().values.tolist()
        return data

    if departemen is not None:
        temp_df = temp_df[temp_df["nama_departemen"] == departemen]

        if timeseries:
            if tahun is None and bulan is None:
                resample_option = "YE"
            elif tahun is not None and bulan is None:
                resample_option = "M"
            else:
                resample_option = "D"

        else:
            data["index"] = temp_df["nama_departemen"].value_counts().index.values.tolist()
            data["values"] = temp_df["nama_departemen"].value_counts().values.tolist()
            data["columns"] = "Jumlah"
            data["indexDiagnosa"] = temp_df["diagnosa_primer"].value_counts().index.values.tolist()
            data["valuesDiagnosa"] = temp_df["diagnosa_primer"].value_counts().values.tolist()
            return data

    df_departemen = temp_df[temp_df["nama_departemen"] == departemen]
    temp_df = pd.crosstab(df_departemen["waktu_registrasi"], df_departemen["nama_departemen"])
    temp_df = temp_df.resample(resample_option).sum()

    # Hitung kategori usia yang dominan untuk setiap waktu registrasi
    if resample_option == "YE":
        dominan_usia_kesimpulan = df_departemen.groupby(df_departemen["waktu_registrasi"].dt.year)["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None).mode()[0]
    elif resample_option == "M":
        dominan_usia_kesimpulan = df_departemen.groupby([df_departemen["waktu_registrasi"].dt.year, df_departemen["waktu_registrasi"].dt.month])["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None).mode()[0]
    elif resample_option == "D":
        dominan_usia_kesimpulan = df_departemen.groupby([df_departemen["waktu_registrasi"].dt.year, df_departemen["waktu_registrasi"].dt.month, df_departemen["waktu_registrasi"].dt.date])["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None).mode()[0]

    dominan_usia = df_departemen.groupby(pd.Grouper(key="waktu_registrasi", freq=resample_option))["kategori_usia"].agg(lambda x: x.mode()[0] if not x.empty else None)


    data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
    data["columns"] = temp_df.columns.tolist()
    # data["values"] = temp_df.values.transpose().tolist()
    data["values"] = temp_df.iloc[:, 0].tolist()

    data["dominant_age_category"] = dominan_usia.reindex(temp_df.index).tolist()
    data["dominant_age_category_summary"] = dominan_usia_kesimpulan

    return data
