import os, pickle
from flask import Blueprint, request
import pandas as pd
from computes.filterdf import filter_in_year, filter_in_year_month,filter_last
from ..data import dataset as d

routes_kunjungan = Blueprint("routes_kunjungan", __name__)
@routes_kunjungan.route("/api/kunjungan", methods=["GET"])
def routes():
    res = {}
    tahun = request.args.get("tahun", type=int)
    bulan = request.args.get("bulan", type=int)
    kabupaten = request.args.get("kabupaten", type=str)
    tipe_data = request.args.get("tipe_data")
    forecast = request.args.get("forecast", type=bool)

    # params detailKunjungan
    timeseries = request.args.get("timeseries", type=bool)
    diagnosa = request.args.get("diagnosa", type=str)
    jenis_registrasi = request.args.get("jenis_registrasi", type=str)
    departemen = request.args.get("departemen", type=str)

    temp_df = d

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


    if tipe_data == "pertumbuhan":
        if tahun is None:
            temp_df = filter_last(temp_df, "waktu_registrasi", from_last_data = "True", months = 6)
        if tahun is None and bulan is None:
            resample_option = "D"

        temp_df = temp_df[["waktu_registrasi"]]
        temp_df = temp_df.set_index("waktu_registrasi")
        temp_df["Jumlah Kunjungan"] = 1
        temp_df = temp_df.resample(resample_option).sum()

        res["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        res["columns"] = temp_df.columns.tolist()
        res["values"] = temp_df.values.transpose().tolist()

        return res

    elif tipe_data == "pertumbuhanPertahun":

        temp_df = d[["waktu_registrasi"]]
        temp_df = temp_df.set_index("waktu_registrasi")
        temp_df["Jumlah Kunjungan"] = 1
        temp_df = temp_df.resample("Y").sum()

        res["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        res["columns"] = temp_df.columns.tolist()
        res["values"] = temp_df["Jumlah Kunjungan"].transpose().tolist()

        return res

    elif tipe_data == "rujukan":
        temp_df = temp_df[["waktu_registrasi", "rujukan"]]
        temp_df = pd.crosstab(temp_df["waktu_registrasi"], temp_df["rujukan"])
        temp_df = temp_df.resample(resample_option).sum()

        res["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        res["columns"] = temp_df.columns.tolist()
        res["values"] = temp_df.values.transpose().tolist()
        return res

    elif tipe_data == "usia":
        temp_df = temp_df[["waktu_registrasi", "kategori_usia"]]
        temp_df = temp_df.groupby([temp_df["waktu_registrasi"]] + ["kategori_usia"]).size().unstack(fill_value=0)
        temp_df = temp_df.resample(resample_option).sum()

        res["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        res["columns"] = temp_df.columns.tolist()
        res["values"] = temp_df.values.transpose().tolist()
        return res

    elif tipe_data == "jenis_registrasi":
        if not forecast:
            if tahun is None:
                temp_df = filter_last(temp_df, "waktu_registrasi", from_last_data = "True", months = 6)
            if tahun is None and bulan is None:
                resample_option = "D"

            temp_df = temp_df[["waktu_registrasi","jenis_registrasi"]]
            temp_df = temp_df.groupby([temp_df["waktu_registrasi"]] + ["jenis_registrasi"]).size().unstack(fill_value=0)
            temp_df = temp_df.resample(resample_option).sum()

            res["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
            res["columns"] = temp_df.columns.tolist()
            res["values"] = temp_df.values.transpose().tolist()
            return res

        else:
            #Preprocessing
            d['waktu_registrasi'] = pd.to_datetime(d['waktu_registrasi'], format= "%Y/%m/%d")
            df = d[['waktu_registrasi', 'jenis_registrasi', 'id_registrasi']]
            agg_df = df.groupby(['waktu_registrasi','jenis_registrasi']).agg({'id_registrasi':'count'}).reset_index().sort_values(['jenis_registrasi','waktu_registrasi'])
            total_sales_df = agg_df.pivot(index='waktu_registrasi',columns='jenis_registrasi', values='id_registrasi')

            #Modeling
            prediction_days = request.args.get("days", type=int)
            forecast_start_date = max(total_sales_df.index)

            models_path = [
                "data/models/kunjungan/prophet_kunjungan_model_IGD.pkl",
                "data/models/kunjungan/prophet_kunjungan_model_OTC.pkl",
                "data/models/kunjungan/prophet_kunjungan_model_Rawat Jalan.pkl",
                "data/models/kunjungan/prophet_kunjungan_model_Rawat Inap.pkl",
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

                # fig1 = model.plot(fcst_prophet_train)
                # fig2 = model.plot_components(fcst_prophet_train)

                forecasted_df = fcst_prophet_train[fcst_prophet_train['ds']>=forecast_start_date]

                # forecasted_dfs.append(forecasted_df)

                forecasted_df = forecasted_df[['ds', 'yhat']]

                res.append({
                    "index": forecasted_df.ds.dt.strftime("%Y-%m-%d").tolist(),
                    "columns": ["Prediksi Jumlah Kunjungan"],
                    "values": [forecasted_df.yhat.tolist()]
                })
            return res

    elif tipe_data == "jumlahJenis_registrasi":
        temp_df = temp_df[["jenis_registrasi"]]
        temp_df["count"] = 1
        temp_df = temp_df.set_index("jenis_registrasi")
        temp_df = temp_df.groupby(["jenis_registrasi"]).sum()

        res["index"] = temp_df.index.values.tolist()
        res["values"] = temp_df["count"].tolist()
        return res

    elif tipe_data == "poliklinik":
        filtered_data = temp_df[temp_df["jenis_registrasi"] == "Rawat Jalan"]
        department_counts = filtered_data['nama_departemen'].value_counts()
        sorted_departments = department_counts.sort_values(ascending=False).iloc[:10]

        res["index"] = sorted_departments.index.values.tolist()
        res["values"] = sorted_departments.values.tolist()
        return res

    elif tipe_data == "gejala":
        top_10_penyakit = temp_df["diagnosa_primer"].value_counts().iloc[:10]

        res["index"] = top_10_penyakit.index.values.tolist()
        res["values"] = top_10_penyakit.values.tolist()
        return res

    elif tipe_data == "jumlahKunjungan":
        if tahun is None :
             temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)
        res["value"] = temp_df['no_registrasi'].nunique()
        return res

    elif tipe_data == "penjamin":
        temp_df = temp_df[["jenis_penjamin"]]
        temp_df["count"] = 1
        temp_df = temp_df.set_index("jenis_penjamin")
        temp_df = temp_df.groupby(["jenis_penjamin"]).sum()

        res["index"] = temp_df.index.values.tolist()
        res["values"] = temp_df["count"].values.tolist()
        return res

    elif tipe_data == "regis-byRujukan":
        df_regis_rujuk = temp_df.groupby(['jenis_registrasi','rujukan']).size().reset_index(name='kunjungan')
        grouped = df_regis_rujuk.groupby('jenis_registrasi')
        res = {regis: group.drop(columns='jenis_registrasi').to_dict(orient='records') for regis, group in grouped}
        return res

    elif tipe_data == "diagnosa":
        if jenis_registrasi is not None:
            temp_df = temp_df[temp_df["jenis_registrasi"] == jenis_registrasi]

            if diagnosa is None:
                res["index"] = temp_df["diagnosa_primer"].value_counts().index.values.tolist()
                res["values"] = temp_df["diagnosa_primer"].value_counts().values.tolist()
                return res

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
                    res["index"] = temp_df["diagnosa_primer"].value_counts().index.values.tolist()[0]
                    res["values"] = temp_df["diagnosa_primer"].value_counts().values.tolist()[0]
                    return res

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

            res["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
            res["columns"] = temp_df.columns.tolist()
            # res["values"] = temp_df.values.transpose().tolist()
            res["values"] = temp_df.iloc[:, 0].tolist()


            res["dominant_age_category"] = dominan_usia.reindex(temp_df.index).tolist()
            res["dominant_age_category_summary"] = dominan_usia_kesimpulan

            return res

        else:
            return res

    elif tipe_data == "departemen":
        if jenis_registrasi is not None:
            temp_df = temp_df[temp_df["jenis_registrasi"] == jenis_registrasi]

        if departemen is None:
            res["index"] = temp_df["nama_departemen"].value_counts().index.values.tolist()
            res["values"] = temp_df["nama_departemen"].value_counts().values.tolist()
            return res

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
                res["index"] = temp_df["nama_departemen"].value_counts().index.values.tolist()
                res["values"] = temp_df["nama_departemen"].value_counts().values.tolist()
                res["columns"] = "Jumlah"
                res["indexDiagnosa"] = temp_df["diagnosa_primer"].value_counts().index.values.tolist()
                res["valuesDiagnosa"] = temp_df["diagnosa_primer"].value_counts().values.tolist()
                return res

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


        res["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        res["columns"] = temp_df.columns.tolist()
        # res["values"] = temp_df.values.transpose().tolist()
        res["values"] = temp_df.iloc[:, 0].tolist()

        res["dominant_age_category"] = dominan_usia.reindex(temp_df.index).tolist()
        res["dominant_age_category_summary"] = dominan_usia_kesimpulan

        return res

