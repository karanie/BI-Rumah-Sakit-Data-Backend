from flask import Blueprint, request
import pandas as pd
from utils.filterdf import filter_in_year_month, resample_opt, default_filter
import data as d

routes_pasien = Blueprint("routes_pasien", __name__)
@routes_pasien.route("/api/pasien", methods=["GET"])
def routes():
    res = {}
    tipe_data = request.args.get("tipe_data")
    bulan = request.args.get("bulan", type=int)
    tahun = request.args.get("tahun", type=int)
    kabupaten = request.args.get("kabupaten", type=str)

    temp_df = d.dataset
    temp_df = default_filter(temp_df, kabupaten, tahun, bulan)

    if tipe_data == "jumlahJenisKelamin":
        if tahun is None :
            temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)
        # FIlter Pasien Unique
        temp_df = temp_df.loc[temp_df.groupby(['id_pasien', 'jenis_kelamin']).head(1).index, ['id_pasien', 'jenis_kelamin',"waktu_registrasi"]]
        res["index"] = temp_df["jenis_kelamin"].value_counts().sort_index().index.values.tolist()
        res["values"] = temp_df["jenis_kelamin"].value_counts().sort_index().values.tolist()
        return res

    elif tipe_data == "timeseriesJenisKelamin":
        # FIlter Pasien Unique
        temp_df = temp_df.loc[temp_df.groupby(['id_pasien', 'jenis_kelamin']).head(1).index, ['id_pasien', 'jenis_kelamin',"waktu_registrasi"]]

        resample_option = resample_opt(tahun,bulan)

        temp_df = pd.crosstab(temp_df["waktu_registrasi"], temp_df["jenis_kelamin"])
        temp_df = temp_df.resample(resample_option).sum()

        res["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        res["columns"] = temp_df.columns.tolist()
        res["values"] = temp_df.values.transpose().tolist()
        return res

    elif tipe_data == "pekerjaan":
        res["index"] = temp_df["pekerjaan"].value_counts().iloc[-10:].index.values.tolist()
        res["values"] = temp_df["pekerjaan"].value_counts().iloc[-10:].values.tolist()
        return res

    elif tipe_data == "jumlahPasien":
        if tahun is None :
             temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)
        res["value"] = temp_df['id_pasien'].nunique()
        return res

    elif tipe_data == "jumlahPasienPertahun":
        df_paling_awal = temp_df.loc[temp_df.groupby('id_pasien')['waktu_registrasi'].idxmin()]

        # Ekstrak tahun dari kolom waktu registrasi
        df_paling_awal['tahun'] = df_paling_awal['waktu_registrasi'].dt.year

        # Hitung jumlah pasien unik untuk setiap tahun
        jumlah_pasien_tahunan = df_paling_awal.groupby('tahun')['id_pasien'].nunique()

        res["index"] = jumlah_pasien_tahunan.index.values.tolist()
        res["values"] = jumlah_pasien_tahunan.values.tolist()

        return res

    elif tipe_data == "pasienLamaBaru":
        if tahun is None :
            temp_df = filter_in_year_month(temp_df, "waktu_registrasi", temp_df.iloc[-1]["waktu_registrasi"].year, temp_df.iloc[-1]["waktu_registrasi"].month)

        df_pasien_baru = temp_df[temp_df["fix_pasien_baru"] == "t"][["waktu_registrasi", "id_pasien"]]
        df_pasien_lama = temp_df[temp_df["fix_pasien_baru"] == "f"][["waktu_registrasi", "id_pasien"]]

        # res["jumlahPasienBaru"] = int(df_pasien_baru['id_pasien'].count())
        # res["jumlahPasienLama"] = int(df_pasien_lama['id_pasien'].count())
        res["index"] = ["Pasien Baru", "Pasien Lama"]
        res["values"] = [int(df_pasien_baru['id_pasien'].count()), int(df_pasien_lama['id_pasien'].count())]

        return res

    elif tipe_data == "usia":
        temp_df = temp_df.loc[temp_df.groupby(['id_pasien', 'kategori_usia']).head(1).index, ['id_pasien', 'kategori_usia',"waktu_registrasi"]]
        res["index"] = temp_df["kategori_usia"].value_counts().sort_index().index.values.tolist()
        res["values"] = temp_df["kategori_usia"].value_counts().sort_index().values.tolist()
        return res

