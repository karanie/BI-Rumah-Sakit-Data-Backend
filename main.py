import json
import os.path, time
from flask import Flask, jsonify, request
from flask_cors import CORS
from werkzeug.utils import secure_filename
import pandas as pd
from tools import read_dataset_pickle, save_dataset_as_pickle
from preprocess import preprocess_dataset
from filterdf import filter_in_year, filter_in_year_month

ALLOWED_EXTENSIONS = { "csv", "xlsx" }
UPLOAD_FOLDER = "dataset/"

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
CORS(app)

dc1 = read_dataset_pickle(["dataset/DC1"])[0]
dc1 = preprocess_dataset(dc1)

@app.route("/api/dashboard", methods=["GET"])
def data_dashboard():
    data = {}
    bulan = request.args.get("bulan", type=int)
    tahun = request.args.get("tahun", type=int)
    kabupaten = request.args.get("kabupaten", type=str)
    temp_df = dc1

    if kabupaten is not None:
        temp_df = temp_df[temp_df["kabupaten"] == kabupaten]
    if tahun is not None:
        temp_df =  filter_in_year(temp_df,"waktu_registrasi",tahun)
    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df,"waktu_registrasi",tahun,bulan)

    jml_pasien = temp_df['id_pasien'].nunique()
    jml_kunjungan = temp_df['no_registrasi'].nunique()

    # Jumlah pasien tiap tahun
    df_paling_awal = dc1.loc[dc1.groupby('id_pasien')['waktu_registrasi'].idxmin()]
    df_paling_awal['tahun'] = df_paling_awal['waktu_registrasi'].dt.year
    jumlah_pasien_tahunan = df_paling_awal.groupby('tahun')['id_pasien'].nunique()

    data = {
        "jumlahPasien": jml_pasien,
        "jumlahKunjungan": jml_kunjungan,
        "jumlahPasienTahunan": jumlah_pasien_tahunan.to_dict(),
    }
    return data

@app.route("/api/jenis_registrasi", methods=["GET"])
def data_jenisRegistrasi():
    data = {}
    tahun = request.args.get("tahun", type=int)
    bulan = request.args.get("bulan", type=int)
    kabupaten = request.args.get("kabupaten", type=str)

    temp_df = dc1

    if kabupaten is not None:
        temp_df = temp_df[temp_df["kabupaten"] == kabupaten]
    if tahun is not None:
        temp_df =  filter_in_year(temp_df,"waktu_registrasi",tahun)
    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df,"waktu_registrasi",tahun,bulan)

    if tahun is None and bulan is None:
        resample_option = "Y"
    elif tahun is not None and bulan is None:
        resample_option = "M"
    else:
        resample_option = "D"

    temp_df = temp_df[["waktu_registrasi","jenis_registrasi"]]
    temp_df = pd.crosstab(temp_df["waktu_registrasi"],temp_df["jenis_registrasi"])
    temp_df = temp_df.resample(resample_option).sum()

    data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
    data["columns"] = temp_df.columns.tolist()
    data["values"] = temp_df.values.transpose().tolist()
    return data


@app.route("/api/kunjungan", methods=["GET"])
def data_kunjungan():
    data = {}
    tahun = request.args.get("tahun", type=int)
    bulan = request.args.get("bulan", type=int)
    kabupaten = request.args.get("kabupaten", type=int)

    temp_df = dc1

    if kabupaten is not None:
        temp_df = temp_df[temp_df["kabupaten"] == kabupaten]
    if tahun is not None:
        temp_df =  filter_in_year(temp_df,"waktu_registrasi",tahun)
    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df,"waktu_registrasi",tahun,bulan)


    if tahun is None and bulan is None:
        resample_option = "Y"
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


@app.route("/api/demografi", methods=["GET"])
def data_demografi():
    data = {}
    tipe_data = request.args.get("tipe_data")
    tahun = request.args.get("tahun", type=int)
    bulan = request.args.get("bulan", type=int)

    temp_df = dc1
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
            temp_df = dc1[["waktu_registrasi", "kabupaten"]]
            temp_df = filter_in_year(temp_df, "waktu_registrasi", 2021)
        else:
            temp_df = temp_df[["waktu_registrasi", "kabupaten"]]

        # Pick top 10
        temp_df = temp_df[["kabupaten", "waktu_registrasi"]]
        temp_df["kabupaten_filtered"] = temp_df["kabupaten"]
        temp_df.loc[~temp_df["kabupaten_filtered"].isin(temp_df["kabupaten"].value_counts()[:10].index), "kabupaten_filtered"] = "Lainnya"
        temp_df = temp_df.drop(columns="kabupaten")

        temp_df = pd.crosstab(temp_df["waktu_registrasi"], temp_df["kabupaten_filtered"])
        resample_option = "D" if bulan is not None else "W"
        temp_df = temp_df.resample(resample_option).sum()

        data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df.values.transpose().tolist()
        return data

    return data

@app.route("/api/usia", methods=["GET"])
def data_usia():

    jml_pasien = dc1['id_pasien'].nunique()

    #jumlah pasien tahunan
    # Mencari waktu registrasi paling awal untuk setiap id_pasien
    df_paling_awal = dc1.loc[dc1.groupby('id_pasien')['waktu_registrasi'].idxmin()]

    # Ekstrak tahun dari kolom waktu registrasi
    df_paling_awal['tahun'] = df_paling_awal['waktu_registrasi'].dt.year

    # Hitung jumlah pasien unik untuk setiap tahun
    jumlah_pasien_tahunan = df_paling_awal.groupby('tahun')['id_pasien'].nunique()

    # Kelompokkan berdasarkan kelompok usia dan hitung jumlahnya
    jumlah_kelompok_usia = df_paling_awal.groupby('kategori_usia').size()

    # Menghitung jumlah kategori pada setiap tahun
    dc1_count = df_paling_awal.groupby([df_paling_awal['waktu_registrasi'].dt.year, 'kategori_usia']).size().unstack(fill_value=0).reset_index()

    data = {
        "jumlah_pasien":jml_pasien,
        "jumlah_pasien_tahunan": jumlah_pasien_tahunan.to_dict(),
        "kategori": jumlah_kelompok_usia.to_dict(),
        "bytahun": dict(zip(dc1_count['waktu_registrasi'], dc1_count.set_index('waktu_registrasi').to_dict(orient='index').values()))
    }
    return data

@app.route("/api/jeniskelamin", methods=["GET"])
def data_jeniskelamin():
    data = {}
    tipe_data = request.args.get("tipe_data")
    bulan = request.args.get("bulan", type=int)
    tahun = request.args.get("tahun", type=int)
    kabupaten = request.args.get("kabupaten", type=str)
    temp_df = dc1

    if kabupaten is not None:
        temp_df = temp_df[temp_df["kabupaten"] == kabupaten]
    if tahun is not None:
        temp_df =  filter_in_year(temp_df,"waktu_registrasi",tahun)
    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df,"waktu_registrasi",tahun,bulan)

    if tipe_data is None:
        data["index"] = temp_df["jenis_kelamin"].value_counts().sort_index().index.values.tolist()
        data["values"] = temp_df["jenis_kelamin"].value_counts().sort_index().values.tolist()
        return data
    elif tipe_data == "timeseries":
        if tahun is None and bulan is None:
            # temp_df = temp_df.groupby([temp_df['waktu_registrasi'].dt.month, 'jenis_kelamin']).size().unstack(fill_value=0)
            resample_option = "Y"
        elif tahun is not None and bulan is None:
            resample_option = "M"
        else:
            resample_option = "D"

        temp_df = temp_df[["waktu_registrasi", "jenis_kelamin"]]
        temp_df = pd.crosstab(temp_df["waktu_registrasi"], temp_df["jenis_kelamin"])
        temp_df = temp_df.resample(resample_option).sum()

        data["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        data["columns"] = temp_df.columns.tolist()
        data["values"] = temp_df.values.transpose().tolist()
        return data

    return data

@app.route("/api/penjamin", methods=["GET"])
def data_jenispenjamin():
    data = {}
    bulan = request.args.get("bulan", type=int)
    tahun = request.args.get("tahun", type=int)
    kabupaten = request.args.get("kabupaten", type=str)
    temp_df = dc1

    if kabupaten is not None:
        temp_df = temp_df[temp_df["kabupaten"] == kabupaten]
    if tahun is not None:
        temp_df =  filter_in_year(temp_df,"waktu_registrasi",tahun)
    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df,"waktu_registrasi",tahun,bulan)

    data["index"] = temp_df["jenis_penjamin"].value_counts().index.values.tolist()
    data["values"] = temp_df["jenis_penjamin"].value_counts().values.tolist()
    return data

@app.route("/api/instansi", methods=["GET"])
def data_instansi():
    # Menghapus data dengan nama_instansi_utama bernama BPJS Kesehatan
    filtered_data = dc1[dc1["jenis_penjamin"] == "Perusahaan"]

    # Menghitung ulang value_counts setelah data di-filter
    data = {}
    bulan = request.args.get("bulan", type=int)
    tahun = request.args.get("tahun", type=int)
    kabupaten = request.args.get("kabupaten", type=str)
    temp_df = filtered_data

    if kabupaten is not None:
        temp_df = temp_df[temp_df["kabupaten"] == kabupaten]
    if tahun is not None:
        temp_df =  filter_in_year(temp_df,"waktu_registrasi",tahun)
    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df,"waktu_registrasi",tahun,bulan)

    data["index"] = temp_df["nama_instansi_utama"].value_counts().index.values.tolist()
    data["values"] = temp_df["nama_instansi_utama"].value_counts().values.tolist()

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
        df_new = preprocess_dataset(pd.read_csv(dataset_file_path))
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
