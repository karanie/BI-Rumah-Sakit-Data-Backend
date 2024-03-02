import json
from flask import Flask, jsonify
from flask_cors import CORS
from tools import read_dataset_pickle

app = Flask(__name__)
CORS(app)

dc1 = read_dataset_pickle(["dataset/DC1"])[0]
dc1 = preprocess_dataset(dc1)

@app.route("/api/demografi", methods=["GET"])
def data_demografi():
    data = {}
    data["index"] = dc1["kabupaten"].value_counts().index.values.tolist()
    data["values"] = dc1["kabupaten"].value_counts().values.tolist()
    return data

@app.route("/api/usia", methods=["GET"])
def data_usia():
    # Menghitung jumlah kategori pada setiap tahun
    dc1_count = dc1.groupby([dc1['waktu_registrasi'].dt.year, 'kategori_usia']).size().unstack(fill_value=0).reset_index()

    # Menghitung jumlah kategori secara keseluruhan
    total_kategori = dc1['kategori_usia'].value_counts().to_dict()

    data = {
        "kategori": total_kategori,
        "bytahun": dict(zip(dc1_count['waktu_registrasi'], dc1_count.set_index('waktu_registrasi').to_dict(orient='index').values()))
    }
    return data

@app.route("/api/jeniskelamin", methods=["GET"])
def data_jeniskelamin():
    data = {}
    data["index"] = dc1["jenis_kelamin"].value_counts().index.values.tolist()
    data["values"] = dc1["jenis_kelamin"].value_counts().values.tolist()
    return data

@app.route("/api/penjamin", methods=["GET"])
def data_jenispenjamin():
    data = {}
    data["index"] = dc1["jenis_penjamin"].value_counts().index.values.tolist()
    data["values"] = dc1["jenis_penjamin"].value_counts().values.tolist()
    return data

@app.route("/api/instansi", methods=["GET"])
def data_instansi():
    # Menghapus data dengan nama_instansi_utama bernama BPJS Kesehatan
    filtered_data = dc1[dc1["nama_instansi_utama"] != "BPJS Kesehatan"]

    # Menghitung ulang value_counts setelah data di-filter
    data = {}
    data["index"] = filtered_data["nama_instansi_utama"].value_counts().index.values.tolist()
    data["values"] = filtered_data["nama_instansi_utama"].value_counts().values.tolist()

    return data
