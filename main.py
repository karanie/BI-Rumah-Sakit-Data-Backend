import json
from flask import Flask, jsonify
from flask_cors import CORS
from tools import read_dataset_pickle

app = Flask(__name__)
CORS(app)

dc1 = read_dataset_pickle(["dataset/DC"])[0]

@app.route("/api/demografi", methods=["GET"])
def data_demografi():
    data = {}
    data["index"] = dc1["kabupaten"].value_counts().index.values.tolist()
    data["values"] = dc1["kabupaten"].value_counts().values.tolist()
    return data

@app.route("/api/usia", methods=["GET"])
def data_usia():
    data = {}
    # Menghitung jumlah kategori pada setiap tahun
    dc1_count = dc1.groupby([dc1['waktu_registrasi'].dt.year, 'kategori_usia']).size().unstack(fill_value=0).reset_index()

    # Menghitung jumlah kategori secara keseluruhan
    total_kategori = dc1['kategori_usia'].value_counts().to_dict()

    data = {
        "kategori": total_kategori,
        "bytahun": dict(zip(dc1_count['waktu_registrasi'], dc1_count.set_index('waktu_registrasi').to_dict(orient='index').values()))
    }
    return data
