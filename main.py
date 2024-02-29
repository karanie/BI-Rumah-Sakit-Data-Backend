import json
from flask import Flask
from flask_cors import CORS
from tools import read_dataset_pickle

app = Flask(__name__)
CORS(app)

dc1 = read_dataset_pickle(["dataset/DC1"])[0]

@app.route("/api/demografi", methods=["GET"])
def data_demografi():
    data = {}
    data["index"] = dc1["kabupaten"].value_counts().index.values.tolist()
    data["values"] = dc1["kabupaten"].value_counts().values.tolist()
    return data
