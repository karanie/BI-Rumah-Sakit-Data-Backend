import os.path, time
from flask import Blueprint, request
import pandas as pd
from werkzeug.utils import secure_filename
from datastore.file import save_dataset_as_pickle, read_dataset
from computes.preprocess import PreprocessPandas
from config import ALLOWED_EXTENSIONS, DATASTORE_FILE_PATH, UPLOAD_FOLDER
from ..data import dataset as d

routes_utils = Blueprint("routes_utils", __name__)
@routes_utils.route("/api/last-update", methods=["GET"])
def last_update():
    res = {}
    res["mtime"] = os.path.getmtime(DATASTORE_FILE_PATH)
    res["mtimeLocaltime"] = time.ctime(os.path.getmtime(DATASTORE_FILE_PATH))
    res["waktuRegistrasiTerakhir"] = d.iloc[-1]["waktu_registrasi"]
    return res

@routes_utils.route("/api/filter-options", methods=["GET"])
def data_filter_options():
    res = {}
    res["kabupaten"] = sorted(d["kabupaten"].unique().tolist())
    res["tahun"] = sorted(d["waktu_registrasi"].dt.year.unique().tolist())
    res["bulan"] = sorted(d["waktu_registrasi"].dt.month.unique().tolist())
    return res

@routes_utils.route("/api/update-dataset", methods=["POST"])
def update_dataset():
    global d
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
        dataset_file_path = os.path.join(UPLOAD_FOLDER, filename)
        dataset_file.save(dataset_file_path)

        read_dataset_time_start = time.time()
        df_new = read_dataset(dataset_file_path)
        read_dataset_time_end = time.time()

        concat_time_start = time.time()
        df = pd.concat([d, df_new], ignore_index=True)
        concat_time_end = time.time()

        preprocess_time_start = time.time()
        pr = PreprocessPandas()
        df = pr.convert_dtypes(df)
        df = pr.preprocess_dataset(df)
        preprocess_time_end = time.time()

        save_time_start = time.time()
        # Compressing the pickle file with gzip takes around ~85 seconds on
        # my computer. This may be a concern if you want faster dataset update
        save_dataset_as_pickle(df, DATASTORE_FILE_PATH)
        save_time_end = time.time()

        d = df
        return {
                "status": "Updated",
                "read_dataset_time": read_dataset_time_end - read_dataset_time_start,
                "preprocess_time": preprocess_time_end - preprocess_time_start,
                "concat_time": concat_time_end - concat_time_start,
                "save_time": save_time_end - save_time_start,
        }

    return {
            "status": "Unsupported method"
    }
