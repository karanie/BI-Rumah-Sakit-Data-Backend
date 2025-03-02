from flask import Blueprint, request
import pandas as pd
from computes.filterdf import filter_in_year, filter_in_year_month
from ..data import dataset as d

routes_demografi = Blueprint("routes_demografi", __name__)
@routes_demografi.route("/api/demografi", methods=["GET"])
def data_demografi():
    res = {}
    tipe_data = request.args.get("tipe_data")
    tahun = request.args.get("tahun", type=int)
    bulan = request.args.get("bulan", type=int)

    temp_df = d

    temp_df = temp_df.loc[temp_df["provinsi"] == "RIAU"]

    if tahun is not None and bulan is not None:
        temp_df = filter_in_year_month(temp_df, "waktu_registrasi", tahun, bulan)
    if tahun is not None:
        temp_df = filter_in_year(temp_df, "waktu_registrasi", tahun)

    if tipe_data is None:
        res["index"] = temp_df["kabupaten"].value_counts().index.values.tolist()
        res["values"] = temp_df["kabupaten"].value_counts().values.tolist()
        return res
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

        res["index"] = temp_df.index.strftime("%Y-%m-%d").tolist()
        res["columns"] = temp_df.columns.tolist()
        res["values"] = temp_df.values.transpose().tolist()
        return res

    return res
