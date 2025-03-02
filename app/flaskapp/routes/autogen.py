from flask import Blueprint
from ..data import dataset as d
from ..utils.generateroutes import generate_route_callback, init_routes_data

def filterDemografiInRiau(df):
    return df.loc[df["provinsi"] == "RIAU"]

routes_autogen = Blueprint("routes_autogen", __name__)
routes = [
    {
        "route": "/demografi",
        "callback": generate_route_callback(
            name="demografi",
            df=d,
            timeCol="waktu_registrasi",
            categoricalCols=["kabupaten"],
            preprocess=filterDemografiInRiau,
        )
    },
    {
        "route": "/jenisregis",
        "callback": generate_route_callback(
            name="jenisregis",
            df=d,
            timeCol="waktu_registrasi",
            categoricalCols=["jenis_registrasi"]
        )
    },
    {
        "route": "/pendapatan",
        "callback": generate_route_callback(
            name="pendapatan",
            df=d,
            timeCol="waktu_registrasi",
            numericalCols=["total_tagihan", "total_semua_hpp"]
        )
    },
    {
        "route": "/pendapatan/jenisregis",
        "callback": generate_route_callback(
            name="pendapatanJenisregis",
            df=d,
            timeCol="waktu_registrasi",
            categoricalCols=["jenis_registrasi"],
            numericalCols=["total_tagihan"],
            models=[
                { "path": "data/models/pendapatan/prophet_pendapatan_IGD.pkl", "column": "IGD" },
                { "path": "data/models/pendapatan/prophet_pendapatan_OTC.pkl", "column": "OTC" },
                { "path": "data/models/pendapatan/prophet_pendapatan_Rawat Jalan.pkl", "column": "Rawat Jalan" },
                { "path": "data/models/pendapatan/prophet_pendapatan_Rawat Inap.pkl", "column": "Rawat Inap" },
            ]
        )
    },
    {
        "route": "/kunjungan/jenisregis",
        "callback": generate_route_callback(
            name="kunjunganJenisregis",
            df=d,
            timeCol="waktu_registrasi",
            categoricalCols=["jenis_registrasi"],
            models=[
                { "path": "data/models/kunjungan/prophet_kunjungan_model_IGD.pkl", "column": "IGD" },
                { "path": "data/models/kunjungan/prophet_kunjungan_model_OTC.pkl", "column": "OTC" },
                { "path": "data/models/kunjungan/prophet_kunjungan_model_Rawat Jalan.pkl", "column": "Rawat Jalan" },
                { "path": "data/models/kunjungan/prophet_kunjungan_model_Rawat Inap.pkl", "column": "Rawat Inap" },
            ]
        )
    },
]
routes_autogen = init_routes_data(routes_autogen, routes)
