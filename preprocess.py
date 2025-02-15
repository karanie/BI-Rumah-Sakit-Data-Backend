import pandas as pd
import numpy as np

def convert_dtypes(df):
    dtype = {
            "jenis_kelamin": "category",
            "provinsi": "category",
            "kabupaten": "category",
            "rujukan": "category",
            "jenis_registrasi": "category",
            "fix_pasien_baru": "category",
            "nama_departemen": "category",
            "jenis_penjamin": "category",
            "diagnosa_primer": "category",
            "nama_instansi_utama": "category",
            "kategori_usia": "category",
            "kelas_hak": "category",
            "pekerjaan": "category",
    }
    for col, dt in dtype.items():
        df[col] = df[col].astype(dt)
    return df

def convert_kabupaten_na(df):
    df["kabupaten"] = df["kabupaten"].astype("category")
    try:
        df["kabupaten"] = df["kabupaten"].cat.add_categories(["Tidak diketahui"])
    except ValueError:
        pass
    df["kabupaten"] = df["kabupaten"].fillna("Tidak diketahui")
    return df

def convert_kabupaten_name(df):
    df["kabupaten"] = df["kabupaten"].astype(str)

    df.loc[df["kabupaten"] == "KABUPATEN S I A K", "kabupaten"] = "KABUPATEN SIAK"
    df.loc[df["kabupaten"] == "KOTA B A T A M", "kabupaten"] = "KABUPATEN BATAM"
    df.loc[df["kabupaten"] == "KOTA D U M A I", "kabupaten"] = "KOTA DUMAI"
    df["kabupaten"] = df["kabupaten"].replace("^KABUPATEN", "KAB.", regex=True)

    df["kabupaten"] = df["kabupaten"].astype("category")
    return df

def convert_kabupaten_casing(df):
    df["kabupaten"] = df["kabupaten"].astype(str)

    df["kabupaten"] = df["kabupaten"].str.title()

    df["kabupaten"] = df["kabupaten"].astype("category")
    return df

def drop_gender_ambigu(df):
    df["jenis_kelamin"] = df["jenis_kelamin"].astype("category")
    df.loc[df["jenis_kelamin"] == "Ambigu", "jenis_kelamin"] = np.NaN
    df["jenis_kelamin"] = df["jenis_kelamin"].cat.remove_unused_categories()
    return df

def drop_duplicates(df):
    return df.drop_duplicates()

def convert_rujukan(df):
    df["rujukan"] = df["rujukan"].astype(str)

    df.loc[df["rujukan"] == "Dalam", "rujukan"] = "Dalam RS"
    df.loc[df["rujukan"] == "Luar", "rujukan"] = "Luar RS"

    df["rujukan"] = df["rujukan"].astype("category")
    return df

def convert_gender_name(df):
    df["jenis_kelamin"] = df["jenis_kelamin"].astype(str)

    df.loc[df["jenis_kelamin"] == "perempuan", "jenis_kelamin"] = "Perempuan"
    df.loc[df["jenis_kelamin"] == "laki-laki", "jenis_kelamin"] = "Laki-laki"

    df["jenis_kelamin"] = df["jenis_kelamin"].astype("category")
    return df

def sort_date_values(df):
    df = df.sort_values("waktu_registrasi")
    return df

from tools import malloc_trim
def preprocess_dataset(df):
    func_list = [
            convert_kabupaten_na,
            convert_kabupaten_name,
            convert_kabupaten_casing,
            drop_gender_ambigu,
            drop_duplicates,
            convert_rujukan,
            convert_gender_name,
            sort_date_values,
            ]

    for f in func_list:
        df = f(df)

    malloc_trim(0)

    return df

