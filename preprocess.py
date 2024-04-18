import pandas as pd
import numpy as np

def convert_kabupaten_na(df):
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
    df.loc[df["jenis_kelamin"] == "Ambigu", "jenis_kelamin"] = np.NaN
    df["jenis_kelamin"] = df["jenis_kelamin"].cat.remove_unused_categories()
    return df

def drop_duplicates(df):
    return df.drop_duplicates()

def convert_rujukan(df):
    df.loc[df["rujukan"] == "Dalam", "rujukan"] = "Dalam RS"
    df.loc[df["rujukan"] == "Luar", "rujukan"] = "Luar RS"
    return df

def convert_gender_name(df):
    df.loc[df["jenis_kelamin"] == "perempuan", "jenis_kelamin"] = "Perempuan"
    df.loc[df["jenis_kelamin"] == "laki-laki", "jenis_kelamin"] = "Laki-laki"
    df["jenis_kelamin"] = df["jenis_kelamin"].cat.remove_unused_categories()
    return df

def sort_date_values(df):
    df = df.sort_values("waktu_registrasi")
    return df

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

    return df

