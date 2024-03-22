import pandas as pd
import numpy as np

def convert_kabupaten_na(df):
    df["kabupaten"] = df["kabupaten"].fillna("Tidak diketahui")
    return df

def convert_kabupaten_name(df):
    df.loc[df["kabupaten"] == "KABUPATEN S I A K", "kabupaten"] = "KABUPATEN SIAK"
    df.loc[df["kabupaten"] == "KOTA B A T A M", "kabupaten"] = "KABUPATEN BATAM"
    df.loc[df["kabupaten"] == "KOTA D U M A I", "kabupaten"] = "KOTA DUMAI"
    df["kabupaten"] = df["kabupaten"].replace("^KABUPATEN", "KAB.", regex=True)
    return df

def convert_kabupaten_casing(df):
    df["kabupaten"] = df["kabupaten"].str.title()
    return df

def drop_gender_ambigu(df):
    df.loc[df["jenis_kelamin"] == "Ambigu", "jenis_kelamin"] = np.NaN
    return df

def convert_waktu_registrasi_to_datetime(df):
    df["waktu_registrasi"] = pd.to_datetime(df["waktu_registrasi"])
    return df

def convert_rujukan(df):
    df.loc[df["rujukan"] == "Dalam", "rujukan"] = "Dalam RS"
    df.loc[df["rujukan"] == "Luar", "rujukan"] = "Luar RS"
    return df

def preprocess_dataset(df):
    func_list = [
            convert_kabupaten_na,
            convert_kabupaten_name,
            convert_kabupaten_casing,
            drop_gender_ambigu,
            convert_waktu_registrasi_to_datetime,
            convert_rujukan,
            ]

    for f in func_list:
        df = f(df)

    return df

