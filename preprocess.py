import pandas as pd

def convert_kabupaten_na(df):
    df["kabupaten"] = df["kabupaten"].fillna("Tidak diketahui")
    return df

def convert_kabupaten_name(df):
    df.loc[df["kabupaten"] == "KABUPATEN S I A K", "kabupaten"] = "KABUPATEN SIAK"
    df.loc[df["kabupaten"] == "KOTA B A T A M", "kabupaten"] = "KABUPATEN BATAM"
    df.loc[df["kabupaten"] == "KOTA D U M A I", "kabupaten"] = "KOTA DUMAI"
    return df

def convert_kabupaten_casing(df):
    df["kabupaten"] = df["kabupaten"].str.title()
    return df

def preprocess_dataset(df):
    func_list = [
            convert_kabupaten_na,
            convert_kabupaten_name,
            convert_kabupaten_casing,
            ]

    for f in func_list:
        df = f(df)

    return df

