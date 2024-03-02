import pandas as pd

def convert_na_kabupaten(df):
    df["kabupaten"] = df["kabupaten"].fillna("Tidak diketahui")
    return df

def preprocess_dataset(df):
    func_list = [
            convert_na_kabupaten
            ]

    for f in func_list:
        df = f(df)

    return df

