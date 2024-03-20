import pandas as pd
import os
import pickle
import gzip

def read_dataset_pickle(files, save_as_pickle=True):
    out = []
    for i in files:
        # Read the pickle file of the pandas dataframe if it exists.
        # This prevents a really long time file reading by pandas.
        if os.path.isfile(i + ".pkl.gz"):
            with gzip.open(i + ".pkl.gz", "rb") as f:
                out.append(pickle.load(f))
        # Otherwise, read the xlsx file and save it in pickle if save_as_pickle parameter is True.
        else:
            out.append(pd.read_excel(i + ".xlsx"))
            if (save_as_pickle):
                save_dataset_as_pickle(out[len(out) - 1], i)
    return out

def read_dataset(path):
    ext = path.rsplit('.', 1)[1].lower()
    read_map = {
            "csv": pd.read_csv,
            "xlsx": pd.read_excel,
            }
    return read_map[ext](path)

def save_dataset_as_pickle(df, filename):
    with gzip.open(filename + ".pkl.gz", "wb") as f:
        pickle.dump(df, f)
