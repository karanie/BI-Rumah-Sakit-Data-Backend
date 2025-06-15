import os
import pickle
import gzip
from sources.file import read_dataset

def read_pickle(path):
    print(f"Reading {path}")

    is_gzipped = False
    with open(path, 'rb') as test_f:
        is_gzipped = test_f.read(2) == b'\x1f\x8b'

    if is_gzipped:
        with gzip.open(path, "rb") as f:
            out = pickle.load(f)
    else:
        with open(path, "rb") as f:
            out = pickle.load(f)

    return out

def save_dataset_as_pickle(df, filename, compress=True):
    if compress:
        with gzip.open(filename, "wb") as f:
            pickle.dump(df, f)
    else:
        with open(filename, "wb") as f:
            out = pickle.dump(df, f)
