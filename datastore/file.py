import os
import pickle
import gzip
from sources.file import read_dataset

def read_dataset_pickle(path, save_as_pickle=True):
    """
    Deprecated. This shit too confusing and takes too much work for something
    that does really simple thing.
    """
    file_list = [
            {"ext": ".pkl.gz"},
            {"ext": ".csv.gz"},
            {"ext": ".csv"},
            {"ext": ".xlsx"},
            ]

    for j, _ in enumerate(file_list):
        file_list[j]["exists"] = os.path.isfile(path + file_list[j]["ext"])
        if file_list[j]["exists"]:
            file_list[j]["mtime"] = os.path.getmtime(path + file_list[j]["ext"])

    file_list = list(filter(lambda item: item["exists"], file_list))
    if not file_list:
        raise Exception("No available dataset")
    latest_file = sorted(file_list, key=lambda item: item["mtime"])[-1]

    print(f"Reading {path + latest_file['ext']}")

    # Read the pickle file of the pandas dataframe if it exists.
    # This prevents a really long time file reading by pandas.
    if latest_file["ext"] == ".pkl.gz":
        with pgzip.open(path + latest_file["ext"], "rb", thread=0) as f:
            out = pickle.load(f)
    # Otherwise, read the xlsx file and save it in pickle if save_as_pickle parameter is True.
    else:
        out = read_dataset(path + latest_file["ext"])
        if (save_as_pickle):
            save_dataset_as_pickle(out, path + ".pkl.gz")
    return out

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
