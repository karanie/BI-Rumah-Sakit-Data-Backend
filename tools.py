import pandas as pd
import os
import pickle
import pgzip

def read_dataset_pickle(files, save_as_pickle=True):
    out = []
    for i in files:
        pickle_file = i + ".pkl.gz"
        excel_file = i + ".xlsx"

        pickle_exists = os.path.isfile(pickle_file)
        excel_exists = os.path.isfile(excel_file)

        pickle_mtime = os.path.getmtime(pickle_file) if pickle_exists else 0
        excel_mtime = os.path.getmtime(excel_file) if excel_exists else 0

        # Read the pickle file of the pandas dataframe if it exists.
        # This prevents a really long time file reading by pandas.
        if pickle_exists and (not excel_exists or pickle_mtime >= excel_mtime):
            with pgzip.open(pickle_file, "rb", thread=0) as f:
                out.append(pickle.load(f))
        # Otherwise, read the xlsx file and save it in pickle if save_as_pickle parameter is True.
        else:
            out.append(read_dataset(excel_file))
            if (save_as_pickle):
                save_dataset_as_pickle(out[len(out) - 1], i)
    return out

def read_dataset(path):
    ext = path.rsplit('.', 1)[1].lower()

    cols = ["id_registrasi",
            "id_pasien",
            "jenis_kelamin",
            "ttl",
            "provinsi",
            "kabupaten",
            "rujukan",
            "no_registrasi",
            "jenis_registrasi",
            "fix_pasien_baru",
            "nama_departemen",
            "jenis_penjamin",
            "diagnosa_primer",
            "nama_instansi_utama",
            "waktu_registrasi",
            "total_semua_hpp",
            "total_tagihan",
            "tanggal_lahir",
            "tglPulang",
            "usia",
            "kategori_usia"
    ]
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
    }
    parse_dates = [
        "waktu_registrasi",
        "tanggal_lahir",
        "tglPulang"
    ]

    read_map = {
            "csv": pd.read_csv,
            "xlsx": pd.read_excel,
            }
    return read_map[ext](path, usecols=cols, dtype=dtype, parse_dates=parse_dates)

def save_dataset_as_pickle(df, filename):
    with pgzip.open(filename + ".pkl.gz", "wb", thread=0) as f:
        pickle.dump(df, f)
