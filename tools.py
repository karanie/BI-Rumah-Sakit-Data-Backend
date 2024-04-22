import pandas as pd
import os
import pickle
import pgzip

def read_dataset_pickle(files, save_as_pickle=True):
    out = []
    for i in files:
        file_list = [
                {"ext": ".pkl.gz"},
                {"ext": ".csv"},
                {"ext": ".xlsx"},
                ]

        for j, _ in enumerate(file_list):
            file_list[j]["exists"] = os.path.isfile(i + file_list[j]["ext"])
            if file_list[j]["exists"]:
                file_list[j]["mtime"] = os.path.getmtime(i + file_list[j]["ext"])

        file_list = list(filter(lambda item: item["exists"], file_list))
        if not file_list:
            raise Exception("No available dataset")
        latest_file = sorted(file_list, key=lambda item: item["mtime"])[-1]

        print(f"Reading {i + latest_file['ext']}")

        # Read the pickle file of the pandas dataframe if it exists.
        # This prevents a really long time file reading by pandas.
        if latest_file["ext"] == ".pkl.gz":
            with pgzip.open(i + latest_file["ext"], "rb", thread=0) as f:
                out.append(pickle.load(f))
        # Otherwise, read the xlsx file and save it in pickle if save_as_pickle parameter is True.
        else:
            out.append(read_dataset(i + latest_file["ext"]))
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
