import pandas as pd

def read_dataset(path):
    ext = path.split('.', 1)[1].lower()

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
            "kategori_usia",
            "kelas_hak",
            "los_rawatan",
            "pekerjaan",
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
            "kelas_hak": "category",
            "pekerjaan": "category",
    }
    parse_dates = [
        "waktu_registrasi",
        "tanggal_lahir",
        "tglPulang"
    ]

    read_map = {
            "csv": pd.read_csv,
            "csv.gz": pd.read_csv,
            "xlsx": pd.read_excel,
            }
    return read_map[ext](path, usecols=cols, dtype=dtype, parse_dates=parse_dates)
