import random
import time
import datetime
import numpy as np
import fastapi

app = fastapi.FastAPI()

def id_registrasi():
    return -1

def id_pasien():
    return -1

def jenis_kelamin():
    return random.choice(["Laki-laki", "Perempuan"])

def ttl():
    return -1

def pekerjaan():
    label = ['Profesional ( Dokter, Pengacara, Dll )', 'Karyawan BUMN',
                  'Pemerintahan / Militer', 'Akademik/Pengajar', 'Petani',
                  'Pensiunan', 'Pegawai Negeri Sipil (PNS)', 'Lain-lain',
                  'Tidak Bekerja', 'Wiraswasta', 'Pelajar / Mahasiswa',
                  'Karyawan Swasta', 'Ibu Rumah Tangga']
    p = [0.00840089, 0.00895199, 0.01062335, 0.01648914, 0.01916252,
       0.02123067, 0.03207046, 0.06339142, 0.10545797, 0.11619956,
       0.12947224, 0.19138268, 0.27716711]
    return np.random.choice(label, p=p)

def provinsi():
    label = ['PAPUA BARAT', 'MALUKU', 'SULAWESI BARAT', 'GORONTALO',
                  'MALUKU UTARA', 'KALIMANTAN UTARA', 'SULAWESI UTARA',
                  'PAPUA', 'SULAWESI TENGAH', 'NUSA TENGGARA TIMUR',
                  'SULAWESI TENGGARA', 'BALI', 'NUSA TENGGARA BARAT',
                  'KEPULAUAN BANGKA BELITUNG', 'KALIMANTAN SELATAN',
                  'KALIMANTAN TENGAH', 'KALIMANTAN BARAT', 'KALIMANTAN TIMUR',
                  'SULAWESI SELATAN', 'BENGKULU', 'ACEH', 'LAMPUNG',
                  'DI YOGYAKARTA', 'SUMATERA SELATAN', 'JAMBI', 'JAWA TIMUR',
                  'BANTEN', 'JAWA TENGAH', 'DKI JAKARTA', 'JAWA BARAT',
                  'KEPULAUAN RIAU', 'SUMATERA UTARA', 'SUMATERA BARAT',
                  'RIAU']
    p = [6.01752303e-06, 8.02336404e-06, 1.00292050e-05, 1.00292050e-05,
       1.40408871e-05, 1.40408871e-05, 1.60467281e-05, 1.60467281e-05,
       2.00584101e-05, 2.00584101e-05, 2.60759331e-05, 3.00876151e-05,
       3.81109792e-05, 5.01460252e-05, 7.82277994e-05, 1.02297891e-04,
       1.44420553e-04, 1.56455599e-04, 1.62473122e-04, 3.14917038e-04,
       3.14917038e-04, 3.38987131e-04, 4.33261658e-04, 7.18091081e-04,
       7.60213742e-04, 8.86581726e-04, 9.64809525e-04, 9.94897140e-04,
       2.19439006e-03, 2.46919028e-03, 2.81218909e-03, 6.38258609e-03,
       8.32424019e-03, 9.71168041e-01]
    return np.random.choice(label, p=p)

def kabupaten():
    label = [
        'KOTA PEKANBARU',
        'KABUPATEN KAMPAR',
        'KABUPATEN ROKAN HULU',
        'KABUPATEN BENGKALIS',
        'KABUPATEN S I A K',
        'KABUPATEN KUANTAN SINGINGI',
        'KABUPATEN PELALAWAN',
        'KABUPATEN ROKAN HILIR',
        'KABUPATEN INDRAGIRI HULU',
        'KABUPATEN INDRAGIRI HILIR',
        'KOTA D U M A I',
        'KOTA PADANG',
        'KOTA B A T A M',
        'KABUPATEN KEPULAUAN MERANTI',
        'KOTA MEDAN',
        'KABUPATEN TANAH DATAR',
        'KABUPATEN LIMA PULUH KOTA',
        'KABUPATEN AGAM',
        'KABUPATEN PADANG LAWAS'
    ]
    p = [0.56234707, 0.30886203, 0.02591117, 0.02571404, 0.02365709,
       0.01493861, 0.00790855, 0.00790212, 0.00639798, 0.00426389,
       0.00280902, 0.00166485, 0.00164985, 0.00140558, 0.00116561,
       0.00095348, 0.00088278, 0.00088063, 0.00068565]
    return np.random.choice(label, p=p)

def rujukan():
    label = ['Dalam RS', 'Luar RS', 'Inisiatif Sendiri']
    p = [0.28420063, 0.32570935, 0.39009003]
    return np.random.choice(label, p=p)

def no_registrasi():
    return random.randint(1,100000)

def jenis_registrasi():
    label = ['OTC', 'Rawat Inap', 'IGD', 'Rawat Jalan']
    p = [0.04102797, 0.0820216 , 0.11560745, 0.76134298]
    return np.random.choice(label, p=p)

def fix_pasien_baru():
    label = ['t', 'f']
    p = [0.04285559, 0.95714441]
    return np.random.choice(label, p=p)

def nama_departemen():
    label = ['KLINIK BEDAH DIGESTIF', 'ONE DAY CARE',
                  'KLINIK GYNECOLOGY ONCOLOGY', 'ADC - EDUCATOR',
                  'ADC - FOOT CLINIC', 'KLINIK PENYAKIT MULUT',
                  'KLINIK KECANTIKAN DAN AKUPUNTUR', 'KLINIK PSIKOLOGY',
                  'KLINIK BEDAH SYARAF', 'RADIOLOGI', 'ADC - INTERNA',
                  'LAIN-LAIN', 'KLINIK BEDAH ONKOLOGI', 'KLINIK UMUM',
                  'AULIA CANCER CENTER', 'KLINIK BEDAH UROLOGY',
                  'KLINIK PSICIATRY', 'KLINIK JANTUNG', 'KLINIK THT',
                  'KLINIK KULIT DAN KELAMIN', 'KLINIK MATA',
                  'KLINIK PARU CENTER', 'KLINIK ANAK',
                  'KLINIK BEDAH ORTHOPEDY', 'KLINIK BEDAH UMUM',
                  'KLINIK BEDAH MULUT', 'HEMODIALISA', 'MEDICAL CHECK UP',
                  'KLINIK OBGYN', 'APOTIK', 'REHABILITASI MEDIK',
                  'KLINIK SYARAF', 'LABORATORIUM', 'RAWAT INAP',
                  'KLINIK GIGI UMUM', 'IGD', 'KLINIK PENYAKIT DALAM']
    p = [1.90774525e-06, 1.90774525e-06, 1.90774525e-06, 3.81549051e-06,
       5.72323576e-06, 1.65973837e-04, 6.25740444e-04, 9.06178996e-04,
       1.27246609e-03, 1.42317796e-03, 1.68072357e-03, 2.11378174e-03,
       3.16876487e-03, 4.66634489e-03, 6.63132251e-03, 1.00309246e-02,
       1.80663476e-02, 1.86978112e-02, 2.15098277e-02, 2.21699076e-02,
       2.32382450e-02, 2.34576357e-02, 2.45755744e-02, 2.51078353e-02,
       3.01996074e-02, 3.16456783e-02, 3.26510600e-02, 3.30803027e-02,
       3.47495798e-02, 3.89141877e-02, 4.94296796e-02, 6.47049958e-02,
       7.58385971e-02, 8.20196917e-02, 8.38797434e-02, 1.15607455e-01,
       1.17755576e-01]
    return np.random.choice(label, p=p)

def jenis_penjamin():
    label = ['Cob', 'Kemenkes', 'Perusahaan', 'Umum', 'BPJS']
    p = [0.00313635, 0.0161358 , 0.07528578, 0.19303059, 0.71241148]
    return np.random.choice(label, p=p)

def diagnosa_primer():
    label = ['Congestive heart failure',
                  'Cervical root disorders, not elsewhere classified',
                  'Gastro-oesophageal reflux disease without oesophagitis',
                  'Necrosis of pulp',
                  'Follow-up examination after other treatment for other conditions',
                  'Dyspepsia', 'Fever, unspecified', 'Impacted teeth',
                  'Essential (primary) hypertension',
                  'Follow-up examination after surgery for other conditions',
                  'Other physical therapy',
                  'Non-insulin-dependent diabetes mellitus without complications',
                  'Extracorporeal dialysis', 'Low back pain', 'Pulpitis']
    p = [0.02884196, 0.03290797, 0.03989336, 0.04460655, 0.04502863,
       0.04697722, 0.04700536, 0.05472938, 0.05913305, 0.06253078,
       0.07697286, 0.08039872, 0.0891709 , 0.11566329, 0.17613996]
    return np.random.choice(label, p=p)

def nama_instansi_utama():
    label = ['PT. Riau Mas Bersaudara', 'Rumah Sakit UNRI',
                  'PT. Indonesia Comnets Plus (ICON+)',
                  'Mandiri Inhealth-RAPP', 'PRUDENTIAL', 'PT. PEGADAIAN',
                  'Mandiri Inhealth', 'PT. ADONARA BAKTI BANGSA',
                  'KLINIK PRATAMA BMC', 'BRI LIFE AdMedika',
                  'PT. Bank BRI Kantor Bangkinang',
                  'PT. NAWAKARA PERKASA NUSANTARA', 'BPJS Kesehatan N.K',
                  'PT. Hutahean Group (LABERSA)', 'Jasa Raharja',
                  'Aulia Hospital', 'Medika Plaza', 'KEMENKES',
                  'BPJS Ketenagakerjaan', 'BPJS Kesehatan']
    p = [0.00115691, 0.00140831, 0.00159137, 0.00179639, 0.00195016,
       0.00206243, 0.00223816, 0.00232603, 0.00235044, 0.00252373,
       0.00252373, 0.00317785, 0.00336579, 0.00350735, 0.00441043,
       0.00485952, 0.00589684, 0.02064138, 0.02469302, 0.90752018]
    p = np.array(p)
    p /= p.sum()
    return np.random.choice(label, p=p)

def kelas_hak():
    label = ['ISOLASI', 'PERINATOLOGI', 'PRESIDENT SUITE', 'ISOLASI C',
                  'RUANG BAYI', 'NICU / PICU', 'SUITE',
                  'KELAS 1 MAIN BUILDING - TIDAK AKTIF', 'ISOLASI INFEKSIUS',
                  'KELAS 3 A - TIDAK AKTIF', 'ICU / ICCU', 'HCU/IMC',
                  'JUNIOR SUITE', 'DELUXE (VIP)', 'KELAS 1', 'KELAS 2',
                  'KELAS 3']
    p = [8.67001907e-05, 8.67001907e-05, 1.73400381e-04, 4.33500954e-04,
       4.33500954e-04, 7.80301717e-04, 1.12710248e-03, 1.21380267e-03,
       1.64730362e-03, 3.20790706e-03, 3.29460725e-03, 7.45621640e-03,
       1.57794347e-02, 2.51430553e-02, 1.34298595e-01, 3.72897520e-01,
       4.31940350e-01]
    return np.random.choice(label, p=p)

def waktu_registrasi(datetime_start):
    fmt = '%Y-%m-%d %H:%M:%S'
    _datetime_start = datetime.datetime.strptime(datetime_start, fmt)
    datetime_offset = datetime.timedelta(hours=random.randint(0,12), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    res = _datetime_start + datetime_offset
    return res.strftime(fmt)

def los_rawatan():
    return 42

def total_semua_hpp():
    return 42

def total_tagihan():
    return 42

def tanggal_lahir(datetime_start):
    fmt = '%Y-%m-%d %H:%M:%S'
    _datetime_start = datetime.datetime.strptime(datetime_start, fmt)
    datetime_offset = datetime.timedelta(weeks=random.randint(260, 2600), hours=random.randint(0,12), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    res = _datetime_start + datetime_offset
    return res.strftime(fmt)

def tglPulang(datetime_start):
    fmt = '%Y-%m-%d %H:%M:%S'
    _datetime_start = datetime.datetime.strptime(datetime_start, fmt)
    datetime_offset = datetime.timedelta(minutes=random.randint(0, 59), seconds=random.randint(0, 59))
    res = _datetime_start + datetime_offset
    return res.strftime(fmt)

def usia():
    return 42

def kategori_usia():
    label = ['anak-anak', 'bayi & balita', 'dewasa', 'lansia', 'remaja']
    return np.random.choice(label)


columns = {
    "id_registrasi": id_registrasi,
    "id_pasien": id_pasien,
    "jenis_kelamin": jenis_kelamin,
    "ttl": ttl,
    "pekerjaan": pekerjaan,
    "provinsi": provinsi,
    "kabupaten": kabupaten,
    "rujukan": rujukan,
    "no_registrasi": no_registrasi,
    "jenis_registrasi": jenis_registrasi,
    "fix_pasien_baru": fix_pasien_baru,
    "nama_departemen": nama_departemen,
    "jenis_penjamin": jenis_penjamin,
    "diagnosa_primer": diagnosa_primer,
    "nama_instansi_utama": nama_instansi_utama,
    "kelas_hak": kelas_hak,
    "waktu_registrasi": waktu_registrasi,
    "los_rawatan": los_rawatan,
    "total_semua_hpp": total_semua_hpp,
    "total_tagihan": total_tagihan,
    "tanggal_lahir": tanggal_lahir,
    "tglPulang": tglPulang,
    "usia": usia,
    "kategori_usia": kategori_usia
}

@app.get("/api/data/dummy")
def dummy(datetime_start: str | None = None):
    res_len = np.random.choice([random.randint(1, 10), 0], p=[0.2, 0.8])
    if res_len == 0:
        return []

    res = [ {} for i in range(res_len) ]
    for i in range(res_len):
        for k, v in columns.items():
            if k == "waktu_registrasi" or k == "tanggal_lahir" or k == "tglPulang":
                fmt = '%Y-%m-%d %H:%M:%S'
                if (datetime_start):
                    dt_start = datetime_start
                else:
                    dt_start = datetime.datetime.fromtimestamp(time.time()).strftime(fmt)
                res[i][k] = v(dt_start)
            else:
                res[i][k] = v()

    return res
