# About

Backend dari `BI-Rumah-Sakit`.

# Setup

Projek menggunakan `pipenv` sebagai package managernya. Jalankan command berikut
untuk menginstall semua dependencies-nya.

```sh
pipenv install
```

## Generate `requirements.txt`

Saat menjalankan server untuk environment yang berbeda, kemungkinan diperlukan
file `requirements.txt`. Untuk generate file `requirements.txt`, jalankan
command berikut.

```sh
pipenv requirements
```

## Dataset

Projek ini menggunakan wrapper script yang ada di `tools.py` untuk membaca lalu
menyimpan variable dalam pickle yang dikompresi. Contoh penggunaan wrapper
adalah sebagai berikut:

```python
dc1, dc2 = read_dataset_pickle(["dataset/DC1", "dataset/DC2"])
```

Kode diatas akan mencoba membaca file `dataset/DC1.xlsx` dan `dataset/DC2.xlsx`;
 mengembalikannya dalam tipe data `pandas.DataFrame`.

Direkomendasikan untuk menyimpan dataset di direktori `dataset`.

# Menjalankan Server

## Dengan `pipenv`

Untuk menjalankan server saat production, jalankan command berikut.

```sh
pipenv run flask --app main run
```

Option `--debug` dapat digunakan saat development.

```sh
pipenv run flask --app main run --debug
```
