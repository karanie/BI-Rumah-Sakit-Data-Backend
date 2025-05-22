import numpy as np
import polars as pl

class PreprocessPolars():
    def filter_cols(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.select(
            pl.col("id_registrasi"),
            pl.col("id_pasien"),
            pl.col("jenis_kelamin"),
            pl.col("ttl"),
            pl.col("provinsi"),
            pl.col("kabupaten"),
            pl.col("rujukan"),
            pl.col("no_registrasi"),
            pl.col("jenis_registrasi"),
            pl.col("fix_pasien_baru"),
            pl.col("nama_departemen"),
            pl.col("jenis_penjamin"),
            pl.col("diagnosa_primer"),
            pl.col("nama_instansi_utama"),
            pl.col("waktu_registrasi"),
            pl.col("total_semua_hpp"),
            pl.col("total_tagihan"),
            pl.col("tanggal_lahir"),
            pl.col("tglPulang"),
            pl.col("usia"),
            pl.col("kategori_usia"),
            pl.col("kelas_hak"),
            pl.col("los_rawatan"),
            pl.col("pekerjaan"),
        )
        return df

    def convert_dtypes(self, df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            pl.col("id_registrasi").cast(pl.Int64),
            pl.col("id_pasien").cast(pl.Int64),
            pl.col("jenis_kelamin").cast(pl.Categorical),
            pl.col("ttl").cast(pl.Int64),
            pl.col("provinsi").cast(pl.Categorical),
            pl.col("kabupaten").cast(pl.Categorical),
            pl.col("rujukan").cast(pl.Categorical),
            pl.col("no_registrasi").cast(pl.Int64),
            pl.col("jenis_registrasi").cast(pl.Categorical),
            pl.col("fix_pasien_baru").cast(pl.Categorical),
            pl.col("nama_departemen").cast(pl.Categorical),
            pl.col("jenis_penjamin").cast(pl.Categorical),
            pl.col("diagnosa_primer").cast(pl.Categorical),
            pl.col("nama_instansi_utama").cast(pl.Categorical),
            pl.col("waktu_registrasi").str.to_datetime(),
            pl.col("total_semua_hpp").cast(pl.Float64),
            pl.col("total_tagihan").cast(pl.Float64),
            pl.col("tanggal_lahir").str.to_date(),
            pl.col("tglPulang").str.to_datetime(),
            pl.col("usia").cast(pl.Float64),
            pl.col("kategori_usia").cast(pl.Categorical),
            pl.col("kelas_hak").cast(pl.Categorical),
            pl.col("los_rawatan").cast(pl.Float64),
            pl.col("pekerjaan").cast(pl.Categorical),
        )
        return df

    def convert_kabupaten_na(self, df: pl.LazyFrame) -> pl.LazyFrame:
        df = df.with_columns(
            pl.col("kabupaten").fill_null("Tidak Diketahui")
        )
        return df

    def convert_kabupaten_name(self, df: pl.LazyFrame) -> pl.LazyFrame:
        df = df.with_columns(
            pl.col("kabupaten").str.replace(r"^KABUPATEN S I A K$", "KABUPATEN SIAK")
        )
        df = df.with_columns(
            pl.col("kabupaten").str.replace(r"^KOTA B A T A M$", "KOTA BATAM")
        )
        df = df.with_columns(
            pl.col("kabupaten").str.replace(r"^KOTA D U M A I$", "KOTA DUMAI")
        )
        df = df.with_columns(
            pl.col("kabupaten").str.replace(r"^KABUPATEN", "KAB.")
        )
        return df

    def convert_kabupaten_casing(self, df: pl.LazyFrame) -> pl.LazyFrame:
        df = df.with_columns(
            pl.col("kabupaten").str.to_titlecase()
        )
        return df

    def drop_gender_ambigu(self, df: pl.LazyFrame) -> pl.LazyFrame:
        # Don't really need this honestly
        return df

    def drop_duplicates(self, df: pl.LazyFrame) -> pl.LazyFrame:
        df = df.unique(subset=["id_registrasi"])
        return df

    def convert_rujukan(self, df: pl.LazyFrame) -> pl.LazyFrame:
        df = df.with_columns(
            pl.col("rujukan").str.replace(r"^Dalam$", "Dalam RS")
        )
        df = df.with_columns(
            pl.col("rujukan").str.replace(r"^Luar$", "Luar RS")
        )
        return df

    def convert_gender_name(self, df: pl.LazyFrame) -> pl.LazyFrame:
        df = df.with_columns(
            pl.col("jenis_kelamin").str.replace(r"^perempuan$", "Perempuan")
        )
        df = df.with_columns(
            pl.col("jenis_kelamin").str.replace(r"^laki-laki$", "Laki-laki")
        )
        return df

    def sort_date_values(self, df: pl.LazyFrame) -> pl.LazyFrame:
        df = df.sort("waktu_registrasi")
        return df

    def preprocess_dataset(self, df: pl.LazyFrame) -> pl.DataFrame:
        func_list = [
                self.convert_kabupaten_na,
                self.convert_kabupaten_name,
                self.convert_kabupaten_casing,
                self.drop_gender_ambigu,
                self.drop_duplicates,
                self.convert_rujukan,
                self.convert_gender_name,
                self.sort_date_values,
                ]

        df = df.lazy()
        for f in func_list:
            df = f(df)
        df = df.collect()

        # from optimization import malloc_trim
        # malloc_trim(0)

        return df

class PreprocessPandas():
    def convert_dtypes(self, df):
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
        for col, dt in dtype.items():
            df[col] = df[col].astype(dt)
        return df

    def convert_kabupaten_na(self, df):
        df["kabupaten"] = df["kabupaten"].astype("category")
        try:
            df["kabupaten"] = df["kabupaten"].cat.add_categories(["Tidak diketahui"])
        except ValueError:
            pass
        df["kabupaten"] = df["kabupaten"].fillna("Tidak diketahui")
        return df

    def convert_kabupaten_name(self, df):
        df["kabupaten"] = df["kabupaten"].astype(str)

        df.loc[df["kabupaten"] == "KABUPATEN S I A K", "kabupaten"] = "KABUPATEN SIAK"
        df.loc[df["kabupaten"] == "KOTA B A T A M", "kabupaten"] = "KABUPATEN BATAM"
        df.loc[df["kabupaten"] == "KOTA D U M A I", "kabupaten"] = "KOTA DUMAI"
        df["kabupaten"] = df["kabupaten"].replace("^KABUPATEN", "KAB.", regex=True)

        df["kabupaten"] = df["kabupaten"].astype("category")
        return df

    def convert_kabupaten_casing(self, df):
        df["kabupaten"] = df["kabupaten"].astype(str)

        df["kabupaten"] = df["kabupaten"].str.title()

        df["kabupaten"] = df["kabupaten"].astype("category")
        return df

    def drop_gender_ambigu(self, df):
        df["jenis_kelamin"] = df["jenis_kelamin"].astype("category")
        df.loc[df["jenis_kelamin"] == "Ambigu", "jenis_kelamin"] = np.nan
        df["jenis_kelamin"] = df["jenis_kelamin"].cat.remove_unused_categories()
        return df

    def drop_duplicates(self, df):
        return df.drop_duplicates()

    def convert_rujukan(self, df):
        df["rujukan"] = df["rujukan"].astype(str)

        df.loc[df["rujukan"] == "Dalam", "rujukan"] = "Dalam RS"
        df.loc[df["rujukan"] == "Luar", "rujukan"] = "Luar RS"

        df["rujukan"] = df["rujukan"].astype("category")
        return df

    def convert_gender_name(self, df):
        df["jenis_kelamin"] = df["jenis_kelamin"].astype(str)

        df.loc[df["jenis_kelamin"] == "perempuan", "jenis_kelamin"] = "Perempuan"
        df.loc[df["jenis_kelamin"] == "laki-laki", "jenis_kelamin"] = "Laki-laki"

        df["jenis_kelamin"] = df["jenis_kelamin"].astype("category")
        return df

    def sort_date_values(self, df):
        df = df.sort_values("waktu_registrasi")
        return df

    def preprocess_dataset(self, df):
        from optimization import malloc_trim
        func_list = [
                self.convert_kabupaten_na,
                self.convert_kabupaten_name,
                self.convert_kabupaten_casing,
                self.drop_gender_ambigu,
                self.drop_duplicates,
                self.convert_rujukan,
                self.convert_gender_name,
                self.sort_date_values,
                ]

        for f in func_list:
            df = f(df)

        malloc_trim(0)

        return df
