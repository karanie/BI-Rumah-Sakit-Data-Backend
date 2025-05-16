import sys
import config

def app_flask():
    from app.flaskapp import app
    app.run(host="0.0.0.0")

def app_fastapi():
    import uvicorn
    uvicorn.run("app.fastapi:app", host="0.0.0.0", port=5001, log_level="info")

def init_datastore_dbms():
    import connectorx as cx
    try:
        print("Checking if dbms have dataset..")
        cx.read_sql(config.DB_CONNECTION, "SELECT id_registrasi FROM dataset LIMIT 1")
        print("Table have dataset")
    except Exception as e:
        print("Table doesn't have dataset")
        print(f"Initializing dataset from {config.INIT_SOURCE_PATH}")
        import polars as pl
        from sources.file import SourceFile
        s = SourceFile()
        df = s.read_dataset(config.INIT_SOURCE_PATH)
        df = df.select(
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
        from datastore.rdbms import pl_write_database
        pl_write_database(df)

cli_tree = {
    "app": {
        "flask": app_flask,
        "fastapi": app_fastapi
    },
    "init": {
        "datastore-dbms": init_datastore_dbms
    }
}

def interpret_argv(argv, tree):
    tree = tree[argv.pop(0)]
    if len(argv) != 0:
        interpret_argv(argv, tree)
    else:
        tree()

if __name__ == "__main__":
    interpret_argv(sys.argv[1:], cli_tree)
