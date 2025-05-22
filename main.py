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
    except RuntimeError as e:
        print("Table doesn't have dataset")
        print(f"Initializing dataset from {config.INIT_SOURCE_PATH}")

        import tools
        import polars as pl

        from sources.file import SourceFile
        s = SourceFile()
        t_read, df = tools.measure_time(lambda: s.read_dataset(config.INIT_SOURCE_PATH))

        from computes.preprocess import PreprocessPolars
        pre = PreprocessPolars()
        t_filter_cols, df = tools.measure_time(lambda: pre.filter_cols(df))
        t_preprocess, df = tools.measure_time(lambda: pre.preprocess_dataset(df))
        t_conv_dtypes, df = tools.measure_time(lambda: pre.convert_dtypes(df))

        from datastore.rdbms import pl_write_database
        t_write, _ = tools.measure_time(lambda: pl_write_database(df))

        print(f"Time to read {config.INIT_SOURCE_PATH}: {t_read}s")
        print(f"Time to filter columns: {t_filter_cols}s")
        print(f"Time to preprocess: {t_preprocess}s")
        print(f"Time to convert dtypes: {t_conv_dtypes}s")
        print(f"Time to write: {t_write}s")
        print(f"Initialization done!")

def rm_datastore_dbms():
    import sqlalchemy
    engine = sqlalchemy.engine.create_engine(config.DB_CONNECTION)
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("DROP TABLE dataset"))
        conn.commit()

cli_tree = {
    "app": {
        "flask": app_flask,
        "fastapi": app_fastapi
    },
    "init": {
        "datastore-dbms": init_datastore_dbms
    },
    "rm": {
        "datastore-dbms": rm_datastore_dbms
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
