from typing import Literal
import polars as pl
import sqlalchemy
import uuid
import config

class DatastoreDB():
    def __init__(
        self,
        backend: Literal["polars", "pandas"]="polars",
        connection=config.DB_CONNECTION,
    ):
        self.backend = backend
        self.connection = connection
        self._sqlalchemy_engine = sqlalchemy.engine.create_engine(self.connection)

    def _pl_write_database(
        self,
        df: pl.DataFrame,
        db_table=config.DB_TABLE,
        if_table_exists="append",
        engine="adbc"
    ):
        if engine == "sqlalchemy":
            return df.write_database(
                db_table,
                connection=self.connection,
                if_table_exists=if_table_exists,
                engine=engine
            )
        if engine == "adbc":
            return df.write_database(
                db_table,
                connection=self.connection,
                if_table_exists=if_table_exists,
                engine=engine
            )

    def _pl_read_database(
        self,
        query=f"SELECT * FROM {config.DB_TABLE}",
        execute_options = None,
        engine="sqlalchemy",
    ) -> pl.DataFrame:
        if engine == "sqlalchemy":
            conn = self._sqlalchemy_engine.connect()
            res =  pl.read_database(query, connection=conn, execute_options=execute_options, infer_schema_length=None)
            conn.close()
            return res
        if engine == "connectorx":
            return pl.read_database_uri(
                query,
                uri=self.connection,
                execute_options=execute_options
            )

    def read_database(
        self,
        query=f"SELECT * FROM {config.DB_TABLE}",
        execute_options=None,
    ):
        if self.backend == "polars":
            return self._pl_read_database(query=query, execute_options=execute_options)
        if self.backend == "pandas":
            return Exception(f"Not supported anymore. Please use Polars.")
        raise Exception(f"{self.backend} is not available")

    def write_database(
        self,
        df,
        db_table=config.DB_TABLE,
        if_table_exists="append",
        engine="adbc"
    ):
        if self.backend == "polars":
            return self._pl_write_database(df=df, engine=engine, if_table_exists=if_table_exists)
        if self.backend == "pandas":
            return Exception(f"Not supported anymore. Please use Polars.")
        raise Exception(f"{self.backend} is not available")

    def create_table(self, table: str = config.DB_TABLE):
        with self._sqlalchemy_engine.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    f"""
                    CREATE TABLE {table} (
                        id_registrasi bigint,
                        id_pasien bigint,
                        jenis_kelamin text,
                        ttl bigint,
                        provinsi text,
                        kabupaten text,
                        rujukan text,
                        no_registrasi bigint,
                        jenis_registrasi text,
                        fix_pasien_baru text,
                        nama_departemen text,
                        jenis_penjamin text,
                        diagnosa_primer text,
                        nama_instansi_utama text,
                        waktu_registrasi timestamp,
                        total_semua_hpp double precision,
                        total_tagihan double precision,
                        tanggal_lahir date,
                        "tglPulang" timestamp,
                        usia double precision,
                        kategori_usia text,
                        kelas_hak text,
                        los_rawatan double precision,
                        pekerjaan text
                    )
                    """
                )
            )
            conn.execute(
                sqlalchemy.text(
                    """
                    CREATE OR REPLACE FUNCTION notify_new_data()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        PERFORM pg_notify('new_data_channel', NEW.waktu_registrasi::text);
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    """
                )
            )
            conn.execute(
                sqlalchemy.text(
                    f"""
                    CREATE TRIGGER new_data_notification
                    AFTER INSERT ON {table}
                    FOR EACH STATEMENT
                    EXECUTE FUNCTION notify_new_data();
                    """
                )
            )
            conn.commit()

    def execute(self, query):
        conn = self._sqlalchemy_engine.connect()
        res = conn.execute(query)
        conn.close()
        return res

