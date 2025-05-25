from typing import Literal
import pandas as pd
import polars as pl
import sqlalchemy
import uuid
import config

class DatastoreDB():
    def __init__(self, backend: Literal["polars", "pandas"]="polars"):
        self.backend = backend

    def _pl_write_database(
        self,
        df: pl.DataFrame,
        db_table=config.DB_TABLE,
        connection=config.DB_CONNECTION,
        if_table_exists="append",
        engine="adbc"
    ):
        return df.write_database(
            db_table,
            connection=connection,
            if_table_exists=if_table_exists,
            engine=engine
        )

    def _pl_read_database(
        self,
        query=f"SELECT * FROM {config.DB_TABLE}",
        connection=config.DB_CONNECTION,
        execute_options = None,
        engine="sqlalchemy",
    ) -> pl.DataFrame:
        if engine == "sqlalchemy":
            sqlalchemy_engine = sqlalchemy.engine.create_engine(connection)
            conn = sqlalchemy_engine.connect()
            res =  pl.read_database(query, connection=conn, execute_options=execute_options, infer_schema_length=None)
            conn.close()
            return res
        if engine == "connectorx":
            return pl.read_database_uri(
                query,
                uri=connection,
                execute_options=execute_options
            )

    def _pd_write_database(
        self,
        df: pd.DataFrame,
        db_table=config.DB_TABLE,
        connection=config.DB_CONNECTION
    ):
        return df.to_sql(db_table, connection)

    def _pd_read_database(
        self,
        query=f"SELECT * FROM {config.DB_TABLE}",
        connection=config.DB_CONNECTION,
        dtype={
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
        },
        parse_dates = [
            "waktu_registrasi",
            "tanggal_lahir",
            "tglPulang"
        ]
    ) -> pd.DataFrame:
        return pd.read_sql(
            query,
            connection,
            dtype=dtype,
            parse_dates=parse_dates
        )

    def read_database(
        self,
        query=f"SELECT * FROM {config.DB_TABLE}",
        connection=config.DB_CONNECTION,
        execute_options=None,
    ):
        if self.backend == "polars":
            return self._pl_read_database(query=query, connection=connection, execute_options=execute_options)
        if self.backend == "pandas":
            return self._pd_read_database(query=query, connection=connection)
        raise Exception(f"{self.backend} is not available")

    def write_database(
        self,
        df,
        db_table=config.DB_TABLE,
        connection=config.DB_CONNECTION,
        if_table_exists="append",
        engine="adbc"
    ):
        if self.backend == "polars":
            return self._pl_write_database(df=df, connection=connection, engine=engine, if_table_exists=if_table_exists)
        if self.backend == "pandas":
            return self._pd_write_database(df=df, connection=connection)
        raise Exception(f"{self.backend} is not available")

    def create_table(self, table: str = config.DB_TABLE):
        import sqlalchemy
        engine = sqlalchemy.engine.create_engine(config.DB_CONNECTION)
        with engine.connect() as conn:
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


def upsert_df(df: pd.DataFrame, table_name: str, conn):
    """Implements the equivalent of pd.DataFrame.to_sql(..., if_exists='update')
    (which does not exist). Creates or updates the db records based on the
    dataframe records.
    Conflicts to determine update are based on the dataframes index.
    This will set unique keys constraint on the table equal to the index names
    1. Create a temp table from the dataframe
    2. Insert/update from temp table into table_name
    Returns: True if successful
    """

    # # If the table does not exist, we should just use to_sql to create it
    # if not engine.execute(
    #     f"""SELECT EXISTS (
    #         SELECT FROM information_schema.tables
    #         WHERE  table_schema = 'public'
    #         AND    table_name   = '{table_name}');
    #         """
    # ).first()[0]:
    #     df.to_sql(table_name, engine)
    #     return True

    engine = sqlalchemy.create_engine(conn)
    with engine.connect() as c:
        query = sqlalchemy.text(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE  table_schema = 'public'
                AND    table_name   = :table_name
            );
        """)
        result = c.execute(query, {"table_name": table_name})
        if not result.scalar():
            print("Creating non-existing table..", flush=True)
            df.to_sql(table_name, conn)
            return True

        # If it already exists...
        print("Table already exists", flush=True)
        print("Creating temporary table..", flush=True)
        temp_table_name = f"temp_{uuid.uuid4().hex[:6]}"
        df.to_sql(temp_table_name, c, index=True)

        index = list(df.index.names)
        index_sql_txt = ", ".join([f'"{i}"' for i in index])
        columns = list(df.columns)
        headers = index + columns
        headers_sql_txt = ", ".join(
            [f'"{i}"' for i in headers]
        )  # index1, index2, ..., column 1, col2, ...

        # col1 = exluded.col1, col2=excluded.col2
        update_column_stmt = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

        # For the ON CONFLICT clause, postgres requires that the columns have unique constraint
        query_pk = f"""
        ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS unique_constraint_for_upsert;
        ALTER TABLE "{table_name}" ADD CONSTRAINT unique_constraint_for_upsert UNIQUE ({index_sql_txt});
        """
        c.exec_driver_sql(query_pk)

        # Compose and execute upsert query
        print("Upserting the table..", flush=True)
        query_upsert = f"""
        INSERT INTO "{table_name}" ({headers_sql_txt})
        SELECT {headers_sql_txt} FROM "{temp_table_name}"
        ON CONFLICT ({index_sql_txt}) DO UPDATE
        SET {update_column_stmt};
        """
        c.exec_driver_sql(query_upsert)
        c.exec_driver_sql(f"DROP TABLE {temp_table_name}")

    return True
