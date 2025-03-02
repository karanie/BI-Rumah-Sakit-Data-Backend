import pandas as pd
import os
import sqlalchemy
import uuid

def seed_df_to_db(df, db_table, db_connection):
    df.to_sql(db_table, db_connection)

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

def read_sql(db_table, db_connection):
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
    return pd.read_sql(db_table, db_connection, dtype=dtype, parse_dates=parse_dates)
