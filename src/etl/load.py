import sqlite3


def load_to_sqlite(data_dict, db_path="data/payments.db"):
    """
    Loads transformed DataFrames into a SQLite database.
    data_dict: a dictionary with table_name -> DataFrame
    """
    conn = sqlite3.connect(db_path)

    for table_name, df in data_dict.items():
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"Loaded {len(df)} rows into '{table_name}' table.")

    conn.close()
    print(f"All data written to SQLite: {db_path}")
