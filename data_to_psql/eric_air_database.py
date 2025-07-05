# yla_funs/data_to_psql/eric_air_database.py

import psycopg2
from io import StringIO
import pandas as pd
import os

def eric_air_database(date, input_file, psql_connection, table_name, date_column):
    """
    Process a CSV file, clean its data, and append it to a PostgreSQL table
    if the date doesn't already exist.
    """
    def check_date_exists(conn, table_name, date_column, target_date):
        cursor = conn.cursor()
        query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {date_column} = %s);"
        cursor.execute(query, (target_date,))
        result = cursor.fetchone()[0]
        cursor.close()
        return result

    def copy_from_dataframe(conn, df, table_name):
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, table_name, sep=",")
            conn.commit()
            print("eric_air successfully copied to the table.")
        except Exception as e:
            conn.rollback()
            print(f"An error occurred: {e}")
        finally:
            cursor.close()

    def clean_eric_air(input_file, date):
        try:
            df = pd.read_csv(input_file)
            df['Date'] = pd.to_datetime(date)
            df_1 = df[['Date', 'NodeId', 'SectorCarrierId', 'digitalTilt']]
            df_1['digitalTilt'] = pd.to_numeric(df_1['digitalTilt'], errors='coerce').fillna(99999).astype(int)
            return df_1
        except Exception as e:
            print(f"Error cleaning file {input_file}: {e}")
            return pd.DataFrame()

    target_date = pd.to_datetime(date).strftime('%Y-%m-%d')
    if check_date_exists(psql_connection, table_name, date_column, target_date):
        print(f"eric_air for date {date} already exists in the table. Skipping operation.")
    else:
        df = clean_eric_air(input_file, target_date)
        if not df.empty:
            copy_from_dataframe(psql_connection, df, table_name)
        else:
            print("No eric_air to insert.")
