# yla_funs/data_to_psql/sectorsplitcell_database.py

import psycopg2
from io import StringIO
import pandas as pd

def sectorsplitcell_database(date, input_file, psql_connection, table_name, date_column):
    """
    Processes a CSV file and appends its cleaned data to a PostgreSQL table.
    """
    def check_date_exists(conn, table_name, date_column, target_date):
        with conn.cursor() as cursor:
            query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {date_column} = %s);"
            cursor.execute(query, (target_date,))
            return cursor.fetchone()[0]

    def copy_from_dataframe(conn, df, table_name):
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        with conn.cursor() as cursor:
            try:
                cursor.copy_from(buffer, table_name, sep=",")
                conn.commit()
                print("sectorsplitcell successfully copied to the table.")
            except Exception as e:
                conn.rollback()
                print(f"An error occurred: {e}")

    def clean_csv(input_file, date):
        try:
            df = pd.read_csv(input_file, encoding='ISO-8859-1')
            if df.empty:
                print(f"File {input_file} is empty. Skipping.")
                return pd.DataFrame()
            column_mapping = {
                'FileName': 'FileName',
                'NAME': 'NAME',
                'MMLÃüÁî': 'MML',
                'Ö´ÐÐ½á¹û': 'status',
                'Local cell ID': 'Local_cell_ID',
                'Sector Split Group ID': 'Sector_Split_Group_ID',
                'Cell Beam Index': 'Cell_Beam_Index',
                'Cell Beam Tilt(degree)': 'Cell_Beam_Tilt',
                'Cell Beam Azimuth Offset(degree)': 'Cell_Beam_Azimuth_Offset',
                'Cell Beam Azimuth(degree)': 'Cell_Beam_Azimuth',
                'Cell Beamwidth': 'Cell_Beamwidth',
                'Cell Beam Tilt Fraction Part(0.1degree)': 'Cell_Beam_Tilt_Fraction_Part',
                'Cell Beam Power(0.1W)': 'Cell_Beam_Power',
                'Low-Frequency Ultra-Lean Site Weight': 'Low_Frequency_Ultra_Lean_Site_Weight'
            }
            df.rename(columns=column_mapping, inplace=True)
            df['Date'] = date
            column_to_move = 'Date'
            cols = [column_to_move] + [col for col in df.columns if col != column_to_move]
            df = df[cols]
            df.replace({r'\r': '', r'\n': '', ',': ''}, regex=True, inplace=True)
            return df
        except Exception as e:
            print(f"Error cleaning file {input_file}: {e}")
            return pd.DataFrame()

    try:
        target_date = pd.to_datetime(date).strftime('%Y-%m-%d')
    except ValueError:
        print(f"Invalid date format: {date}. Expected format: YYYY-MM-DD")
        return

    if check_date_exists(psql_connection, table_name, date_column, target_date):
        print(f"sectorsplitcell for date {date} already exists in the table. Skipping operation.")
    else:
        df = clean_csv(input_file, target_date)
        if not df.empty:
            copy_from_dataframe(psql_connection, df, table_name)
        else:
            print("No sectorsplitcell to insert.")
