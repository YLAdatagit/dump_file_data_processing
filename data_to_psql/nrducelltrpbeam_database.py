# yla_funs/data_to_psql/nrducelltrpbeam_database.py

import psycopg2
from io import StringIO
import pandas as pd

def nrducelltrpbeam_database(date, input_file, psql_connection, table_name, date_column):
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
                print("nrducelltrpbeam successfully copied to the table.")
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
                'NR DU Cell TRP ID': 'NR_DU_Cell_TRP_ID',
                'Coverage Scenario': 'Coverage_Scenario',
                'Tilt(degree)': 'Tilt',
                'Azimuth(degree)': 'Azimuth',
                'Max SSB Power Offset(dB)': 'Max_SSB_Power_Offset',
                'Scenario-based Beam Algorithm Switch': 'Scenario_based_Beam_Algorithm_Switch',
                'Connected Mode Coverage Scenario': 'Connected_Mode_Coverage_Scenario',
                'TRP Antenna Type': 'TRP_Antenna_Type'
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
        print(f"nrducelltrpbeam for date {date} already exists in the table. Skipping operation.")
    else:
        df = clean_csv(input_file, target_date)
        if not df.empty:
            copy_from_dataframe(psql_connection, df, table_name)
        else:
            print("No nrducelltrpbeam to insert.")
