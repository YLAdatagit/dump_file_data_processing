# yla_funs/data_to_psql/dev_database.py

import psycopg2
from io import StringIO
import pandas as pd

def dev_database(date, input_file, psql_connection, table_name, date_column):
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
                print("retdevicedata successfully copied to the table.")
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
                'Device No.': 'Device_No',
                'Device Name': 'Device_Name',
                'Subunit No.': 'Subunit_No',
                'Subunit Name': 'Subunit_Name',
                'Antenna Model Number': 'Antenna_Model_Number',
                'Antenna Serial No.': 'Antenna_Serial_No',
                'Max Tilt(0.1degree)': 'Max_Tilt',
                'Min Tilt(0.1degree)': 'Min_Tilt',
                'Band1': 'Band1',
                'Beamwidth1(degree)': 'Beamwidth1',
                'Gain1(0.1dBi)': 'Gain1',
                'Band2': 'Band2',
                'Beamwidth2(degree)': 'Beamwidth2',
                'Gain2(0.1dBi)': 'Gain2',
                'Band3': 'Band3',
                'Beamwidth3(degree)': 'Beamwidth3',
                'Gain3(0.1dBi)': 'Gain3',
                'Band4': 'Band4',
                'Beamwidth4(degree)': 'Beamwidth4',
                'Gain4(0.1dBi)': 'Gain4',
                'Installation Date': 'Installation_Date',
                """Installer's ID""": """Installer_ID""",
                'Base Station ID': 'Base_Station_ID',
                'AISG Sector ID': 'AISG_Sector_ID',
                'Antenna Bearing(degree)': 'Antenna_Bearing',
                'Installed Mechanical Tilt(0.1degree)': 'Installed_Mechanical_Tilt_degree'
            }
            df.rename(columns=column_mapping, inplace=True)
            df['Date'] = date
            df['Device_No'] = pd.to_numeric(df['Device_No'], errors='coerce').fillna(99999).astype(int)
            df['Subunit_No'] = pd.to_numeric(df['Subunit_No'], errors='coerce').fillna(99999).astype(int)
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
        print(f"retdevicedata for date {date} already exists in the table. Skipping operation.")
    else:
        df = clean_csv(input_file, target_date)
        if not df.empty:
            copy_from_dataframe(psql_connection, df, table_name)
        else:
            print("No retdevicedata to insert.")
