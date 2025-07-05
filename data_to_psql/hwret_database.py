# yla_funs/data_to_psql/hwret_database.py

import psycopg2
from io import StringIO
import pandas as pd
import os
import re

def hwret_database(date, input_file, psql_connection, table_name, date_column):
    """
    Processes a CSV file and appends its cleaned data to a PostgreSQL table.
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
            print("hwret successfully copied to the table.")
        except Exception as e:
            conn.rollback()
            print(f"An error occurred: {e}")
        finally:
            cursor.close()

    def clean_csv(input_file, date):
        try:
            df = pd.read_csv(input_file, encoding='ISO-8859-1')
            column_mapping = {
                'FileName': 'FileName',
                'NAME': 'NAME',
                'MMLÃüÁî': 'mmlcmd',
                'Ö´ÐÐ½á¹û': 'status',
                'Device No.': 'Device_No',
                'Device Name': 'Device_Name',
                'Subunit No.': 'Subunit_no',
                'Subunit Name': 'Subunit_Name',
                'Online Status': 'Online_Status',
                'Actual Tilt(0.1degree)': 'Actual_Tilt',
                'Actual Sector ID': 'Actual_Sector_ID',
                'RET Configuration Data File Name': 'RET_Cfg_File_Name',
                'Configuration Data File Load Time': 'Cfg_File_Load_Time'
            }
            df.rename(columns=column_mapping, inplace=True)
            df['Date'] = date

            def get_site_name(name, device_name):
                name = str(name) if name else ''
                device_name = str(device_name) if device_name else ''
                device_name_cleaned = device_name.replace('AAU', '')
                match_device = re.search(r'[A-Z]{3,4}\d{3,4}', device_name_cleaned)
                if match_device:
                    return match_device.group(0)
                return name[:7]

            df['site_name'] = df.apply(lambda row: get_site_name(row['NAME'], row['Device_Name']), axis=1)
            df['Device_No'] = pd.to_numeric(df['Device_No'], errors='coerce').fillna(99999).astype(int)
            df['Actual_Tilt'] = pd.to_numeric(df['Actual_Tilt'], errors='coerce').fillna(99999).astype(int)
            df['Actual_Sector_ID'] = pd.to_numeric(df['Actual_Sector_ID'], errors='coerce').fillna(99999).astype(int)
            df['Subunit_no'] = pd.to_numeric(df['Subunit_no'], errors='coerce').fillna(99999).astype(int)
            return df
        except Exception as e:
            print(f"Error cleaning file {input_file}: {e}")
            return pd.DataFrame()

    target_date = pd.to_datetime(date).strftime('%Y-%m-%d')
    if check_date_exists(psql_connection, table_name, date_column, target_date):
        print(f"hwret for date {date} already exists in the table. Skipping operation.")
    else:
        df = clean_csv(input_file, target_date)
        if not df.empty:
            copy_from_dataframe(psql_connection, df, table_name)
        else:
            print("No hwret to insert.")
