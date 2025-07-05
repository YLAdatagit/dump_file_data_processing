import psycopg2
from scripts.db_config import db_config
from data_to_psql import (
    hwret_database, eric_air_database, eric_non_air_database,
    bfant_database, sectorsplitcell_database, nrducelltrpbeam_database,
    cellphytopo_database, dev_database
)

def update_tilt_database(config):
    str_date = config.date_to_add

    base_dir = config.base_dir
    date_column = "Date"

    bfant_file = f"{base_dir}/BFANT/LST BFANT_{str_date}.csv"
    split_file = f"{base_dir}/SECTORSPLITCELL/LST SECTORSPLITCELL_{str_date}.csv"
    nrducell_file = f"{base_dir}/NRDUCELLTRPBEAM/LST NRDUCELLTRPBEAM_{str_date}.csv"
    cellphytopo_file = f"{base_dir}/CELLPHYTOPO/DSP CELLPHYTOPO_{str_date}.csv"
    dev_file = f"{base_dir}/RETDEVICEDATA/DSP RETDEVICEDATA_{str_date}.csv"
    hwret_file = f"{base_dir}/RETSUBUNIT/DSP RETSUBUNIT_{str_date}.csv"
    eric_air_file = f"{base_dir}/RETSUBUNIT/List_ENM_SectorCarrier_All_{str_date}.csv"
    eric_non_air_file = f"{base_dir}/RETSUBUNIT/List_ENM_electricalAntennaTilt_All_{str_date}.csv"

    connection = None
    try:
        connection = psycopg2.connect(**db_config)

        bfant_database(str_date, bfant_file, connection, "bfant", date_column)
        sectorsplitcell_database(str_date, split_file, connection, "sectorsplitcell", date_column)
        nrducelltrpbeam_database(str_date, nrducell_file, connection, "nrducelltrpbeam", date_column)
        cellphytopo_database(str_date, cellphytopo_file, connection, "cellphytopo", date_column)
        dev_database(str_date, dev_file, connection, "retdevicedata_1", date_column)
        hwret_database(str_date, hwret_file, connection, "hwret_data", date_column)
        eric_air_database(str_date, eric_air_file, connection, "eric_air_data", date_column)
        eric_non_air_database(str_date, eric_non_air_file, connection, "eric_non_air_data", date_column)

    finally:
        if connection:
            connection.close()
