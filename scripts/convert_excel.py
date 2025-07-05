from concurrent.futures import ThreadPoolExecutor
import os
from openpyxl import load_workbook
import csv

def process_file(file, dest_folder, config):
    src = os.path.join(config.folder2, file)
    if os.path.exists(src):
        new_name = f"{os.path.splitext(file)[0]}_{config.date_to_add}.csv"
        dst = os.path.join(dest_folder, new_name)
        try:
            wb = load_workbook(filename=src, read_only=True)
            ws = wb.active
            with open(dst, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerows(ws.values)
            print(f"Converted: {file} -> {new_name}")
        except Exception as e:
            print(f"Error processing {file}: {e}")
    else:
        print(f"Missing Excel: {file}")

def convert_excels(config):
    excel_files = {
        "List_ENM_electricalAntennaTilt_All.xlsx": os.path.join(config.base_dir, "RETSUBUNIT"),
        "List_ENM_SectorCarrier_All.xlsx": os.path.join(config.base_dir, "RETSUBUNIT")
    }

    with ThreadPoolExecutor() as executor:
        for file, dest_folder in excel_files.items():
            os.makedirs(dest_folder, exist_ok=True)
            executor.submit(process_file, file, dest_folder, config)
