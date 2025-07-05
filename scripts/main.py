import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import traceback
import logging
from scripts.extract_rar import extract_with_winrar
from scripts.shift_files import process_csv_files
from scripts.convert_excel import convert_excels
from scripts.tilt_value import update_tilt_database
import config

import warnings
import pandas as pd

warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)
warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

# === Configure Logging ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("process_log.txt"),  # Log to file
        logging.StreamHandler()                 # Also log to terminal
    ]
)

# === Timing + Error Wrapper ===
def run_step(step_name, func):
    logging.info(f"Starting step: {step_name}")
    start = time.perf_counter()
    try:
        func()
        end = time.perf_counter()
        logging.info(f"Finished: {step_name} in {end - start:.2f}s")
    except Exception as e:
        end = time.perf_counter()
        logging.error(f"Failed: {step_name} after {end - start:.2f}s")
        logging.error(f"{type(e).__name__}: {e}")
        logging.debug(traceback.format_exc())  # Full traceback for debugging

# === Step Functions ===
def step_extract():
    extract_with_winrar(config.rar1, config.folder1)
    extract_with_winrar(config.rar2, config.folder2)

def step_move_csvs():
    process_csv_files(config)

def step_convert_excel():
    convert_excels(config)

# === Main Runner ===
def main():
    logging.info("Starting Data Processing Workflow")
    run_step("Extract RAR files", step_extract)
    run_step("Move CSV files", step_move_csvs)
    run_step("Convert Excel files", step_convert_excel)
    run_step("Update Tilt Value Database", lambda: update_tilt_database(config))

    logging.info("All steps attempted. See above for success/fail status.")

if __name__ == "__main__":
    main()
