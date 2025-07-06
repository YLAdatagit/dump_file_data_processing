# Dump File Data Processing

This repository contains utilities for processing radio network parameter dumps and updating a PostgreSQL database.  The main workflow extracts RAR archives, organizes the files, converts Excel sheets to CSV and finally loads the data into database tables.

## Requirements

- Python 3.8 or newer
- Install the Python packages listed in `requirements.txt` using:

```bash
pip install -r requirements.txt
```

- WinRAR installed at `C:\Program Files\WinRAR\WinRAR.exe` (used for extraction on Windows)

## Configuration

Create a `.env` file in the project root with the following variables:

```bash
DATE_TO_ADD=2024-01-01        # Date of the dump to process
BASE_DIR=/path/to/dump/files  # Base folder containing the archives and output directories
DB_NAME=database
DB_USER=user
DB_PASSWORD=password
DB_HOST=localhost
DB_PORT=5432
```

`set_date.py` can be used to update `DATE_TO_ADD` and immediately run the processing pipeline:

```bash
python set_date.py YYYY-MM-DD
```

## Workflow

`scripts/main.py` orchestrates the steps below:

1. **Extract RAR archives** from `BASE_DIR` for the given date.
2. **Move and rename CSV files** into sub‑folders.
3. **Convert Excel files** to CSV.
4. **Update the PostgreSQL database** using functions inside `data_to_psql/`.

Each loader function checks if records for the specified date already exist before inserting new rows.

## Running Manually

If you prefer to run the pipeline manually, execute:

```bash
python -m scripts.main
```

Make sure the `.env` file is configured with the desired date beforehand.

## Notes

The code assumes a directory layout defined by `BASE_DIR` and expects specific CSV and Excel file names as produced by network management systems.  Adjust the paths or filenames in `scripts/shift_files.py` and `scripts/convert_excel.py` if your files differ.

