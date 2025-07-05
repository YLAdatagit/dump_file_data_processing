import os
import shutil

def process_csv_files(config):
    destinations = {
        "LST BFANT.csv": os.path.join(config.base_dir, "BFANT"),
        "DSP CELLPHYTOPO.csv": os.path.join(config.base_dir, "CELLPHYTOPO"),
        "LST NRDUCELLTRPBEAM.csv": os.path.join(config.base_dir, "NRDUCELLTRPBEAM"),
        "LST SECTORSPLITCELL.csv": os.path.join(config.base_dir, "SECTORSPLITCELL"),
        "DSP RETSUBUNIT.csv": os.path.join(config.base_dir, "RETSUBUNIT"),
        "DSP RETDEVICEDATA.csv": os.path.join(config.base_dir, "RETDEVICEDATA")
        # ... add other mappings
    }

    for path in destinations.values():
        os.makedirs(path, exist_ok=True)

    for file, dest_folder in destinations.items():
        src = os.path.join(config.folder1, file)
        if os.path.exists(src):
            new_name = f"{os.path.splitext(file)[0]}_{config.date_to_add}.csv"
            dst = os.path.join(dest_folder, new_name)
            shutil.move(src, dst)
            print(f"Moved: {file} -> {new_name}")
        else:
            print(f"Missing: {file}")
