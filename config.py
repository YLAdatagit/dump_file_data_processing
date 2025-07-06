from dotenv import load_dotenv
from pathlib import Path
import os
from datetime import datetime


# ✅ load from same directory (project root)
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)



date_to_add = os.getenv("DATE_TO_ADD")
date_fo = datetime.strptime(date_to_add, "%Y-%m-%d").strftime("%Y%m%d")

print(f"🧪 Loading .env from: {env_path}")
print(f"🧾 File exists: {env_path.exists()}")

base_dir = os.getenv("BASE_DIR")
if base_dir is None:
    raise ValueError("❌ BASE_DIR not loaded. Check .env formatting and load_dotenv() location.")


rar1 = f"{base_dir}/{date_fo}.rar"
rar2 = f"{base_dir}/ericsson_{date_fo}.rar"

folder1 = f"{base_dir}/{date_fo}"
folder2 = f"{base_dir}/ericsson_{date_fo}"
