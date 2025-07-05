from dotenv import load_dotenv
from pathlib import Path
import os

# Load .env from project root (one level up)
env_path = Path(__file__).resolve().parents[0] / ".env"
load_dotenv(dotenv_path=env_path)


date_to_add = "2025-07-04"
date_fo = "20250704"

base_dir = os.getenv("base_dir")

rar1 = f"{base_dir}/{date_fo}.rar"
rar2 = f"{base_dir}/ericsson_{date_fo}.rar"

folder1 = f"{base_dir}/{date_fo}"
folder2 = f"{base_dir}/ericsson_{date_fo}"
