import sys
from pathlib import Path
from datetime import datetime
import subprocess

def update_env_date(new_date):
    try:
        # Validate format
        datetime.strptime(new_date, "%Y-%m-%d")
    except ValueError:
        print("❌ Invalid date format. Use YYYY-MM-DD (e.g. 2025-07-08)")
        sys.exit(1)

    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        print(f"❌ .env file not found at {env_path}")
        sys.exit(1)

    # Read and update DATE_TO_ADD only
    lines = env_path.read_text().splitlines()
    updated = []
    found = False

    for line in lines:
        if line.startswith("DATE_TO_ADD="):
            updated.append(f"DATE_TO_ADD={new_date}")
            found = True
        else:
            updated.append(line)

    if not found:
        updated.append(f"DATE_TO_ADD={new_date}")

    env_path.write_text("\n".join(updated))
    print(f"✅ .env updated: DATE_TO_ADD={new_date}")

def run_main_script():
    print("🚀 Running main pipeline...")
    result = subprocess.run(["python", "-m", "scripts.main"], check=False)
    if result.returncode == 0:
        print("✅ scripts.main ran successfully.")
    else:
        print("❌ Error running scripts.main.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python set_date.py YYYY-MM-DD")
        sys.exit(1)

    update_env_date(sys.argv[1])
    run_main_script()
