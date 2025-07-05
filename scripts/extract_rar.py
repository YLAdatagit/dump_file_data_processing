import os
import subprocess

def extract_with_winrar(rar_path, extract_to):
    if not os.path.exists(rar_path):
        print(f"RAR file not found: {rar_path}")
        return

    os.makedirs(extract_to, exist_ok=True)
    result = subprocess.run(
        ["C:\\Program Files\\WinRAR\\WinRAR.exe", "x", "-y", rar_path, extract_to],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if result.returncode != 0:
        print(f"Extraction failed: {result.stderr}")
    else:
        print(f"Extraction successful: {rar_path}")
