# src/data/atlas_ingest.py

import os
import sys
import pandas as pd

def load_atlas(path: str) -> pd.DataFrame:
    if not os.path.isfile(path):
        sys.exit(f"ERROR: Atlas file not found at {path}")
    xls = pd.ExcelFile(path)
    sheet = xls.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet)
    return df, sheet

def preview_atlas():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    base_raw  = os.path.join(repo_root, "data", "raw")

    # Explicit antifungals path
    antifungals_file = os.path.join(base_raw, "ATLAS_Antifungals", "vivli_sentry_2010_2023.xlsx")
    antibiotics_dir  = os.path.join(base_raw, "ATLAS_Antibiotics")

    # Find the one antibiotics file in that folder
    abx_files = [f for f in os.listdir(antibiotics_dir) if f.lower().endswith(".xlsx")]
    if not abx_files:
        sys.exit(f"ERROR: No .xlsx in {antibiotics_dir}")
    antibiotics_file = os.path.join(antibiotics_dir, abx_files[0])

    for name, path in [("Antifungals", antifungals_file),
                       ("Antibiotics", antibiotics_file)]:

        df, sheet = load_atlas(path)
        print(f"\n--- {name} ({os.path.basename(path)}) :: sheet '{sheet}' ---")
        print(f"Rows:    {df.shape[0]}    Columns: {df.shape[1]}")
        print("First 3 rows:")
        print(df.head(3))

if __name__ == "__main__":
    preview_atlas()
