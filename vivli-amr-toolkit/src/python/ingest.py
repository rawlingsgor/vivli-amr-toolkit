import pathlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------
# Paths
ROOT         = pathlib.Path(__file__).resolve().parents[2]    # repo root
RAW_FOLDER   = ROOT / "data" / "raw"                         # raw .xlsx location
PROC_FOLDER  = ROOT / "data" / "processed"                   # output parquet
BP_FILE      = ROOT / "docs" / "eucast_2025_breakpoints.csv" # breakpoint table

# ---------------------------------------------------------------------
# Load breakpoint table once
def load_breakpoints() -> pd.DataFrame:
    bp = pd.read_csv(BP_FILE)
    bp["key"] = (
        bp["organism"].str.lower().str.strip()
        + "||"
        + bp["drug"].str.lower().str.strip()
    )
    return bp.set_index("key")

BREAKPOINTS = load_breakpoints()

# ---------------------------------------------------------------------
def mic_to_sir(row) -> str | pd.NA:
    """Convert MIC to S/I/R using EUCAST 2025 breakpoints."""
    key = f"{row.organism.lower().strip()}||{row.drug.lower().strip()}"
    if key not in BREAKPOINTS.index:
        return pd.NA

    s_max = BREAKPOINTS.at[key, "susceptible_MIC_max"]
    r_min = BREAKPOINTS.at[key, "resistant_MIC_min"]
    mic   = row.mic

    if pd.isna(mic):
        return pd.NA
    if mic <= s_max:
        return "S"
    if pd.notna(r_min) and mic >= r_min:
        return "R"
    return "I"

# ---------------------------------------------------------------------
def ingest_one_excel(path: pathlib.Path, vendor: str):
    print(f"[+] {vendor}: reading {path.name}")
    df = pd.read_excel(path, engine="openpyxl")

    # Minimal canonical rename map — extend as needed
    rename = {
        "Isolate ID": "isolate_id",
        "Isolate_ID": "isolate_id",
        "Species":    "organism",
        "Organism":   "organism",
        "Country":    "country",
        "Year":       "year",
        "MIC":        "mic",
        "Drug":       "drug",
        "Antibiotic": "drug",
    }
    df = df.rename(columns=rename, errors="ignore")

    # Ensure required columns exist
    required = {"organism", "drug", "mic", "country", "year"}
    missing  = required - set(df.columns)
    if missing:
        raise ValueError(f"{vendor}: missing columns {missing}")

    # Apply breakpoint conversion
    df["susceptibility"] = df.apply(mic_to_sir, axis=1)

    # Output parquet
    PROC_FOLDER.mkdir(exist_ok=True)
    out_file = PROC_FOLDER / f"{vendor.lower()}.parquet"
    pq.write_table(pa.Table.from_pandas(df), out_file)
    print(f"[+] {vendor}: wrote {len(df):,} rows to {out_file}")

# ---------------------------------------------------------------------
def main():
    PROC_FOLDER.mkdir(exist_ok=True)
    excel_files = sorted(RAW_FOLDER.glob("**/*.xlsx"))
    if not excel_files:
        raise SystemExit("No .xlsx files found in data/raw/")

    for f in excel_files:
        vendor = f.parts[-2]  # folder name above the file
        ingest_one_excel(f, vendor)

if __name__ == "__main__":
    main()
