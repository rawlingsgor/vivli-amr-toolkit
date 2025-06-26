import pathlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT         = pathlib.Path(__file__).resolve().parents[2]
RAW_FOLDER   = ROOT / "data" / "raw"
PROC_FOLDER  = ROOT / "data" / "processed"
BP_FILE      = ROOT / "docs" / "eucast_2025_breakpoints.csv"

def load_breakpoints() -> pd.DataFrame:
    bp = pd.read_csv(BP_FILE)
    bp["key"] = bp["organism"].str.lower().str.strip() + "||" + bp["drug"].str.lower().str.strip()
    return bp.set_index("key")

BREAKPOINTS = load_breakpoints()

def mic_to_sir(row):
    key = f"{row.organism.lower().strip()}||{row.drug.lower().strip()}"
    if key not in BREAKPOINTS.index:
        return pd.NA
    s_max = BREAKPOINTS.at[key, "susceptible_MIC_max"]
    r_min = BREAKPOINTS.at[key, "resistant_MIC_min"]
    mic   = row.mic
    if pd.isna(mic): return pd.NA
    if mic <= s_max: return "S"
    if pd.notna(r_min) and mic >= r_min: return "R"
    return "I"

def ingest_one_excel(path, vendor):
    print(f"[+] {vendor}: reading {path.name}")
    df = pd.read_excel(path, engine="openpyxl")
    rename = {
        "Isolate ID":"isolate_id","Isolate_ID":"isolate_id",
        "Species":"organism","Organism":"organism",
        "Country":"country","Year":"year","MIC":"mic",
        "Drug":"drug","Antibiotic":"drug",
    }
    df = df.rename(columns=rename, errors="ignore")
    required = {"organism","drug","mic","country","year"}
    missing  = required - set(df.columns)
 if missing == {"drug", "mic"}:
        print(f"[!] SKIP {vendor}: wide format (no drug/mic columns)")
        return

    if missing: raise ValueError(f"{vendor}: missing {missing}")
    df["susceptibility"] = df.apply(mic_to_sir, axis=1)
    PROC_FOLDER.mkdir(exist_ok=True)
    out = PROC_FOLDER / f"{vendor.lower()}.parquet"
    pq.write_table(pa.Table.from_pandas(df), out)
    print(f"[+] {vendor}: wrote {len(df):,} rows to {out}")

def main():
    PROC_FOLDER.mkdir(exist_ok=True)
    files = sorted((RAW_FOLDER).glob("**/*.xlsx"))
    if not files: raise SystemExit("No .xlsx files in data/raw/")
    for f in files:
        ingest_one_excel(f, f.parts[-2])

if __name__ == "__main__":
    main()

