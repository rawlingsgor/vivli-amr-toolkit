import pathlib, re, pandas as pd, pyarrow as pa, pyarrow.parquet as pq

ROOT        = pathlib.Path(__file__).resolve().parents[2]
RAW_FOLDER  = ROOT / "data" / "raw"
PROC_FOLDER = ROOT / "data" / "processed"

MIC_COL_RX  = re.compile(r"(.*)(?:_mic$| mic$|_MIC$)", re.IGNORECASE)

def reshape_file(path: pathlib.Path, vendor: str):
    print(f"[+] {vendor}: reshaping {path.name}")
    df = pd.read_excel(path, engine="openpyxl")

    # detect MIC columns
    mic_cols = [c for c in df.columns if MIC_COL_RX.search(str(c))]
    if not mic_cols:
        print(f"[!] {vendor}: no MIC columns found – skipped")
        return 0

    # context columns (keep those that exist)
    context_cols = [c for c in ["Isolate ID", "Species", "Organism", "Country", "Year"]
                    if c in df.columns]

    long = (
        df[context_cols + mic_cols]
        .melt(id_vars=context_cols, value_vars=mic_cols,
              var_name="drug_raw", value_name="mic")
        .dropna(subset=["mic"])
    )

    # harmonise column names
    rename = {
        "Isolate ID": "isolate_id",
        "Species":    "organism",
        "Organism":   "organism",
        "Country":    "country",
        "Year":       "year",
    }
    long = long.rename(columns=rename, errors="ignore")

    # extract drug name from column header
    long["drug"] = long["drug_raw"].str.replace(r"(_mic$| mic$|_MIC$)", "", regex=True).str.strip()
    long = long.drop(columns="drug_raw")

    long["drug"] = long["drug_raw"].str.replace(r"(_mic$| mic$|_MIC$)", "", regex=True).str.strip()
    long = long.drop(columns="drug_raw")

    # clean MIC values
    long["mic"] = pd.to_numeric(long["mic"], errors="coerce")
    long = long.dropna(subset=["mic"])

    out = PROC_FOLDER / f"{vendor.lower()}_long.parquet"
    PROC_FOLDER.mkdir(exist_ok=True)

    out = PROC_FOLDER / f"{vendor.lower()}_long.parquet"
    PROC_FOLDER.mkdir(exist_ok=True)
    pq.write_table(pa.Table.from_pandas(long), out)
    print(f"[+] {vendor}: wrote {len(long):,} rows to {out}")
    return len(long)

def main():
    total = 0
    for xlsx in sorted(RAW_FOLDER.glob("**/*.xlsx")):
        vendor = xlsx.parts[-2]
        total += reshape_file(xlsx, vendor)
    if total == 0:
        print("No rows reshaped – check MIC column patterns.")
    else:
        print(f"[✓] Total reshaped rows: {total:,}")

if __name__ == "__main__":
    main()

