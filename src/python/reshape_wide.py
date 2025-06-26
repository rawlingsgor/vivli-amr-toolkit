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

import pathlib, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
from collections import defaultdict

ROOT        = pathlib.Path(__file__).resolve().parents[2]
RAW_FOLDER  = ROOT / "data" / "raw"
PROC_FOLDER = ROOT / "data" / "processed"

# context columns we never melt
CONTEXT_COLS = {
    "Isolate ID",   # original isolate identifier column
    "Isolate",      # some files name it this way
    "Age",          # drop age values entirely
    "Species",
    "Organism",
    "Country",
    "Year"
}

def detect_numeric_mic_cols(df: pd.DataFrame) -> list[str]:
    """Return column names that look like numeric MIC values."""
    numeric_cols = []
    for col in df.columns:
        key = str(col).lower().strip()
        # skip context columns by lowercase match
        if key in {"isolate id", "isolate", "age", "species", "organism", "country", "year"}:
            continue
        # skip paired categorical “_i” columns
        if key.endswith("_i"):
            continue
        # test numeric content
        s = pd.to_numeric(df[col], errors="coerce")
        if (s.notna().sum() / max(len(s), 1)) >= 0.70:
            numeric_cols.append(col)
    return numeric_cols

def reshape_file(path: pathlib.Path, vendor: str) -> int:
    print(f"[+] {vendor}: reshaping {path.name}")
    df = pd.read_excel(path, engine="openpyxl")
    mic_cols = detect_numeric_mic_cols(df)
    if not mic_cols:
        print(f"[!] {vendor}: no numeric MIC columns found – skipped")
        return 0

    keep_cols = [c for c in CONTEXT_COLS if c in df.columns]
    long = (
        df[keep_cols + mic_cols]
        .melt(id_vars=keep_cols, value_vars=mic_cols,
              var_name="drug", value_name="mic")
        .dropna(subset=["mic"])
    )

    # harmonise context names
    rename = {
        "Isolate ID": "isolate_id",
        "Species":    "organism",
        "Organism":   "organism",
        "Country":    "country",
        "Year":       "year",
    }
    long = long.rename(columns=rename, errors="ignore")

    # numeric coercion
    long["mic"] = pd.to_numeric(long["mic"], errors="coerce")
    long = long.dropna(subset=["mic"])

    PROC_FOLDER.mkdir(exist_ok=True)
    out = PROC_FOLDER / f"{vendor.lower()}_long.parquet"
    pq.write_table(pa.Table.from_pandas(long), out)
    print(f"[+] {vendor}: wrote {len(long):,} rows to {out}")
    return len(long)

def main():
    total = 0
    vendor_rows = defaultdict(int)
    for xlsx in sorted(RAW_FOLDER.glob("**/*.xlsx")):
        vendor = xlsx.parts[-2]
        rows   = reshape_file(xlsx, vendor)
        vendor_rows[vendor] += rows
        total += rows
    print("\n=== Row summary ===")
    for v, r in vendor_rows.items():
        print(f"{v}: {r:,}")
    print(f"[✓] Total reshaped rows: {total:,}")

if __name__ == "__main__":
    main()

