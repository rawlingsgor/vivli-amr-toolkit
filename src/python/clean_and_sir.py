"""
clean_and_sir.py
----------------
Merge all *_long.parquet files, normalise names, attach EUCAST-2025
breakpoints, convert MIC → S/I/R, drop duplicates, write master.parquet.
"""

import pathlib, re, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
from collections import Counter

# ---------------------------------------------------------------------
ROOT   = pathlib.Path(__file__).resolve().parents[2]
PROC   = ROOT / "data" / "processed"
BP_CSV = ROOT / "docs" / "eucast_2025_breakpoints.csv"

# simple synonym maps (extend as needed)
ORGANISM_MAP = {
    "e. coli": "escherichia coli",
    "k. pneumoniae": "klebsiella pneumoniae",
}
DRUG_MAP = {
    "cip": "ciprofloxacin",
    "mem": "meropenem",
}

# ---------------------------------------------------------------------
def load_breakpoints() -> pd.DataFrame:
    bp = pd.read_csv(BP_CSV)
    bp["organism"] = bp["organism"].str.lower().str.strip()
    bp["drug"]     = bp["drug"].str.lower().str.strip()
    bp["key"]      = bp["organism"] + "||" + bp["drug"]
    return bp.set_index("key")

BP = load_breakpoints()

def mic_to_sir(row):
    key = f"{row.organism}||{row.drug}"
    if key not in BP.index:
        return pd.NA
    s_max = BP.at[key, "susceptible_MIC_max"]
    r_min = BP.at[key, "resistant_MIC_min"]
    mic   = row.mic
    if pd.isna(mic):
        return pd.NA
    if mic <= s_max:
        return "S"
    if pd.notna(r_min) and mic >= r_min:
        return "R"
    return "I"

# ---------------------------------------------------------------------
def normalise(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # lowercase + strip + remove unwanted suffixes
    df["organism"] = (
        df["organism"]
        .str.lower().str.strip()
        .str.replace(r"[-].*$", "", regex=True)             # drop complex suffixes
        .replace(ORGANISM_MAP)
    )

    # lowercase, strip, drop trivial columns, strip "_mic"
    df["drug"] = (
        df["drug"]
        .str.lower().str.strip()
        .replace({"isolate": None, "age": None})             # drop these
        .dropna()
        .str.replace(r"_mic$", "", regex=True)
        .replace({
            **DRUG_MAP,
            "caz": "ceftazidime",
            "fep": "cefepime",
            "gm":  "gentamicin",
            "mem": "meropenem",
            "tzp": "piperacillin+tazobactam",
            "lvx": "levofloxacin",
        })
    )

    # ensure isolate_id exists
    if "isolate_id" not in df.columns:
        df["isolate_id"] = pd.RangeIndex(start=1, stop=len(df)+1)

    # numeric MIC
    df["mic"] = pd.to_numeric(df["mic"], errors="coerce")

    return df
        

# ---------------------------------------------------------------------
def main():
    long_files = PROC.glob("*_long.parquet")
    parts      = []
    for f in long_files:
        df = pq.read_table(f).to_pandas()
        print(f"[+] loaded {len(df):,} rows from {f.name}")
        parts.append(df)
    raw = pd.concat(parts, ignore_index=True)

    print(f"[=] total concatenated rows: {len(raw):,}")

    clean = normalise(raw)
    clean["sir"] = clean.apply(mic_to_sir, axis=1)
    before = len(clean)
    clean  = clean.dropna(subset=["sir"])
    after_label = len(clean)

    # drop exact duplicates
    dedup_cols = ["isolate_id", "organism", "drug", "country", "year", "mic", "sir"]
    clean = clean.drop_duplicates(subset=dedup_cols)
    after_dedup = len(clean)

    # write master parquet
    out = PROC / "master.parquet"
    pq.write_table(pa.Table.from_pandas(clean), out)

    # summary
    print("\n=== Cleaning summary ===")
    print(f"Rows after concat:        {len(raw):,}")
    print(f"Rows with S/I/R label:    {after_label:,}  (-{before-after_label:,})")
    print(f"Rows after de-duplication:{after_dedup:,}  (-{after_label-after_dedup:,})")
    print(f"[✓] Wrote {out}")

if __name__ == "__main__":
    main()

