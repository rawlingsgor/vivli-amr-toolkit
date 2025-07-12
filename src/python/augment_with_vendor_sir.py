#!/usr/bin/env python3
"""
augment_with_vendor_sir.py

• Recursively scans every Excel/CSV in data/raw
• Extracts vendor S/I/R calls (*_I / *_S / *_R)
• Dedupes on (collection_number, drug, year)
• Merges onto master.parquet
• Converts vendor wording → single letter
• Relabels that letter using EUCAST break-points
"""

import re
from pathlib import Path
import numpy as np
import pandas as pd
from apply_breakpoints import apply_eucast   # EUCAST helper

RAW_DIR    = Path("data/raw")
MASTER_IN  = Path("data/processed/master.parquet")
VENDOR_OUT = Path("data/processed/vendor_sir.parquet")
MERGED_OUT = MASTER_IN.parent / "master_with_vendor_sir.parquet"
MERGE_KEYS = ["collection_number", "drug", "year"]

# ───────────── helpers ──────────────────────────────────────────────
def normalize(text: str) -> str:
    return re.sub(r"\s+", "_", str(text)).lower()

def drop_dup_cols(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    if df.columns.duplicated().any():
        print("[!] Dropping duplicated columns from", tag)
        df = df.loc[:, ~df.columns.duplicated()]
    return df

def read_any(path: Path) -> dict:
    suf = path.suffix.lower()
    if suf in [".xlsx", ".xls"]:
        return pd.read_excel(path, sheet_name=None, header=0)
    if suf == ".csv":
        return {"Sheet1": pd.read_csv(path, header=0)}
    return {}

def vendor_to_letter(x):
    if pd.isna(x):
        return None
    ch = str(x).strip().upper()[0]
    return ch if ch in {"S", "I", "R"} else None

# ───────────── main ────────────────────────────────────────────────
if __name__ == "__main__":
    # 1️⃣  load + tidy master
    master = pd.read_parquet(MASTER_IN)
    master.columns = [normalize(c) for c in master.columns]
    master = drop_dup_cols(master, "master")

    if "isolate" in master.columns:
        m = master["collection_number"].isna() & master["isolate"].notna()
        master.loc[m, "collection_number"] = master.loc[m, "isolate"]

    for k in MERGE_KEYS:
        if k not in master.columns:
            master[k] = np.nan

    # 2️⃣  extract vendor S/I/R
    parts = []
    for path in RAW_DIR.rglob("*.*"):
        if path.suffix.lower() not in {".xlsx", ".xls", ".csv"}:
            continue

        for sheet_name, raw in read_any(path).items():
            raw.columns = [normalize(c) for c in raw.columns]
            raw = drop_dup_cols(raw, f"{path.name}:{sheet_name}")

            if "isolate_id" in raw.columns and "collection_number" not in raw.columns:
                raw.rename(columns={"isolate_id": "collection_number"}, inplace=True)

            raw["vendor"] = path.stem.lower().replace(" ", "_")
            if "study_year" in raw.columns and "year" not in raw.columns:
                raw.rename(columns={"study_year": "year"}, inplace=True)
            for k in ["vendor", "collection_number", "year"]:
                if k not in raw.columns:
                    raw[k] = np.nan

            sir_cols = [c for c in raw.columns if re.search(r"_[isr]$", c)]
            if not sir_cols:
                continue

            long = raw.melt(
                id_vars=["vendor", "collection_number", "year"],
                value_vars=sir_cols,
                var_name="cls_i_drug",
                value_name="sir_vendor_raw",
            )
            long["drug"] = long["cls_i_drug"].str.replace(r"_[isr]$", "", flags=re.I, regex=True)
            parts.append(long[["collection_number", "drug", "year", "sir_vendor_raw"]])

    vs = (pd.concat(parts, ignore_index=True)
          if parts else pd.DataFrame(columns=MERGE_KEYS + ["sir_vendor_raw"]))

    # 3️⃣  dedupe & normalise year
    vs = vs.drop_duplicates(subset=MERGE_KEYS)
    for df in (master, vs):
        df["year"] = (pd.to_numeric(df["year"], errors="coerce")
                        .round()
                        .astype("Int64")
                        .astype(str))

    # 4️⃣  write vendor_sir table
    VENDOR_OUT.parent.mkdir(parents=True, exist_ok=True)
    vs.to_parquet(VENDOR_OUT, index=False)
    print("✓ wrote", VENDOR_OUT)

    # 5️⃣  merge
    for k in MERGE_KEYS:
        master[k] = master[k].astype(str)
        vs[k]     = vs[k].astype(str)

    aug = master.merge(vs, on=MERGE_KEYS, how="left", validate="many_to_one")

    # 6️⃣  convert wording → letter and relabel to EUCAST
    aug["sir_vendor_raw"] = aug["sir_vendor_raw"].map(vendor_to_letter)
    aug["sir_vendor"] = aug.apply(
        lambda r: apply_eucast(r["mic"], r["cls_i_organism"], r["drug"]),
        axis=1
    )

    # 7️⃣  save merged master
    aug.to_parquet(MERGED_OUT, index=False)
    print("✓ wrote", MERGED_OUT)

