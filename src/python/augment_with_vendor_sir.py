#!/usr/bin/env python3
"""
augment_with_vendor_sir.py

Recursively scans every Excel/CSV under data/raw, extracts vendor
S/I/R calls (*_I / *_S / *_R), reshapes to long form, dedupes on
(collection_number, drug, year), and left-merges onto master.parquet.
"""
import re
from pathlib import Path
import numpy as np
import pandas as pd

RAW_DIR    = Path("data/raw")
MASTER_IN  = Path("data/processed/master.parquet")
VENDOR_OUT = Path("data/processed/vendor_sir.parquet")
MERGED_OUT = MASTER_IN.parent / "master_with_vendor_sir.parquet"
MERGE_KEYS = ["collection_number", "drug", "year"]

# ───────────────────────── helpers ──────────────────────────
def normalize(col: str) -> str:
    return re.sub(r"\s+", "_", str(col)).lower()

def drop_dup_cols(df: pd.DataFrame, tag: str) -> pd.DataFrame:
    if df.columns.duplicated().any():
        d = df.columns[df.columns.duplicated()].unique().tolist()
        print(f"[!] Dropping duplicated columns from {tag}: {d}")
        df = df.loc[:, ~df.columns.duplicated()]
    return df

def read_any(path: Path) -> dict:
    suf = path.suffix.lower()
    if suf in [".xlsx", ".xls"]:
        return pd.read_excel(path, sheet_name=None, header=0)
    if suf == ".csv":
        return {"Sheet1": pd.read_csv(path, header=0)}
    return {}

# ───────────────────────── main ──────────────────────────
if __name__ == "__main__":
    # 1) load + tidy master
    master = pd.read_parquet(MASTER_IN)
    master.columns = [normalize(c) for c in master.columns]
    master = drop_dup_cols(master, "master")

    # copy isolate → collection_number
    if "isolate" in master.columns:
        mask = master["collection_number"].isna() & master["isolate"].notna()
        master.loc[mask, "collection_number"] = master.loc[mask, "isolate"]

    for k in MERGE_KEYS:
        if k not in master.columns:
            master[k] = np.nan

    # 2) extract vendor S/I/R
    parts = []
    for path in RAW_DIR.rglob("*.*"):
        if path.suffix.lower() not in [".xlsx", ".xls", ".csv"]:
            continue

        sheets = read_any(path)
        for sname, raw in sheets.items():
            raw.columns = [normalize(c) for c in raw.columns]
            raw = drop_dup_cols(raw, f"{path.name}:{sname}")

            # harmonise id + vendor + year
            if "isolate_id" in raw.columns and "collection_number" not in raw.columns:
                raw.rename(columns={"isolate_id": "collection_number"}, inplace=True)
            raw["vendor"] = path.stem.lower().replace(" ", "_")
            if "study_year" in raw.columns and "year" not in raw.columns:
                raw.rename(columns={"study_year": "year"}, inplace=True)
            for k in ["vendor", "collection_number", "year"]:
                if k not in raw.columns:
                    raw[k] = np.nan

            # interpretive cols
            sir_cols = [c for c in raw.columns if re.search(r"_[isr]$", c)]
            if not sir_cols:
                continue

            long = raw.melt(
                id_vars=["vendor", "collection_number", "year"],
                value_vars=sir_cols,
                var_name="cls_i_drug",
                value_name="sir_vendor",
            )
            long["drug"] = long["cls_i_drug"].str.replace(r"_[isr]$", "", regex=True, flags=re.I)
            parts.append(long[["collection_number", "drug", "year", "sir_vendor"]])

    vs = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=MERGE_KEYS + ["sir_vendor"])

    # 3) dedupe & normalise year
    vs = vs.drop_duplicates(subset=MERGE_KEYS)
    for df in (master, vs):
        df["year"] = (
            pd.to_numeric(df["year"], errors="coerce")
              .round()
              .astype("Int64")
              .astype(str)
        )

    # 4) write vendor_sir
    VENDOR_OUT.parent.mkdir(parents=True, exist_ok=True)
    vs.to_parquet(VENDOR_OUT, index=False)
    print(f"[✓] Wrote {VENDOR_OUT}")

    # 5) merge
    for k in MERGE_KEYS:
        master[k] = master[k].astype(str)
        vs[k]     = vs[k].astype(str)

    aug = master.merge(vs, on=MERGE_KEYS, how="left", validate="many_to_one")

    # consolidate vendor columns
    if "sir_vendor_x" in aug.columns and "sir_vendor_y" in aug.columns:
        aug["sir_vendor"] = aug["sir_vendor_y"].combine_first(aug["sir_vendor_x"])
        aug.drop(["sir_vendor_x", "sir_vendor_y"], axis=1, inplace=True)

    aug.to_parquet(MERGED_OUT, index=False)
    print(f"[✓] Wrote {MERGED_OUT}")

