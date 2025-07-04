#!/usr/bin/env python3
"""
clean_and_sir.py
----------------
• Load every *_long.parquet          (adds “vendor” from the filename)
• Normalise text, map raw → CLSI canonical names (docs/mapping.csv)
• Attach CLSI 2024 break-points, fall back to EUCAST 2025 where missing
• Classify each MIC -> S / I / R / U
• **No de-duplication**  – keep full row set for modelling
• Write unified data/processed/master.parquet
"""
from pathlib import Path
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
DATA_FOLDER   = Path("data/processed")
MASTER_OUT    = DATA_FOLDER / "master.parquet"
MAPPING_FILE  = Path("docs/mapping.csv")
CLSI_FILE     = Path("docs/clsi_2024_breakpoints.xlsx")
EUCAST_FILE   = Path("docs/eucast_2025_breakpoints.csv")

# ---------------------------------------------------------------------
# 1. Load & tag vendor
# ---------------------------------------------------------------------
def load_data() -> pd.DataFrame:
    parts = []
    for p in DATA_FOLDER.glob("*_long.parquet"):
        df = pd.read_parquet(p)
        df["vendor"] = p.stem.replace("_long", "")
        print(f"[+] loaded {len(df):,} rows from {p.name}")
        parts.append(df)
    return pd.concat(parts, ignore_index=True)

# ---------------------------------------------------------------------
# 2. Normalise organism / drug text
# ---------------------------------------------------------------------
def normalise(df: pd.DataFrame) -> pd.DataFrame:
    df["organism"] = (
        df["organism"]
        .str.lower().str.strip()
        .str.replace(r",.*$", "", regex=True)       # drop ", mssa", etc.
    )
    df["drug"] = (
        df["drug"]
        .str.lower().str.strip()
        .str.replace(r"_mic$", "", regex=True)
        .str.replace("+", "/", regex=False)         # piperacillin+tazo → piperacillin/tazo
    )
    return df

# ---------------------------------------------------------------------
# 3. Raw → canonical CLSI names
# ---------------------------------------------------------------------
def map_raw(df: pd.DataFrame) -> pd.DataFrame:
    mapping = (
        pd.read_csv(MAPPING_FILE, dtype=str)
          .fillna("")
          .rename(columns={
              "raw_organism":"organism",
              "raw_drug":"drug",
              "canonical_organism":"cls_i_organism",
              "canonical_drug":"cls_i_drug"
          })
    )
    return df.merge(
        mapping[["organism","drug","cls_i_organism","cls_i_drug"]],
        on=["organism","drug"],
        how="left"
    )

# ---------------------------------------------------------------------
# 4. Attach CLSI + EUCAST break-points
# ---------------------------------------------------------------------
def attach_breakpoints(df: pd.DataFrame) -> pd.DataFrame:
    # ---- CLSI ----
    bp_c = pd.read_excel(CLSI_FILE, sheet_name="MIC BP Table", engine="openpyxl")
    s_col = next(c for c in bp_c.columns if "CLSI" in c and ("<=" in c or "≤" in c) and "S" in c)
    r_col = next(c for c in bp_c.columns if "CLSI" in c and (">=" in c or "≥" in c) and "R" in c)
    bp_c = (bp_c
        .rename(columns={
            "Organism/Organism Group":"cls_i_organism",
            "DRUG NAME":"cls_i_drug",
            s_col:"s_max",
            r_col:"r_min"
        })[["cls_i_organism","cls_i_drug","s_max","r_min"]]
        .assign(source="CLSI")
    )

    # ---- EUCAST ----
    bp_e = (pd.read_csv(EUCAST_FILE)
        .rename(columns={
            "organism":"cls_i_organism",
            "drug":"cls_i_drug",
            "susceptible_MIC_max":"s_max",
            "resistant_MIC_min":"r_min"
        })[["cls_i_organism","cls_i_drug","s_max","r_min"]]
        .assign(source="EUCAST")
    )

    bp_all = (pd.concat([bp_c, bp_e], ignore_index=True)
                .drop_duplicates(subset=["cls_i_organism","cls_i_drug","source"]))

    bp_all["s_max"] = pd.to_numeric(bp_all["s_max"], errors="coerce")
    bp_all["r_min"] = pd.to_numeric(bp_all["r_min"], errors="coerce")

    return df.merge(bp_all, on=["cls_i_organism","cls_i_drug"], how="left")

# ---------------------------------------------------------------------
# 5. Classify S / I / R / U
# ---------------------------------------------------------------------
def assign_sir(df: pd.DataFrame) -> pd.DataFrame:
    def _classify(row):
        mic, s_max, r_min = row["mic"], row["s_max"], row["r_min"]
        if pd.isna(mic) or pd.isna(s_max) or pd.isna(r_min):
            return "U"
        if mic <= s_max:  return "S"
        if mic >= r_min:  return "R"
        return "I"

    df["sir"] = df.apply(_classify, axis=1)
    labelled = df["sir"].notna().sum()
    print(f"[=] Rows with S/I/R/U label: {labelled:,} / {len(df):,}")
    return df

# ---------------------------------------------------------------------
# 6. Write master.parquet (no dedupe)
# ---------------------------------------------------------------------
def write_master(df: pd.DataFrame) -> None:
    DATA_FOLDER.mkdir(exist_ok=True)
    df.to_parquet(MASTER_OUT, index=False)
    print(f"[=] Total rows fed to modelling: {len(df):,}")
    print(f"[✓] Wrote {MASTER_OUT}")

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
if __name__ == "__main__":
    df = load_data()
    df = normalise(df)
    df = map_raw(df)
    df = attach_breakpoints(df)
    df = assign_sir(df)
    write_master(df)

