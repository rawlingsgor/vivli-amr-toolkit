"""
src/cleaning/atlas_clean.py
===========================

Cleaner for Pfizer-ATLAS Antibiotics and Vivli-Sentry Antifungals workbooks.
Transforms wide Excel sheets into tidy long-format DataFrames ready for EDA
and predictive modelling, meeting VIVLI AMR Challenge robustness standards.
"""

from __future__ import annotations
import os
import re
import unicodedata
from pathlib import Path
import pandas as pd
import numpy as np
from pandas.api.types import is_numeric_dtype

# ---------------------------------------------------------------------------
# Directories (adjust ROOT if project layout differs)
# ---------------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw"
PROC_DIR = ROOT / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FLAG_SUFFIXES = ("_i", "_flag", "_interpretation")
META_COLS = {
    "isolate_id", "vivli_no", "uid",
    "pathogen", "species", "organismname",
    "country", "region", "state",
    "year", "yearcollected",
    "gender", "age", "age_group",
    "study", "bodylocation", "source",
}
FLAG_MAP = {
    "S": "S", "SUSCEPTIBLE": "S",
    "R": "R", "RESISTANT": "R", "NS": "R",
    "I": "I", "INTERMEDIATE": "I",
    "U": pd.NA,  # unknown / indeterminate
}

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def ascii_snake(col: str) -> str:
    """ASCII-strip + snake_case."""
    col = unicodedata.normalize("NFKD", col).encode("ascii", "ignore").decode()
    col = re.sub(r"[^0-9a-zA-Z]+", "_", col).strip("_").lower()
    return col


def parse_mic(series: pd.Series) -> pd.Series:
    """Parse MIC strings (commas, inequalities) to float64."""
    return (
        series.astype("string")
        .str.replace(",", ".", regex=False)
        .str.extract(r"([0-9.]+)")
        .astype("float64")
    )


def iqr_trim(group: pd.Series) -> pd.Series:
    """Remove outliers via Tukey IQR per drug-year."""
    q1, q3 = group.quantile([0.25, 0.75])
    lo, hi = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    return group.where(group.between(lo, hi))


def is_probably_mic_col(series: pd.Series, sample_size: int = 100) -> bool:
    """
    Heuristic: Check if column is likely MIC by sampling values and
    testing if they are numeric or parseable numeric with inequalities.
    """
    sample = series.dropna().astype("string").head(sample_size)
    if sample.empty:
        return False

    def parseable(val):
        val = val.strip().replace(",", ".").lstrip("><=")
        try:
            float(val)
            return True
        except ValueError:
            return False

    parseable_count = np.sum(sample.apply(parseable))
    return parseable_count / len(sample) > 0.5


def load_first_sheet(xlsx: Path) -> tuple[pd.DataFrame, str]:
    """Read first worksheet of an Excel file."""
    if not xlsx.exists():
        raise FileNotFoundError(xlsx)
    xl = pd.ExcelFile(xlsx)
    sheet = xl.sheet_names[0]
    return pd.read_excel(xlsx, sheet_name=sheet), sheet


# ---------------------------------------------------------------------------
# Core cleaner
# ---------------------------------------------------------------------------

def clean_atlas(xlsx: str | Path) -> pd.DataFrame:
    xlsx = Path(xlsx) 

    """Return cleaned long DataFrame from an ATLAS workbook."""
    df, sheet = load_first_sheet(xlsx)

    # 1. Header normalisation
    df.rename(columns=ascii_snake, inplace=True)

    # 2. Key-field standardisation
    df.rename(
        columns={
            "vivli_no": "isolate_id",
            "uid": "isolate_id",
            "species": "pathogen",
            "organismname": "pathogen",
            "yearcollected": "year",
        },
        inplace=True,
    )

    # 3. Drop exact duplicates
    df = df.drop_duplicates()

    # 4. Identify flag & MIC columns
    flag_cols = [c for c in df.columns if c.endswith(FLAG_SUFFIXES)]
    mic_cols = [
        c for c in df.columns
        if c not in META_COLS and c not in flag_cols and
        (is_numeric_dtype(df[c]) or is_probably_mic_col(df[c]))
    ]
    if not mic_cols:
        raise ValueError(f"No MIC columns detected in sheet '{sheet}' of {xlsx.name}")

    # 5. Parse MIC values
    for col in mic_cols:
        df[col] = parse_mic(df[col])

    # 6. Remove rows without country
    df = df[df["country"].notna()]

    # 7. Melt to long format
    id_cols = sorted(set(df.columns) - set(mic_cols) - set(flag_cols))
    mic_long = df.melt(
        id_vars=id_cols, value_vars=mic_cols,
        var_name="drug", value_name="mic_value"
    )
    flag_long = df.melt(
        id_vars=id_cols, value_vars=flag_cols,
        var_name="drug_flag", value_name="sir_flag"
    )
    flag_long["drug"] = flag_long["drug_flag"].str.replace(
        r"(_i|_flag|_interpretation)$", "", regex=True
    )
    df_long = mic_long.merge(
        flag_long[["isolate_id", "drug", "sir_flag"]],
        on=["isolate_id", "drug"], how="left"
    )

    # 8. Normalise & validate flags
    df_long["sir_flag"] = (
        df_long["sir_flag"].astype("string").str.strip().str.upper()
    )
    unknown = set(df_long["sir_flag"].dropna().unique()) - set(FLAG_MAP)
    if unknown:
        raise ValueError(f"Unknown flags {unknown} in {xlsx.name}")

    df_long["sir_flag"] = df_long["sir_flag"].map(FLAG_MAP)

    # 9. Binary resistance label
    df_long["resistant"] = df_long["sir_flag"].map({"R": 1, "S": 0})

    # 10. Outlier filtering
    df_long["mic_value"] = (
        df_long.groupby(["drug", "year"], group_keys=False)["mic_value"]
        .apply(iqr_trim)
    )
    df_long = df_long.dropna(subset=["mic_value"])

    # 11. Final dtypes
    df_long["mic_value"] = df_long["mic_value"].astype("float64")
    df_long["resistant"] = df_long["resistant"].astype("Int8")
    df_long[["drug", "sir_flag", "pathogen", "country"]] = (
        df_long[["drug", "sir_flag", "pathogen", "country"]].astype("string")
    )

    return df_long.reset_index(drop=True)


def save_parquet(df: pd.DataFrame, tag: str) -> Path:
    path = PROC_DIR / f"atlas_{tag}_long.parquet"
    df.to_parquet(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Script entrypoint
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ANTIBIOTICS_XLSX = RAW_DIR / "ATLAS_Antibiotics" / "2025_03_11 atlas_antibiotics.xlsx"
    ANTIFUNGALS_XLSX = RAW_DIR / "ATLAS_Antifungals" / "vivli_sentry_2010_2023.xlsx"

    for xlsx, tag in [(ANTIBIOTICS_XLSX, "antibiotics"), (ANTIFUNGALS_XLSX, "antifungals")]:
        print(f"\n• Cleaning {xlsx.name}")
        df_out = clean_atlas(xlsx)
        out_path = save_parquet(df_out, tag)
        print(f"  Rows: {len(df_out):,}  Cols: {df_out.shape[1]}")
        print(f"  Saved → {out_path}")
        print(df_out.head(3))
