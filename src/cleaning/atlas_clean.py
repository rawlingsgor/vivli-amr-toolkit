"""
Atlas Excel → long-format cleaned DataFrame
==========================================

Key features
------------
✓ Header ASCII / snake_case normalisation
✓ Schema harmonisation (isolate_id, pathogen, year, country …)
✓ Duplicate-row removal
✓ Robust MIC parser (handles commas, inequalities)
✓ Wide→long melt **without** relying on “_mic” suffix
✓ Rogue S/I/R flag guard (NS, U, lowercase) – hard-fail on unknown
✓ Binary resistance label (R = 1, S = 0, I/NaN = missing)
✓ Drug-year IQR outlier filter
✓ MIC kept as float64 until modelling
✓ Fixture smoke-test CLI

This version removes the erroneous `.drop(columns=["drug_flag"])`
and fixes the negative-dimension melt bug.
"""

from __future__ import annotations
import os
import re
import unicodedata
from pathlib import Path

import pandas as pd
from pandas.api.types import is_numeric_dtype

# ── Helpers ────────────────────────────────────────────────────────────────
META_COLS = {
    "isolate_id", "vivli_no", "uid",
    "pathogen", "species", "organismname",
    "country", "region", "state",
    "year", "yearcollected",
    "gender", "age", "age_group",
    "study", "bodylocation", "source",
}

FLAG_SUFFIXES = ("_i", "_flag", "_interpretation")


def ascii_snake(col: str) -> str:
    col = unicodedata.normalize("NFKD", col).encode("ascii", "ignore").decode()
    col = re.sub(r"[^0-9a-zA-Z]+", "_", col).strip("_").lower()
    return col


def parse_mic(series: pd.Series) -> pd.Series:
    """Handle decimal commas and inequalities."""
    return (
        series.astype("string")
        .str.replace(",", ".", regex=False)
        .str.extract(r"([0-9.]+)")
        .astype("float64")
    )


def iqr_trim(group: pd.Series) -> pd.Series:
    q1, q3 = group.quantile([0.25, 0.75])
    lo, hi = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    return group.where(group.between(lo, hi))


def load_excel(path: str | Path) -> tuple[pd.DataFrame, str]:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Atlas file not found: {path}")
    xls = pd.ExcelFile(path)
    sheet = xls.sheet_names[0]
    return pd.read_excel(path, sheet_name=sheet), sheet


# ── Main cleaning routine ───────────────────────────────────────────────────
def clean_atlas(path: str | Path) -> pd.DataFrame:
    df, sheet = load_excel(path)

    # 1. Header normalisation
    df.rename(columns=ascii_snake, inplace=True)

    # 2. Harmonise key field names
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

    # 3. Remove exact duplicates
    df = df.drop_duplicates()

    # 4. Identify flag & MIC columns
    flag_cols = [c for c in df.columns if c.endswith(FLAG_SUFFIXES)]
    mic_cols = [
        c
        for c in df.columns
        if c not in META_COLS and c not in flag_cols and is_numeric_dtype(df[c])
    ]
    if not mic_cols:
        raise ValueError(
            f"No MIC columns detected in sheet '{sheet}'. Check column naming."
        )

    # 5. Parse MIC values
    for col in mic_cols:
        df[col] = parse_mic(df[col])

    # 6. Drop rows without country
    df = df[df["country"].notna()]

    # 7. Melt MIC and flag tables
    id_cols = sorted(set(df.columns) - set(mic_cols) - set(flag_cols))

    mic_long = df.melt(
        id_vars=id_cols, value_vars=mic_cols, var_name="drug", value_name="mic_value"
    )

    flag_long = df.melt(
        id_vars=id_cols, value_vars=flag_cols, var_name="drug_flag", value_name="sir_flag"
    )
    flag_long["drug"] = flag_long["drug_flag"].str.replace(
        r"(_i|_flag|_interpretation)$", "", regex=True
    )

    # correct merge (no drop of non-existent column)
    df_long = mic_long.merge(
        flag_long[["isolate_id", "drug", "sir_flag"]],
        on=["isolate_id", "drug"],
        how="left",
    )

    # 8. Rogue flag defence
    df_long["sir_flag"] = (
        df_long["sir_flag"].astype("string").str.strip().str.upper()
    )

    ACCEPTED_MAP = {
        "S": "S",
        "SUSCEPTIBLE": "S",
        "R": "R",
        "RESISTANT": "R",
        "NS": "R",
        "I": "I",
        "INTERMEDIATE": "I",
    }
    unknown = set(df_long["sir_flag"].dropna().unique()) - set(ACCEPTED_MAP)
    if unknown:
        raise ValueError(f"Unknown S/I/R flag(s): {unknown}")

    df_long["sir_flag"] = df_long["sir_flag"].map(ACCEPTED_MAP)

    # 9. Binary label
    df_long["resistant"] = df_long["sir_flag"].map({"R": 1, "S": 0})

    # 10. Drug-year IQR outlier filter
    df_long["mic_value"] = (
        df_long.groupby(["drug", "year"], group_keys=False)["mic_value"]
        .apply(iqr_trim)
    )
    df_long = df_long.dropna(subset=["mic_value"])

    # 11. Final dtypes
    df_long = df_long.reset_index(drop=True)
    df_long["mic_value"] = df_long["mic_value"].astype("float64")
    df_long["resistant"] = df_long["resistant"].astype("Int8")
    df_long[["drug", "sir_flag", "pathogen", "country"]] = df_long[
        ["drug", "sir_flag", "pathogen", "country"]
    ].astype("string")

    return df_long


# ── CLI smoke-test for fixtures ──────────────────────────────────────────────
if __name__ == "__main__":  # pragma: no cover
    root = Path(__file__).resolve().parents[2]
    for fixture in [
        "tests/fixtures/atlas_antibiotics_fixture.xlsx",
        "tests/fixtures/atlas_antifungals_fixture.xlsx",
    ]:
        p = root / fixture
        print(f"\n• Cleaning {p.name}")
        df_ok = clean_atlas(p)
        print("  Rows:", len(df_ok), " Cols:", df_ok.shape[1])
        print(df_ok.head(3))
