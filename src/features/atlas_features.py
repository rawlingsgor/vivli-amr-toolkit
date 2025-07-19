"""
Feature engineering for ATLAS long-format data.

Input  : output of clean_atlas()  (one row = isolate–drug pair)
Output : country-year-pathogen-drug aggregates + rolling trends
"""

import pandas as pd

META = ["country", "year", "pathogen", "drug"]

def unify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure 'pathogen' and 'year' names are present across both datasets."""
    col_map = {"species": "pathogen", "organismname": "pathogen",
               "yearcollected": "year"}
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df})
    return df

# ────────────────────────────────────────────────────────────
# 1. % Resistant & isolate counts
# ────────────────────────────────────────────────────────────
def resistance_rate(df: pd.DataFrame) -> pd.DataFrame:
    flags = df.dropna(subset=["resistant"])
    agg = (flags.groupby(META)["resistant"]
                 .agg(total_tests="count", n_resistant="sum")
                 .reset_index())
    agg["pct_resistant"] = agg["n_resistant"] / agg["total_tests"]
    return agg

def isolate_counts(df: pd.DataFrame) -> pd.DataFrame:
    return (df.groupby(["country", "year", "pathogen"])["isolate_id"]
              .nunique().reset_index(name="n_isolates"))

# ────────────────────────────────────────────────────────────
# 2. Rolling 3-year average of %-resistant
# ────────────────────────────────────────────────────────────
def add_rolling_trend(rates: pd.DataFrame, window:int=3) -> pd.DataFrame:
    rates = rates.sort_values(META)
    rates["rolling_pct_resistant"] = (
        rates.groupby(["country", "pathogen", "drug"])["pct_resistant"]
             .transform(lambda s: s.rolling(window, min_periods=1).mean())
    )
    return rates

# ────────────────────────────────────────────────────────────
# 3. Driver function
# ────────────────────────────────────────────────────────────
def build_features(df_long: pd.DataFrame) -> pd.DataFrame:
    df = unify_columns(df_long.copy())
    rates  = resistance_rate(df)
    rates  = add_rolling_trend(rates)
    counts = isolate_counts(df)
    feat   = pd.merge(rates, counts,
                      on=["country", "year", "pathogen"], how="left")
    return feat

# ────────────────────────────────────────────────────────────
# 4. CLI smoke-test
# ────────────────────────────────────────────────────────────
if __name__ == "__main__":
    from pathlib import Path
    from src.cleaning.atlas_clean import clean_atlas

    root = Path(__file__).resolve().parents[2]  # project root
    abx = clean_atlas(root / "data/raw/ATLAS_Antibiotics/2025_03_11 atlas_antibiotics.xlsx")
    afg = clean_atlas(root / "data/raw/ATLAS_Antifungals/vivli_sentry_2010_2023.xlsx")

    print("Antibiotics features:", build_features(abx).head(), "\n")
    print("Antifungals features:", build_features(afg).head())
