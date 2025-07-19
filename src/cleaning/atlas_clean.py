# src/cleaning/atlas_clean.py

import os
import pandas as pd
import sys

def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Lowercase, strip, and snake_case all column names.
    """
    df = df.rename(columns=lambda x: x.strip()
                                .lower()
                                .replace(" ", "_")
                                .replace("/", "_")
                                .replace("(", "")
                                .replace(")", "")
                                .replace("-", "_")
                                .replace(".", ""))
    return df

def identify_meta_columns(df: pd.DataFrame) -> list:
    """
    Identify metadata columns by name patterns.
    Common metadata include isolate IDs, study info, demographics, and year.
    """
    # Known meta keys
    meta_keys = [
        "isolate_id", "vivli_no", "uid", "study", "species", "family",
        "country", "state", "region", "gender", "age", "age_group",
        "speciality", "source", "in_out_patient", "year", "yearcollected",
        "phenotype", "bodylocation"
    ]
    return [c for c in df.columns if c in meta_keys]

def unpivot_atlas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unpivot MIC and S/I/R flag columns into long format.
    """
    df = standardize_columns(df)

    # Rename yearcollected → year, uid/vivli_no → isolate_id, organismname → pathogen
    if "yearcollected" in df.columns:
        df = df.rename(columns={"yearcollected": "year"})
    if "uid" in df.columns:
        df = df.rename(columns={"uid": "vivli_no"})
    if "vivli_no" in df.columns:
        df = df.rename(columns={"vivli_no": "isolate_id"})
    if "organismname" in df.columns:
        df = df.rename(columns={"organismname": "pathogen"})

    # Identify meta vs drug columns
    meta_cols = identify_meta_columns(df)
    drug_cols = [c for c in df.columns if c not in meta_cols]

    # Flag columns end with '_i'
    flag_cols = [c for c in drug_cols if c.endswith("_i")]
    mic_cols  = [c for c in drug_cols if c not in flag_cols]

    # Melt MIC values
    df_mic = df.melt(
        id_vars=meta_cols,
        value_vars=mic_cols,
        var_name="drug",
        value_name="mic_value"
    )

    # Melt S/I/R flags
    df_flag = df.melt(
        id_vars=meta_cols,
        value_vars=flag_cols,
        var_name="drug_flag",
        value_name="sir_flag"
    )
    # Normalize drug names (remove '_i' suffix)
    df_flag["drug"] = df_flag["drug_flag"].str.replace("_i$", "", regex=True)

    # Merge MIC & flag
    df_long = pd.merge(
        df_mic,
        df_flag[meta_cols + ["drug", "sir_flag"]],
        on=meta_cols + ["drug"],
        how="left"
    )
    
    df_long["sir_flag"] = (
        df_long["sir_flag"]
            .str.strip()
            .str.upper()                     # e.g. 'Susceptible' -> 'SUSCEPTIBLE'
            .replace({
                "SUSCEPTIBLE":  "S",
                "RESISTANT":    "R",
                "INTERMEDIATE": "I"
            })
    )   

    # Derive binary label: R→1, S→0, else NaN
    df_long["resistant"] = df_long["sir_flag"].map({"R": 1, "S": 0})

    return df_long

def clean_atlas(path: str) -> pd.DataFrame:
    """
    Load the first worksheet from `path`, unpivot & clean, filter to country-level.
    """
    if not os.path.isfile(path):
        sys.exit(f"ERROR: File not found at {path}")
    xls = pd.ExcelFile(path)
    df  = pd.read_excel(path, sheet_name=xls.sheet_names[0])

    df_long = unpivot_atlas(df)
    # Keep only rows with non-null country
    df_long = df_long[df_long["country"].notna()].reset_index(drop=True)
    return df_long

if __name__ == "__main__":
    # Quick smoke test
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
    raw       = os.path.join(repo_root, "data", "raw")

    # Paths
    abx_dir = os.path.join(raw, "ATLAS_Antibiotics")
    afg_file = os.path.join(raw, "ATLAS_Antifungals", "vivli_sentry_2010_2023.xlsx")
    abx_file = os.path.join(abx_dir, os.listdir(abx_dir)[0])

    for name, path in [("Antifungals", afg_file), ("Antibiotics", abx_file)]:
        df_clean = clean_atlas(path)
        print(f"{name}: {df_clean.shape[0]} rows, {len(df_clean['drug'].unique())} drugs")
        print(df_clean.head(3))
