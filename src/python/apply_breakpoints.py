#!/usr/bin/env python3
"""
apply_breakpoints.py

EUCAST breakpoint helper (2024 subset).
Extend `eucast_table` with additional organism–drug pairs as needed.
"""

import re
import pandas as pd

# --------------------------------------------------------------------
# EUCAST 2024 – expanded table
# Keys: (organism_key, drug_key) → {"s": S breakpoint (≤), "r": R breakpoint (>)}
# Values in mg/L.  Add more pairs as your analysis demands.
# --------------------------------------------------------------------
eucast_table = {
    # Gram-negative
    ("escherichia_coli",       "amikacin"):       {"s": 8,      "r": 16},
    ("escherichia_coli",       "cefepime"):       {"s": 1,      "r": 4},
    ("escherichia_coli",       "levofloxacin"):   {"s": 0.5,    "r": 1},
    ("escherichia_coli",       "tigecycline"):    {"s": 0.5,    "r": 0.5},

    ("klebsiella_pneumoniae",  "levofloxacin"):   {"s": 0.5,    "r": 1},
    ("klebsiella_pneumoniae",  "tigecycline"):    {"s": 0.5,    "r": 0.5},

    ("pseudomonas_aeruginosa", "amikacin"):       {"s": 8,      "r": 16},
    ("pseudomonas_aeruginosa", "levofloxacin"):   {"s": 0.5,    "r": 1},
    ("pseudomonas_aeruginosa", "tigecycline"):    {"s": 0.5,    "r": 0.5},

    # Gram-positive
    ("staphylococcus_aureus",  "vancomycin"):     {"s": 2,      "r": 2},
    ("staphylococcus_aureus",  "levofloxacin"):   {"s": 1,      "r": 2},
    ("staphylococcus_aureus",  "tigecycline"):    {"s": 0.25,   "r": 0.5},

    ("enterococcus_faecalis",  "tigecycline"):    {"s": 0.25,   "r": 0.25},

    # Candida albicans (echinocandins & azoles)
    ("candida_albicans", "anidulafungin"):         {"s": 0.03,   "r": 0.06},
    ("candida_albicans", "caspofungin"):           {"s": 0.06,   "r": 0.12},
    ("candida_albicans", "micafungin"):            {"s": 0.03,   "r": 0.06},
    ("candida_albicans", "fluconazole"):           {"s": 2,      "r": 4},
    ("candida_albicans", "itraconazole"):          {"s": 0.125,  "r": 0.25},
    ("candida_albicans", "voriconazole"):          {"s": 0.125,  "r": 0.25},
    ("candida_albicans", "posaconazole"):          {"s": 0.125,  "r": 0.25},

    # Candida glabrata
    ("candida_glabrata", "fluconazole"):           {"s": 32,     "r": 32},
    ("candida_glabrata", "anidulafungin"):         {"s": 0.03,   "r": 0.06},
    ("candida_glabrata", "caspofungin"):           {"s": 0.06,   "r": 0.12},
    ("candida_glabrata", "micafungin"):            {"s": 0.03,   "r": 0.06},
}

# --------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------
def _clean_key(text: str) -> str:
    """
    Lowercase; replace spaces with underscores; strip trailing
    '_clsi', '_(clsi)', '_eucast', or '_(eucast)'.
    """
    text = str(text).strip().lower().replace(" ", "_")
    text = re.sub(r"_\(?(clsi|eucast)\)?$", "", text)
    return text

def apply_eucast(mic, organism, drug):
    """
    Return 'S', 'I', 'R', or None if no breakpoint or bad MIC.
    MIC may be numeric or a string like '<=0.5' or '>32'.
    """
    if pd.isna(mic) or pd.isna(organism) or pd.isna(drug):
        return None

    org_key  = _clean_key(organism)
    drug_key = _clean_key(drug)

    try:
        mic_val = float(re.sub(r"[<>]=?", "", str(mic)))
    except ValueError:
        return None

    bp = eucast_table.get((org_key, drug_key))
    if not bp:
        return None

    if mic_val <= bp["s"]:
        return "S"
    elif mic_val > bp["r"]:
        return "R"
    else:
        return "I"

def add_eucast_column(df, mic_col="mic", org_col="cls_i_organism",
                      drug_col="drug", new_col="sir_eucast"):
    """
    Return a copy of df with a new EUCAST S/I/R interpretation column.
    """
    out = df.copy()
    out[new_col] = out.apply(
        lambda r: apply_eucast(r[mic_col], r[org_col], r[drug_col]),
        axis=1
    )
    return out

# --------------------------------------------------------------------
# Demo when run directly
# --------------------------------------------------------------------
if __name__ == "__main__":
    demo = pd.DataFrame({
        "mic": [2, 16, 0.5, ">32"],
        "cls_i_organism": ["Escherichia coli"]*4,
        "drug": ["amikacin"]*4
    })
    print(add_eucast_column(demo))

