#!/usr/bin/env python3
"""
apply_breakpoints.py

Helper functions to apply EUCAST clinical breakpoints (2024 subset) to
MIC data and generate S/I/R interpretations.

Extend `eucast_table` with additional (organism, drug) pairs as needed.
"""

import re
import pandas as pd

# --------------------------------------------------------------------
# EUCAST 2024 – starter table
# Keys are lowercase, snake_case
# Values: {"s": S breakpoint (≤), "r": R breakpoint (>)}
# --------------------------------------------------------------------
eucast_table = {
    # Gram-negative
    ("escherichia_coli", "amikacin"):        {"s": 8,   "r": 16},
    ("escherichia_coli", "cefepime"):        {"s": 1,   "r": 4},
    ("pseudomonas_aeruginosa", "amikacin"):  {"s": 8,   "r": 16},

    # Gram-positive
    ("staphylococcus_aureus", "vancomycin"): {"s": 2,   "r": 2},

    # Candida albicans – echinocandins & azoles
    ("candida_albicans", "anidulafungin"):   {"s": 0.03, "r": 0.06},
    ("candida_albicans", "caspofungin"):     {"s": 0.06, "r": 0.12},
    ("candida_albicans", "micafungin"):      {"s": 0.03, "r": 0.06},
    ("candida_albicans", "fluconazole"):     {"s": 2,    "r": 4},
    ("candida_albicans", "itraconazole"):    {"s": 0.125,"r": 0.25},
    ("candida_albicans", "voriconazole"):    {"s": 0.125,"r": 0.25},
    ("candida_albicans", "posaconazole"):    {"s": 0.125,"r": 0.25},
}

# --------------------------------------------------------------------
# Helper functions
# --------------------------------------------------------------------
def _clean_key(text: str) -> str:
    """
    Lowercase, replace spaces with underscores, and strip trailing
    suffixes like '_clsi' or '_eucast'.
    """
    text = str(text).strip().lower().replace(" ", "_")
    return re.sub(r"_(clsi|eucast)$", "", text)

def apply_eucast(mic, organism, drug):
    """
    Return 'S', 'I', 'R', or None (if no breakpoint available or bad MIC).

    Parameters
    ----------
    mic : float or str
        MIC value (numeric or string like '<=0.5', '>32').
    organism : str
        Organism name (any case/spaces ok).
    drug : str
        Drug name (any case/spaces ok).

    Notes
    -----
    • EUCAST defines I as between S and R thresholds.
    • MIC values '>32' use numeric part (32) for comparison.
    """
    if pd.isna(mic) or pd.isna(organism) or pd.isna(drug):
        return None

    org_key  = _clean_key(organism)
    drug_key = _clean_key(drug)

    # Extract numeric MIC
    try:
        mic_val = float(re.sub(r"[<>]=?", "", str(mic)))
    except ValueError:
        return None

    bp = eucast_table.get((org_key, drug_key))
    if not bp:
        return None  # breakpoint not defined

    if mic_val <= bp["s"]:
        return "S"
    elif mic_val > bp["r"]:
        return "R"
    else:
        return "I"

def add_eucast_column(df, mic_col="mic", org_col="cls_i_organism",
                      drug_col="drug", new_col="sir_eucast"):
    """
    Return a copy of *df* with an added column of EUCAST S/I/R calls.
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

