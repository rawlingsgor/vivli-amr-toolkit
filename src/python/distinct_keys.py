#!/usr/bin/env python
"""
distinct_keys.py
----------------
Scan the processed Parquet files that contain numeric MIC values
and list EVERY distinct (raw_organism, raw_drug) pair.

Output: docs/distinct_keys.json   – sorted list of 2-tuples
"""

import pathlib, json, pandas as pd

PROC = pathlib.Path("data/processed")

# Only include files that truly hold numeric MIC columns
KEEP = {
    "atlas_antibiotics_long.parquet",
    "atlas_antifungals_long.parquet",
    "gears_long.parquet",
    "sidero-wt_long.parquet",
    "keystone_long.parquet",
}

pairs: set[tuple[str, str]] = set()

for fname in KEEP:
    path = PROC / fname
    if not path.exists():
        print(f"[!] Missing {path} – skipped")
        continue

    try:
        df = pd.read_parquet(path)
    except Exception as e:
        print(f"[!] {path.name}: cannot read — {e}")
        continue

    # prefer 'organism', else 'species'
    org_col = (
        "organism" if "organism" in df.columns
        else "species" if "species" in df.columns
        else None
    )
    if org_col is None or "drug" not in df.columns:
        print(f"[!] {path.name}: missing organism/species or drug — skipped")
        continue

    raw_pairs = {
        (str(o).strip().lower(), str(d).strip().lower())
        for o, d in zip(df[org_col], df["drug"])
        if pd.notna(o) and pd.notna(d)
    }
    pairs.update(raw_pairs)
    print(f"[+] {fname}: {len(raw_pairs):,} pairs")

out = pathlib.Path("docs/distinct_keys.json")
out.parent.mkdir(exist_ok=True)
with out.open("w") as w:
    json.dump(sorted(list(pairs)), w, indent=2)

print(f"\n[✓] Wrote {len(pairs):,} unique organism–drug pairs → {out}")

