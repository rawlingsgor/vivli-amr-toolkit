#!/usr/bin/env python
"""
distinct_keys.py
----------------
Scan the numeric-MIC datasets and list every distinct
(raw_organism, raw_drug) pair.  Writes docs/distinct_keys.json.
"""

import pathlib, json, pandas as pd

PROC = pathlib.Path("data/processed")
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
        print(f"[!] Missing {path} – skip")
        continue

    df = pd.read_parquet(path, columns=["organism", "drug"]).dropna()
    raw = {
        (o.strip().lower(), d.strip().lower())
        for o, d in zip(df.organism, df.drug)
    }
    pairs.update(raw)
    print(f"[+] {fname}: {len(raw):,} pairs")

out = pathlib.Path("docs/distinct_keys.json")
out.parent.mkdir(exist_ok=True)
with out.open("w") as w:
    json.dump(sorted(list(pairs)), w, indent=2)

print(f"[✓] Wrote {len(pairs):,} unique pairs → {out.relative_to(pathlib.Path.cwd())}")

