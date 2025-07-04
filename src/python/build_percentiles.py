#!/usr/bin/env python3
"""
build_percentiles.py
--------------------
Compute MIC percentiles (p10/25/50/75/90 + n)
for every country × organism × drug × year.

Output → trend_tables/mic_percentiles.parquet
"""
from pathlib import Path
import pandas as pd

MASTER = Path("data/processed/master.parquet")
OUT    = Path("trend_tables/mic_percentiles.parquet")
OUT.parent.mkdir(exist_ok=True)

print("[+] loading master …")
df = pd.read_parquet(
    MASTER,
    columns=["country","organism","drug","year","mic"]
).dropna(subset=["country","organism","drug","year","mic"])

print("[+] computing percentiles …")
pct = (
    df.groupby(["country","organism","drug","year"])
      .mic.agg(
          n      ="size",
          p10    =lambda s: s.quantile(0.10),
          p25    =lambda s: s.quantile(0.25),
          median =lambda s: s.quantile(0.50),
          p75    =lambda s: s.quantile(0.75),
          p90    =lambda s: s.quantile(0.90),
      )
      .reset_index()
)

pct.to_parquet(OUT, index=False)
print(f"[✓] wrote {len(pct):,} rows → {OUT}")

