#!/usr/bin/env python3
"""
compute_percentiles.py
----------------------
Load all processed *_long.parquet files, compute MIC percentiles
(p10, p25, median, p75, p90) by organism–drug–year, and write out
trend_tables/mic_percentiles.csv
"""
import pandas as pd
from pathlib import Path

# 1) Paths
DATA_PROC = Path("data/processed")
OUT_DIR   = Path("trend_tables")
OUT_DIR.mkdir(exist_ok=True)

# 2) Load & concat
dfs = []
for p in DATA_PROC.glob("*_long.parquet"):
    df = pd.read_parquet(p, use_nullable_dtypes=True)
    dfs.append(df)
master = pd.concat(dfs, ignore_index=True)

# 3) Drop rows without MIC
master = master.dropna(subset=["mic"])

# 4) Compute percentiles
percentiles = (
    master
    .groupby(["organism","drug","year"], as_index=False)["mic"]
    .agg(
        p10    = lambda x: x.quantile(0.10),
        p25    = lambda x: x.quantile(0.25),
        median = "median",
        p75    = lambda x: x.quantile(0.75),
        p90    = lambda x: x.quantile(0.90),
    )
)

# 5) Save
out_path = OUT_DIR / "mic_percentiles.csv"
percentiles.to_csv(out_path, index=False)
print(f"Wrote {out_path} with {len(percentiles):,} rows")
