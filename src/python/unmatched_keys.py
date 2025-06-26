import pathlib, pandas as pd, collections

ROOT   = pathlib.Path(__file__).resolve().parents[2]
PROC   = ROOT / "data" / "processed"
MASTER = PROC / "master.parquet"

BP = pd.read_csv(ROOT / "docs" / "eucast_2025_breakpoints.csv")
BP["key"] = BP.organism.str.lower().str.strip() + "||" + BP.drug.str.lower().str.strip()
bp_keys = set(BP.key)

df = pd.read_parquet(MASTER, columns=["organism", "drug"])
df["key"] = (
    df.organism.str.lower().str.strip()
    + "||"
    + df.drug.str.lower().str.strip()
)

unmatched = df.loc[~df.key.isin(bp_keys)]
ctr = collections.Counter(unmatched.key)

print("\nTop 30 unmatched organism–drug pairs:")
for k, c in ctr.most_common(30):
    org, drug = k.split("||")
    print(f"{c:8,d}  {org}  |  {drug}")

