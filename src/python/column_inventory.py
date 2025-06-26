import pathlib, pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
RAW  = ROOT / "data" / "raw"
OUT  = ROOT / "docs" / "column_inventory.csv"

rows = []
for x in sorted(RAW.glob("**/*.xlsx")):
    vendor = x.parts[-2]
    try:
        df = pd.read_excel(x, nrows=0, engine="openpyxl")
        for col in df.columns:
            rows.append({"vendor": vendor, "file": x.name, "column": str(col)})
    except Exception as e:
        print(f"[!] {vendor}: {x.name} – {e}")

pd.DataFrame(rows).to_csv(OUT, index=False)
print(f"[✓] wrote column list to {OUT}")

