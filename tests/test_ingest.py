import os
from src.data.atlas_ingest import load_atlas

def test_ingest_antibiotics_shape():
    df, _ = load_atlas("data/raw/ATLAS_Antibiotics/2025_03_11 atlas_antibiotics.xlsx")
    assert df.shape[0] > 900_000 and df.shape[1] > 100

def test_ingest_antifungals_shape():
    df, _ = load_atlas("data/raw/ATLAS_Antifungals/vivli_sentry_2010_2023.xlsx")
    assert df.shape[0] > 20_000 and df.shape[1] > 20
