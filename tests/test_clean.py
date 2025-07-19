from src.cleaning.atlas_clean import clean_atlas

def test_clean_has_required_columns():
    df_long = clean_atlas("data/raw/ATLAS_Antibiotics/2025_03_11 atlas_antibiotics.xlsx")
    assert {"drug", "mic_value", "sir_flag", "resistant"}.issubset(df_long.columns)

def test_country_not_null():
    df_long = clean_atlas("data/raw/ATLAS_Antifungals/vivli_sentry_2010_2023.xlsx")
    assert df_long["country"].notna().all()
