from src.data.atlas_ingest import load_atlas

ABX_FIX = "tests/fixtures/atlas_antibiotics_fixture.xlsx"
AFG_FIX = "tests/fixtures/atlas_antifungals_fixture.xlsx"

def test_ingest_antibiotics_shape():
    df, _ = load_atlas(ABX_FIX)
    assert df.shape[0] == 1000   # fixture row count

def test_ingest_antifungals_shape():
    df, _ = load_atlas(AFG_FIX)
    assert df.shape[0] == 1000
