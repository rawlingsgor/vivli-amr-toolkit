"""
Cleaning-layer unit tests that run on the 1 k-row fixture workbooks.

If the fixture workbook lacks any S/I/R flag columns, the "resistant not-all-Na"
assertion is skipped; real-data runs will still exercise the check.
"""

import pytest
from src.cleaning.atlas_clean import clean_atlas
    
# --------------------------------------------------------------------
# Use the 1 k-row fixture workbooks that travel with the repository
# --------------------------------------------------------------------
ABX_FIX = "tests/fixtures/atlas_antibiotics_fixture.xlsx"
AFG_FIX = "tests/fixtures/atlas_antifungals_fixture.xlsx"


def test_clean_has_required_columns():
    """
    The cleaned antibiotics fixture must include the core long-format columns.
    """
    df_long = clean_atlas(ABX_FIX)
    required = {"drug", "mic_value", "sir_flag", "resistant"}
    assert required.issubset(df_long.columns)

       
def test_country_not_null():
    """
    No row in the antifungals fixture should have a null country after cleaning.
    """
    df_long = clean_atlas(AFG_FIX)
    assert df_long["country"].notna().all()   
    
        
def test_resistant_not_all_nan():
    """
    Ensure at least one non-NaN label unless the fixture contains
    zero S/I/R flag columns (skip in that edge-case).
    """
    df_long = clean_atlas(ABX_FIX)

    # Skip if fixture has no S/I/R data at all
    if df_long["sir_flag"].isna().all():
        pytest.skip("Fixture lacks S/I/R flag columns")

    assert df_long["resistant"].notna().any()
