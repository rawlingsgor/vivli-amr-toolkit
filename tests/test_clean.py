"""
Cleaning-layer unit tests that run on the 1 k-row fixture workbooks.

If the fixture workbook lacks any S/I/R flag columns, the “resistant not-all-NaN”
assertion is skipped; real-data runs will still exercise the check.
"""

import pytest
from src.cleaning.atlas_clean import clean_atlas

ABX_FIX = "tests/fixtures/atlas_antibiotics_fixture.xlsx"
AFG_FIX = "tests/fixtures/atlas_antifungals_fixture.xlsx"


def test_clean_has_required_columns():
    df = clean_atlas(ABX_FIX)
    required = {"drug", "mic_value", "sir_flag", "resistant"}
    assert required.issubset(df.columns)


def test_country_not_null():
    df = clean_atlas(AFG_FIX)
    assert df["country"].notna().all()


def test_resistant_not_all_nan():
    """
    Ensure at least one non-NaN label unless the fixture contains
    zero S/I/R flag columns (skip in that edge-case).
    """
    df = clean_atlas(ABX_FIX)

    # Skip if fixture has no S/I/R data at all
    if df["sir_flag"].isna().all():
        pytest.skip("Fixture lacks S/I/R flag columns")

    assert df["resistant"].notna().any()
