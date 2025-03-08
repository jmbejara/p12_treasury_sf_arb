import pytest
import pandas as pd
from pathlib import Path

import load_bases_data  # Import the module as load_bases_data
from settings import config

# Use the configured data directory for tests
DATA_DIR = config("DATA_DIR")


def test_read_usdjpyfxswap():
    # Call the function and check that the returned objects are of expected type
    fxswap, basis = load_bases_data.read_usdjpyfxswap(dirpath=DATA_DIR)
    assert isinstance(fxswap, pd.DataFrame), "fxswap should be a DataFrame"
    assert isinstance(basis, pd.Series), "basis should be a Series"
    # Check that the index is datetime-like
    assert isinstance(fxswap.index, pd.DatetimeIndex), "fxswap index must be datetime"
    # Basic sanity: basis values should be in decimal (e.g., between 0 and 1)
    assert (basis >= 0).all(), "All basis values should be nonnegative"


def test_read_treasury_cash_futures_bases():
    df = load_bases_data.read_treasury_cash_futures_bases(dirpath=DATA_DIR)
    assert isinstance(df, pd.DataFrame), "Should return a DataFrame"
    # Expected contract columns should be present
    for col in ["TU", "FV", "TY", "US"]:
        assert col in df.columns, f"Expected column '{col}' missing"
    # The 'PL' column should be dropped
    assert "PL" not in df.columns, "'PL' column should have been dropped"


def test_read_interest_swap_spread_bases():
    df = load_bases_data.read_interest_swap_spread_bases(dirpath=DATA_DIR)
    assert isinstance(df, pd.DataFrame), "Should return a DataFrame"
    # Since the function divides by 100, we expect the maximum values to be modest
    max_val = df.max().max()
    assert max_val < 1.5, "Values should be in decimal form after dividing by 100"


def test_read_adrien_bases_replication():
    df = load_bases_data.read_adrien_bases_replication(dirpath=DATA_DIR)
    expected_columns = [
        "TreasurySwap1Y", "TreasurySwap2Y", "TreasurySwap3Y",
        "TreasurySwap5Y", "TreasurySwap10Y", "TreasurySwap20Y", "TreasurySwap30Y",
        "AUD_diff_ois", "CAD_diff_ois", "EUR_diff_ois", "GBP_diff_ois",
        "JPY_diff_ois", "NZD_diff_ois", "SEK_diff_ois",
        "BoxSpread6m", "BoxSpread12m", "BoxSpread18m",
        "Eq_SF_Dow", "Eq_SF_NDAQ", "Eq_SF_SPX"
    ]
    for col in expected_columns:
        assert col in df.columns, f"Column {col} is missing in Adrien replication data"


def test__read_old_combined_basis_file():
    df = load_bases_data._read_old_combined_basis_file(data_dir=DATA_DIR, rename=True)
    # Check that some of the expected renamed columns are present
    for col in [
        "Treasury_SF_02Y", "Treasury_SF_05Y", "Treasury_SF_10Y", "Treasury_SF_30Y",
        "Treasury_Swap_02Y", "Treasury_Swap_05Y", "Treasury_Swap_10Y", "Treasury_Swap_30Y",
        "CIP_JPY"
    ]:
        assert col in df.columns, f"Expected renamed column {col} is missing"


def test_read_combined_basis_file_OLD():
    df = load_bases_data.read_combined_basis_file_OLD(
        dirpath=DATA_DIR, rename=True, flip_to_pos_signs=True
    )
    # Check that expected columns from the combined file exist
    for col in ["Treasury_SF_02Y", "Treasury_SF_05Y", "Treasury_SF_10Y", "Treasury_SF_30Y"]:
        assert col in df.columns, f"Expected column {col} missing in combined file"
    # Check that after flipping, the mean of each series is nonnegative
    for col in df.columns:
        assert df[col].mean() >= 0, f"Mean of column {col} should be nonnegative"


def test_load_cds_bond():
    # Test the raw version returns a DataFrame
    df_raw = load_bases_data.load_cds_bond(data_dir=DATA_DIR, raw=True)
    assert isinstance(df_raw, pd.DataFrame), "Raw load_cds_bond should return a DataFrame"
    # Test the processed version returns the expected columns
    df = load_bases_data.load_cds_bond(data_dir=DATA_DIR, raw=False)
    for col in ["CDS_Bond_HY", "CDS_Bond_IG"]:
        assert col in df.columns, f"Processed CDS-Bond data missing column {col}"


def test_load_box_raises():
    with pytest.raises(NotImplementedError):
        load_bases_data.load_box(data_dir=DATA_DIR)


def test_load_CIP_raises():
    with pytest.raises(NotImplementedError):
        load_bases_data.load_CIP(data_dir=DATA_DIR)


def test_load_equity_sf_raises():
    with pytest.raises(NotImplementedError):
        load_bases_data.load_equity_sf(data_dir=DATA_DIR)


def test_load_tips_treasury_raises():
    with pytest.raises(NotImplementedError):
        load_bases_data.load_tips_treasury(data_dir=DATA_DIR)


def test_load_treasury_sf_raises():
    with pytest.raises(NotImplementedError):
        load_bases_data.load_treasury_sf(data_dir=DATA_DIR)


def test_load_treasury_swap_raises():
    with pytest.raises(NotImplementedError):
        load_bases_data.load_treasury_swap()


def test_load_combined_spreads_wide():
    # Test the raw version returns a DataFrame
    df_raw = load_bases_data.load_combined_spreads_wide(data_dir=DATA_DIR, raw=True)
    assert isinstance(df_raw, pd.DataFrame), "Raw wide spreads should be a DataFrame"
    # Test the processed version returns columns renamed per name_map
    df = load_bases_data.load_combined_spreads_wide(data_dir=DATA_DIR, raw=False, rename=True)
    # For example, check that at least one renamed column exists (e.g. starts with 'Treasury_SF')
    assert any(col.startswith("Treasury_SF") for col in df.columns), "Renamed wide spreads missing expected Treasury_SF columns"


def test_load_combined_spreads_long():
    df = load_bases_data.load_combined_spreads_long(dat_dir=DATA_DIR, rename=True)
    assert isinstance(df, pd.DataFrame), "Long-format spreads should be a DataFrame"
    assert "full_trade" in df.columns, "Long-format data should have a 'full_trade' column"
    # Check that the 'full_trade' column values no longer contain the raw prefix
    # (Assuming that after renaming none of the entries should contain 'raw_')
    assert not df["full_trade"].astype(str).str.contains("raw_").any(), "'full_trade' column still contains raw prefixes"


def test_demo_runs(monkeypatch):
    # The demo function performs plotting. To avoid opening plots during tests,
    # monkeypatch the DataFrame.plot method to do nothing.
    monkeypatch.setattr(pd.DataFrame, "plot", lambda *args, **kwargs: None)
    # Running demo should not raise an exception.
    try:
        load_bases_data.demo()
    except Exception as e:
        pytest.fail(f"demo() raised an unexpected exception: {e}")
