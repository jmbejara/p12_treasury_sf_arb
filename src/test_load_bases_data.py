# test_load_bases_data.py
import pytest
import pandas as pd
from pathlib import Path

import load_bases_data
from settings import config

# Use the local data directory from config
DATA_DIR = Path(config("DATA_DIR"))

def test_read_usdjpyfxswap():
    """
    Test that read_usdjpyfxswap can read the real 'fwswaprate_data.xlsx' in DATA_DIR.
    If the file doesn't exist, the test fails with a FileNotFoundError (no skip).
    """
    file_path = DATA_DIR / "fwswaprate_data.xlsx"
    # Attempt to read; if missing, you'll see an error instead of skipping
    fxswap, basis = load_bases_data.read_usdjpyfxswap(DATA_DIR)

    assert isinstance(fxswap, pd.DataFrame), "fxswap must be a DataFrame"
    assert isinstance(basis, pd.Series), "basis must be a Series"
    assert "fxbasisONjpy" in fxswap.columns, "Column 'fxbasisONjpy' not found"
    assert "fxswaprate_LA" in fxswap.columns, "Column 'fxswaprate_LA' not found"
    assert not basis.empty, "Basis Series is unexpectedly empty"

def test_read_treasury_cash_futures_bases():
    """
    Test that read_treasury_cash_futures_bases reads the real 'irr_panel_b.csv' in DATA_DIR.
    If the file doesn't exist or is invalid, the test fails outright.
    """
    file_path = DATA_DIR / "irr_panel_b.csv"
    df = load_bases_data.read_treasury_cash_futures_bases(DATA_DIR)

    assert isinstance(df, pd.DataFrame), "Expected a DataFrame"
    assert "PL" not in df.columns, "Expected 'PL' column to be dropped"
    assert not df.empty, "DataFrame is unexpectedly empty"
