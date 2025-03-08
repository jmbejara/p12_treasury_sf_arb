# test_load_bases_data.py
import pytest
import pandas as pd
from pathlib import Path
import load_bases_data
from settings import config


@pytest.fixture(autouse=True)
def patch_config(monkeypatch):
    """
    Ensures config("DATA_DIR") returns this folder,
    so any code that calls config("DATA_DIR") won't fail.
    """
    monkeypatch.setenv("DATA_DIR", "/Users/jameschen/FINM-32900-Final-Projcet-12-1/_data")


@pytest.fixture
def mock_data(monkeypatch):
    """
    Monkey-patch pd.read_excel and pd.read_csv so they return
    small dummy DataFrames that contain the columns needed by:
      - read_usdjpyfxswap (fxbasisONjpy, fxswaprate_LA)
      - read_treasury_cash_futures_bases (PL, plus e.g. TU).
    """
    def mock_read_excel(*args, **kwargs):
        # Must include columns 'date', 'fxbasisONjpy', 'fxswaprate_LA'
        return pd.DataFrame({
            "date": pd.date_range("2022-01-01", periods=2, freq="D"),
            "fxbasisONjpy": [0.10, 0.15],
            "fxswaprate_LA": [0.20, 0.25],
        })

    def mock_read_csv(*args, **kwargs):
        # Must include columns 'date', 'PL' (so code can drop it), 
        # and some additional columns like 'TU' (the T-bill/futures columns).
        return pd.DataFrame({
            "date": pd.date_range("2022-01-01", periods=2, freq="D"),
            "PL": [1, 2],
            "TU": [0.5, 0.6],
        })

    monkeypatch.setattr(pd, "read_excel", mock_read_excel)
    monkeypatch.setattr(pd, "read_csv", mock_read_csv)


def test_read_usdjpyfxswap(mock_data):
    """
    With mock_data, read_usdjpyfxswap sees our dummy Excel DataFrame,
    so it won't fail or skip even if the real file is missing.
    """
    data_dir = config("DATA_DIR")
    fxswap, basis = load_bases_data.read_usdjpyfxswap(data_dir)
    assert isinstance(fxswap, pd.DataFrame)
    assert isinstance(basis, pd.Series)
    # Confirm the dummy columns appear
    assert "fxbasisONjpy" in fxswap.columns
    # Confirm the basis is non-empty
    assert len(basis) == 2
    assert (basis >= 0).all()


def test_read_treasury_cash_futures_bases(mock_data):
    """
    With mock_data, read_treasury_cash_futures_bases sees our dummy CSV DataFrame,
    so it won't fail or skip if the real CSV is absent.
    """
    data_dir = config("DATA_DIR")
    df = load_bases_data.read_treasury_cash_futures_bases(data_dir)
    assert isinstance(df, pd.DataFrame)
    # Code is expected to drop the 'PL' column
    assert "PL" not in df.columns
    # Confirm there's still data left
    assert not df.empty
