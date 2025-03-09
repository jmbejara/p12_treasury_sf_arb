import pytest
import pandas as pd
from pathlib import Path
import pull_fed_yield_curve  # This is your module under test
from settings import config

DATA_DIR = config("DATA_DIR")


def test_pull_fed_yield_curve():
    """
    Test that pull_fed_yield_curve() returns two DataFrames,
    that the full dataset has a datetime index, and the subset
    includes columns SVENY01 through SVENY30.
    """
    df_all, df_subset = pull_fed_yield_curve.pull_fed_yield_curve()
    assert isinstance(df_all, pd.DataFrame), "df_all should be a DataFrame"
    assert isinstance(df_subset, pd.DataFrame), "df_subset should be a DataFrame"
    # Check that the index is parsed as datetime
    assert isinstance(df_all.index, pd.DatetimeIndex), "Index of df_all should be a DatetimeIndex"
    # Verify that the subset DataFrame contains columns for maturities 1 to 30
    expected_cols = [f"SVENY{i:02d}" for i in range(1, 31)]
    for col in expected_cols:
        assert col in df_subset.columns, f"Column {col} is missing in the subset DataFrame"


def test_load_fed_yield_curve_all():
    """
    Test that load_fed_yield_curve_all() loads a non-empty DataFrame from disk.
    """
    df_all = pull_fed_yield_curve.load_fed_yield_curve_all(data_dir=DATA_DIR)
    assert isinstance(df_all, pd.DataFrame), "Should return a DataFrame"
    assert not df_all.empty, "The loaded DataFrame from fed_yield_curve_all.parquet should not be empty"
    assert isinstance(df_all.index, pd.DatetimeIndex), "The index of the loaded DataFrame should be a DatetimeIndex"


def test_load_fed_yield_curve():
    """
    Test that load_fed_yield_curve() loads the subset DataFrame with expected columns.
    """
    df = pull_fed_yield_curve.load_fed_yield_curve(data_dir=DATA_DIR)
    assert isinstance(df, pd.DataFrame), "Should return a DataFrame"
    assert not df.empty, "The loaded DataFrame from fed_yield_curve.parquet should not be empty"
    expected_cols = [f"SVENY{i:02d}" for i in range(1, 31)]
    for col in expected_cols:
        assert col in df.columns, f"Column {col} is missing in the loaded DataFrame"


def test_demo_output(capsys):
    """
    Test that the _demo() function prints out the expected sample text.
    """
    pull_fed_yield_curve._demo()
    captured = capsys.readouterr().out
    assert "Full dataset sample:" in captured, "Demo output should include 'Full dataset sample:'"
    assert "Subset (SVENY01 - SVENY30) sample:" in captured, "Demo output should include 'Subset (SVENY01 - SVENY30) sample:'"
