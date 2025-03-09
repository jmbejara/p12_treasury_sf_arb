import pytest
import pandas as pd
from pathlib import Path

import pull_treasury_data  # your module
from settings import config

DATA_DIR = config("DATA_DIR")

# --- Tests for load functions (using local parquet files) ---

def test_load_CRSP_treasury_daily():
    path = DATA_DIR / "TFZ_DAILY.parquet"
    if not path.exists():
        pytest.skip("TFZ_DAILY.parquet file not found in DATA_DIR.")
    df = pull_treasury_data.load_CRSP_treasury_daily(data_dir=DATA_DIR)
    assert isinstance(df, pd.DataFrame), "Loaded TFZ_DAILY should be a DataFrame."
    assert not df.empty, "The TFZ_DAILY DataFrame is empty."
    # Check for key columns (e.g. price, caldt, tdbid, tdask, tdaccint, tdyld)
    for col in ["caldt", "price", "tdbid", "tdask", "tdaccint", "tdyld"]:
        assert col in df.columns, f"Expected column '{col}' not found in TFZ_DAILY."

def test_load_CRSP_treasury_info():
    path = DATA_DIR / "TFZ_INFO.parquet"
    if not path.exists():
        pytest.skip("TFZ_INFO.parquet file not found in DATA_DIR.")
    df = pull_treasury_data.load_CRSP_treasury_info(data_dir=DATA_DIR)
    assert isinstance(df, pd.DataFrame), "Loaded TFZ_INFO should be a DataFrame."
    assert not df.empty, "The TFZ_INFO DataFrame is empty."
    # Check for expected columns
    for col in ["tcusip", "tdatdt", "tmatdt", "tcouprt", "itype"]:
        assert col in df.columns, f"Expected column '{col}' not found in TFZ_INFO."

def test_load_CRSP_treasury_consolidated_with_runness():
    path = DATA_DIR / "TFZ_with_runness.parquet"
    if not path.exists():
        pytest.skip("TFZ_with_runness.parquet file not found in DATA_DIR.")
    df = pull_treasury_data.load_CRSP_treasury_consolidated(data_dir=DATA_DIR, with_runness=True)
    assert isinstance(df, pd.DataFrame), "Loaded TFZ_with_runness should be a DataFrame."
    assert not df.empty, "The TFZ_with_runness DataFrame is empty."
    # If runness was computed, the "run" column should be present.
    assert "run" in df.columns, "Column 'run' missing in TFZ_with_runness."

def test_load_CRSP_treasury_consolidated_without_runness():
    path = DATA_DIR / "TFZ_consolidated.parquet"
    if not path.exists():
        pytest.skip("TFZ_consolidated.parquet file not found in DATA_DIR.")
    df = pull_treasury_data.load_CRSP_treasury_consolidated(data_dir=DATA_DIR, with_runness=False)
    assert isinstance(df, pd.DataFrame), "Loaded TFZ_consolidated should be a DataFrame."
    assert not df.empty, "The TFZ_consolidated DataFrame is empty."
    # 'run' column is not required when runness is not included.

# --- Test for calc_runness function ---

def test_calc_runness():
    # Use the consolidated file (without runness) and then compute runness
    path = DATA_DIR / "TFZ_consolidated.parquet"
    if not path.exists():
        pytest.skip("TFZ_consolidated.parquet file not found in DATA_DIR.")
    df = pull_treasury_data.load_CRSP_treasury_consolidated(data_dir=DATA_DIR, with_runness=False)
    df_with_run = pull_treasury_data.calc_runness(df)
    assert "run" in df_with_run.columns, "calc_runness should add a 'run' column."
    # Ensure the run column is numeric
    assert pd.api.types.is_numeric_dtype(df_with_run["run"]), "'run' column should be numeric."

# --- Test for the demo function ---

def test_demo_output(capsys):
    # _demo prints info on the data; we capture the output.
    df = pull_treasury_data._demo()   # no TypeError now
    captured = capsys.readouterr().out
    assert "RangeIndex" in captured or "DatetimeIndex" in captured
    assert isinstance(df, pd.DataFrame)
