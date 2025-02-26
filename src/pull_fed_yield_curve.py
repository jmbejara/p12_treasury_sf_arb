import pandas as pd
import requests
from io import BytesIO
from pathlib import Path

from settings import config

DATA_DIR = config("DATA_DIR")


def pull_fed_yield_curve():
    """
    Fetch yield curve estimates from the Federal Reserve's website.

    The downloaded CSV file contains fitted zero-coupon yields based on the
    methodology of Gurkaynak, Sack, and Wright (2007). We skip the initial
    rows containing header notes and use columns labeled SVENYxx, which are
    yield data for maturities from 1 to 30 years.
    
    Returns:
    --------
    df_all : pandas.DataFrame
        Full DataFrame containing all columns from the CSV.
    df : pandas.DataFrame
        Subset DataFrame containing columns SVENY01 through SVENY30.
    """
    url = "https://www.federalreserve.gov/data/yield-curve-tables/feds200628.csv"
    response = requests.get(url)
    data_stream = BytesIO(response.content)

    # Skip the first 9 lines of header information
    df_all = pd.read_csv(data_stream, skiprows=9, index_col=0, parse_dates=True)

    # Extract yields for maturities 1 through 30 years (SVENY01, SVENY02, ..., SVENY30)
    columns_of_interest = [f"SVENY{i:02d}" for i in range(1, 31)]
    df = df_all[columns_of_interest]

    return df_all, df


def load_fed_yield_curve_all(data_dir=DATA_DIR):
    """
    Load the comprehensive Fed yield curve DataFrame from a local parquet file.
    
    Parameters:
    -----------
    data_dir : Path or str
        Directory where the parquet file is stored.

    Returns:
    --------
    pandas.DataFrame
        Full yield curve dataset as read from the stored parquet file.
    """
    path = Path(data_dir) / "fed_yield_curve_all.parquet"
    return pd.read_parquet(path)


def load_fed_yield_curve(data_dir=DATA_DIR):
    """
    Load a subset of Fed yield curve columns (SVENY01 through SVENY30)
    from a local parquet file.
    
    Parameters:
    -----------
    data_dir : Path or str
        Directory where the parquet file is stored.

    Returns:
    --------
    pandas.DataFrame
        DataFrame with SVENY01 through SVENY30 columns, read from disk.
    """
    path = Path(data_dir) / "fed_yield_curve.parquet"
    return pd.read_parquet(path)


def _demo():
    """
    Demo function: Illustrates pulling the data and printing out
    the first few rows.
    """
    df_all, df_subset = pull_fed_yield_curve()
    print("Full dataset sample:\n", df_all.head())
    print("Subset (SVENY01 - SVENY30) sample:\n", df_subset.head())


if __name__ == "__main__":
    # Download the Fed yield curve data
    df_all, df = pull_fed_yield_curve()

    # Save the full dataset
    all_path = Path(DATA_DIR) / "fed_yield_curve_all.parquet"
    df_all.to_parquet(all_path)

    # Save the subset of columns
    sub_path = Path(DATA_DIR) / "fed_yield_curve.parquet"
    df.to_parquet(sub_path)
