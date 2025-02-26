"""
Fetch and process CRSP Treasury data via WRDS.

References:
  - CRSP US Treasury Database Guide:
    https://www.crsp.org/wp-content/uploads/guides/CRSP_US_Treasury_Database_Guide_for_SAS_ASCII_EXCEL_R.pdf

Tables Used:
  - TFZ_DLY (Daily Time Series Items):
      * kytreasno: Unique Treasury record ID
      * kycrspid: CRSP-assigned identifier
      * caldt: Quotation date
      * tdbid: Daily bid price
      * tdask: Daily ask price
      * tdaccint: Accrued interest
      * tdyld: Yield (annualized, bond-equivalent)

  - TFZ_ISS (Issue Descriptions):
      * tcusip: Treasury CUSIP
      * tdatdt: Original issuance date
      * tmatdt: Maturity date at issue
      * tcouprt: Annual coupon rate
      * itype: Issue type (1 = noncallable bonds, 2 = noncallable notes)

Credits:
  - Original version prepared by Younghun Lee for use in class.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
import wrds

from settings import config

# Define data directory and username from environment settings
DATA_DIR = Path(config("DATA_DIR"))
WRDS_USERNAME = config("WRDS_USERNAME")


def pull_CRSP_treasury_daily(
    start_date="1970-01-01",
    end_date="2023-12-31",
    wrds_username=WRDS_USERNAME,
):
    """
    Retrieve daily Treasury quotes (TFZ_DLY) over a specified date range.
    Constructs a 'price' field as the average of bid/ask plus accrued interest.
    """
    query = f"""
    SELECT 
        kytreasno,
        kycrspid,
        caldt,
        tdbid,
        tdask,
        tdaccint,
        tdyld,
        ((tdbid + tdask) / 2.0 + tdaccint) AS price
    FROM crspm.tfz_dly
    WHERE caldt BETWEEN '{start_date}' AND '{end_date}'
    """

    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(query, date_cols=["tdatdt", "tmatdt"])
    db.close()
    return df


def pull_CRSP_treasury_info(wrds_username=WRDS_USERNAME):
    """
    Acquire treasury issue metadata (TFZ_ISS), including CUSIP, coupon,
    and maturity details, for bonds/notes (itype in [1,2]).
    """
    query = """
        SELECT
            kytreasno,
            kycrspid,
            tcusip,
            tdatdt,
            tmatdt,
            tcouprt,
            itype,
            ROUND((tmatdt - tdatdt) / 365.0) AS original_maturity
        FROM crspm.tfz_iss AS iss
        WHERE iss.itype IN (1, 2)
    """

    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(query, date_cols=["tdatdt", "tmatdt"])
    db.close()
    return df


def calc_runness(data):
    """
    Calculate runness for the securities issued in 1980 or later.

    This is due to the following condition of Gurkaynak, Sack, and Wright (2007):
        iv) Exclude on-the-run issues and 1st off-the-run issues
        for 2,3,5, 7, 10, 20, 30 years securities issued in 1980 or later.
    """

    def _calc_runness(df):
        temp = df.sort_values(by=["caldt", "original_maturity", "tdatdt"])
        next_temp = (
            temp.groupby(["caldt", "original_maturity"])["tdatdt"].rank(
                method="first", ascending=False
            )
            - 1
        )
        return next_temp

    data_run_ = data[data["caldt"] >= "1980"]
    runs = _calc_runness(data_run_)
    data["run"] = 0
    data.loc[data_run_.index, "run"] = runs
    return data


def pull_CRSP_treasury_consolidated(
    start_date="1970-01-01",
    end_date=datetime.today().strftime("%Y-%m-%d"),
    wrds_username=WRDS_USERNAME,
):
    """
    Retrieve a merged dataset with daily quotes (TFZ_DLY) and issue details (TFZ_ISS).
    Incorporates fields for computed dirty price (bid/ask plus accrued interest),
    maturity metrics, and whether the security is callable.
    """
    query = f"""
    SELECT
        tfz.kytreasno,
        tfz.kycrspid,
        iss.tcusip,
        tfz.caldt,
        iss.tdatdt,
        iss.tmatdt,
        iss.tfcaldt,
        tfz.tdbid,
        tfz.tdask,
        tfz.tdaccint,
        tfz.tdyld,
        ((tfz.tdbid + tfz.tdask) / 2.0 + tfz.tdaccint) AS price,
        iss.tcouprt,
        iss.itype,
        ROUND((iss.tmatdt - iss.tdatdt) / 365.0) AS original_maturity,
        ROUND((iss.tmatdt - tfz.caldt) / 365.0) AS years_to_maturity,
        tfz.tdduratn,
        tfz.tdretnua
    FROM crspm.tfz_dly AS tfz
    LEFT JOIN crspm.tfz_iss AS iss
        ON tfz.kytreasno = iss.kytreasno
        AND tfz.kycrspid = iss.kycrspid
    WHERE
        tfz.caldt BETWEEN '{start_date}' AND '{end_date}'
        AND iss.itype IN (1, 2)
    """

    db = wrds.Connection(wrds_username=wrds_username)
    df = db.raw_sql(query, date_cols=["caldt", "tdatdt", "tmatdt", "tfcaldt"])
    db.close()

    df["days_to_maturity"] = (df["tmatdt"] - df["caldt"]).dt.days
    df["tfcaldt"] = df["tfcaldt"].fillna(0)
    df["callable"] = df["tfcaldt"] != 0
    df = df.reset_index(drop=True)
    return df


def load_CRSP_treasury_daily(data_dir=DATA_DIR):
    """
    Load previously stored daily quotes (TFZ_DLY) from a parquet file.
    """
    path = data_dir / "TFZ_DAILY.parquet"
    df = pd.read_parquet(path)
    return df


def load_CRSP_treasury_info(data_dir=DATA_DIR):
    """
    Load previously stored TFZ_ISS metadata from a parquet file.
    """
    path = data_dir / "TFZ_INFO.parquet"
    df = pd.read_parquet(path)
    return df


def load_CRSP_treasury_consolidated(data_dir=DATA_DIR, with_runness=True):
    """
    Load the merged CRSP Treasury data (TFZ_consolidated or TFZ_with_runness)
    from a parquet file, depending on whether runness is included.
    """
    if with_runness:
        path = data_dir / "TFZ_with_runness.parquet"
    else:
        path = data_dir / "TFZ_consolidated.parquet"
    df = pd.read_parquet(path)
    return df


def _demo():
    """
    Simple demonstration of data pulling, merging, and runness calculation.
    """
    # Obtain daily data
    df = pull_CRSP_treasury_daily(data_dir=DATA_DIR)
    df.info()

    # Obtain issue information
    df = pull_CRSP_treasury_info(data_dir=DATA_DIR)
    df.info()

    # Create consolidated dataset
    df = pull_CRSP_treasury_consolidated(data_dir=DATA_DIR)
    df.info()

    # Compute runness and show data summary
    df = calc_runness(df)
    df.info()
    return df


if __name__ == "__main__":
    # 1) Pull daily quotes
    df = pull_CRSP_treasury_daily(
        start_date="1970-01-01",
        end_date="2023-12-31",
        wrds_username=WRDS_USERNAME,
    )
    path = DATA_DIR / "TFZ_DAILY.parquet"
    df.to_parquet(path)

    # 2) Pull issue metadata
    df = pull_CRSP_treasury_info(wrds_username=WRDS_USERNAME)
    path = DATA_DIR / "TFZ_INFO.parquet"
    df.to_parquet(path)

    # 3) Pull and merge quotes + metadata
    df = pull_CRSP_treasury_consolidated(wrds_username=WRDS_USERNAME)
    path = DATA_DIR / "TFZ_consolidated.parquet"
    df.to_parquet(path)

    # 4) Runness calculation
    df = calc_runness(df)
    path = DATA_DIR / "TFZ_with_runness.parquet"
    df.to_parquet(path)
