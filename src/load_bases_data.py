"""
Gather and preprocess data from Siriwardane et al. (and related sources).

Summary:
- We collect various arbitrage spread data that Siriwardane et al. have made
  publicly available on their website.
- They differentiate "unsecured" arbitrages, which require more unsecured
  funding, from "secured" arbitrages, which are more collateralized. The
  paper documents that unsecured strategies (e.g., equity spot-futures,
  options, CIP arbitrage) typically have higher margin requirements.

Coverage:
- JPY/USD FX swap basis data
- Treasury cash-futures bases
- Interest rate swap spreads
- Other data sets, such as CIP and equity spot-futures, are also available in
  the Siriwardane data repository.

Notes:
- Certain functionality here is partially automated. In some places, the data
  is manually extracted or stitched together. 
- The final output is typically combined into a single wide or long format
  dataset for convenience.
"""

import numpy as np
import pandas as pd
from pathlib import Path

from settings import config

# Points to the directory where data is stored
DATA_DIR = config("DATA_DIR")


###############################################################################
# Functions below read older datasets, which are not from the official sources
# provided by OFR but from personal or external collections. They were manually
# compiled and may not be fully consistent with the official version.
###############################################################################





def read_treasury_cash_futures_bases(dirpath=DATA_DIR):
    """
    Load the manually compiled Treasury cash-futures basis data (TU, FV, TY, US).
    
    Returns:
    --------
    DataFrame indexed by date, with columns for each contract type.
    """
    filename = "irr_panel_b.csv"
    dirpath = Path(dirpath)
    df = pd.read_csv(dirpath / filename, parse_dates=["date"]).set_index("date")
    # Column 'PL' is not useful for subsequent analysis
    df = df.drop(columns=["PL"])
    return df



def read_interest_swap_spread_bases(dirpath=DATA_DIR):
    """
    Load the interest rate swap spread panel from a CSV (swap_spreads_panel.csv).
    The data is in percent; we convert to decimal.
    """
    filename = "swap_spreads_panel.csv"
    dirpath = Path(dirpath)
    df = pd.read_csv(dirpath / filename, parse_dates=["date"]).set_index("date")
    return df / 100


def read_adrien_bases_replication(dirpath=DATA_DIR):
    """
    Retrieve bases data from Adrien's replication of certain spreads (FX CIP,
    Treasury-Swap, and so forth). The .dta file includes multiple columns.

    Returns:
    --------
    DataFrame with an index of dates and the following columns:
        - TreasurySwap1Y, TreasurySwap2Y, etc.
        - CIP data for AUD, CAD, EUR, GBP, JPY, etc.
        - Equity spot-futures for Dow, NDAQ, SPX
    """
    df = pd.read_stata(Path(dirpath) / "Final_Spreads.dta")
    df = df.rename(columns={"data": "date"}).set_index("date")

    # Hard-coded subset based on Siriwardane et al. structure
    sa_ordering = [
        "TreasurySwap1Y", "TreasurySwap2Y", "TreasurySwap3Y",
        "TreasurySwap5Y", "TreasurySwap10Y", "TreasurySwap20Y", "TreasurySwap30Y",
        "AUD_diff_ois", "CAD_diff_ois", "EUR_diff_ois", "GBP_diff_ois",
        "JPY_diff_ois", "NZD_diff_ois", "SEK_diff_ois",
        "BoxSpread6m", "BoxSpread12m", "BoxSpread18m",
        "Eq_SF_Dow", "Eq_SF_NDAQ", "Eq_SF_SPX"
    ]
    return df[sa_ordering]


def _read_old_combined_basis_file(data_dir=DATA_DIR, rename=True):
    """
    Combine older JPY CIP, Treasury cash-futures, and interest swap bases
    into a single DataFrame, optionally renaming columns to standardized names.
    """
    data_dir = Path(data_dir)
    _, fxbasis = read_usdjpyfxswap(data_dir)
    tcf = read_treasury_cash_futures_bases(data_dir).rename(columns=lambda x: "tcf_" + x)
    iss = read_interest_swap_spread_bases(data_dir).rename(columns=lambda x: "iss_" + x)

    df = pd.DataFrame()
    df["fxswap_jpy"] = fxbasis
    df = pd.concat([df, tcf], axis=1)
    df = pd.concat([df, iss], axis=1)

    if rename:
        rename_map = {
            "tcf_TU": "Treasury_SF_02Y",
            "tcf_FV": "Treasury_SF_05Y",
            "tcf_TY": "Treasury_SF_10Y",
            "tcf_US": "Treasury_SF_30Y",
            "iss_2": "Treasury_Swap_02Y",
            "iss_5": "Treasury_Swap_05Y",
            "iss_10": "Treasury_Swap_10Y",
            "iss_30": "Treasury_Swap_30Y",
            "fxswap_jpy": "CIP_JPY",
        }
        df = df.rename(columns=rename_map)

    return df


def read_combined_basis_file_OLD(
    dirpath=DATA_DIR,
    rename=True,
    guess_resize=True,
    flip_to_pos_signs=True
):
    """
    Combine older manually-collected data for Treasury cash-futures, Adrien's
    CIP and equity SF data, etc., into one file. Optionally rename columns
    and adjust the sign so that spreads are positive on average.

    Parameters:
    -----------
    dirpath : str or Path
        Base directory containing the CSV/DTA files.
    rename : bool
        Whether to rename columns for standard naming (e.g., 'tcf_TU' -> 'Treasury_SF_02Y').
    guess_resize : bool
        Currently not used, kept for backward compatibility.
    flip_to_pos_signs : bool
        If True, flips the sign of any spread whose mean is negative.

    Returns:
    --------
    DataFrame with combined time series for multiple arbitrage spreads.
    """
    dirpath = Path(dirpath)
    tcf = read_treasury_cash_futures_bases(config.DATA_DIR).rename(columns=lambda x: "tcf_" + x)
    adrien_rep = read_adrien_bases_replication(dirpath=DATA_DIR)
    df = pd.concat([tcf, adrien_rep], axis=1)

    # Rescale a subset of columns from percentage to decimal
    to_scale = [
        "AUD_diff_ois", "CAD_diff_ois", "EUR_diff_ois", "GBP_diff_ois",
        "JPY_diff_ois", "NZD_diff_ois", "SEK_diff_ois",
        "Eq_SF_Dow", "Eq_SF_NDAQ", "Eq_SF_SPX"
    ]
    for col in to_scale:
        df[col] = df[col] / 100

    if rename:
        col_map = {
            "tcf_TU": "Treasury_SF_02Y",
            "tcf_FV": "Treasury_SF_05Y",
            "tcf_TY": "Treasury_SF_10Y",
            "tcf_US": "Treasury_SF_30Y",
            "TreasurySwap1Y": "Treasury_Swap_01Y",
            "TreasurySwap2Y": "Treasury_Swap_02Y",
            "TreasurySwap3Y": "Treasury_Swap_03Y",
            "TreasurySwap5Y": "Treasury_Swap_05Y",
            "TreasurySwap10Y": "Treasury_Swap_10Y",
            "TreasurySwap20Y": "Treasury_Swap_20Y",
            "TreasurySwap30Y": "Treasury_Swap_30Y",
            "AUD_diff_ois": "CIP_AUD",
            "CAD_diff_ois": "CIP_CAD",
            "EUR_diff_ois": "CIP_EUR",
            "GBP_diff_ois": "CIP_GBP",
            "JPY_diff_ois": "CIP_JPY",
            "NZD_diff_ois": "CIP_NZD",
            "SEK_diff_ois": "CIP_SEK",
            "BoxSpread6m": "Box_06m",
            "BoxSpread12m": "Box_12m",
            "BoxSpread18m": "Box_18m",
            "Eq_SF_Dow": "Eq_SF_Dow",
            "Eq_SF_NDAQ": "Eq_SF_NDAQ",
            "Eq_SF_SPX": "Eq_SF_SPX",
        }
        df = df.rename(columns=col_map)

    # Optionally flip any series that has a negative mean
    if flip_to_pos_signs:
        average_vals = df.mean()
        for trade_name in average_vals.index:
            if average_vals[trade_name] < 0:
                df[trade_name] = -df[trade_name]

    return df


###############################################################################
# Functions below read newer, more systematically pulled Siriwardane et al.
# data. Some are stubs (NotImplementedError) for demonstration or placeholders.
###############################################################################


def load_box(data_dir=DATA_DIR, raw=False):
    """
    Load 'box' implied rates vs. OIS for 6, 12, 18 months, in basis points.
    Currently unimplemented here.
    """
    raise NotImplementedError


def load_cds_bond(data_dir=DATA_DIR, raw=False):
    """
    Load the CDS-Bond basis, comparing the implied risk-free rate from bond+CDS
    with a maturity-matched Treasury yield. Typically in basis points.
    """
    filepath = (
        Path(data_dir)
        / "from_siriwardane_et_al"
        / "cds-bond"
        / "cds_bond_implied_rf.dta"
    )
    df = pd.read_stata(filepath).set_index("date")

    if raw:
        return df
    else:
        df["CDS_Bond_HY"] = df["cds_bond_hy_rf"] - df["cds_bond_hy_treas"]
        df["CDS_Bond_IG"] = df["cds_bond_ig_rf"] - df["cds_bond_ig_treas"]
        return df[["CDS_Bond_HY", "CDS_Bond_IG"]]


def load_CIP(data_dir=DATA_DIR, raw=False):
    """
    Placeholder for CIP data from Siriwardane et al. 
    Currently raises NotImplementedError.
    """
    raise NotImplementedError


def load_equity_sf(data_dir=DATA_DIR, raw=False):
    """
    Placeholder for equity spot-futures implied rate minus OIS from Siriwardane et al.
    Currently raises NotImplementedError.
    """
    raise NotImplementedError


def load_tips_treasury(data_dir=DATA_DIR, raw=False):
    """
    Placeholder for TIPS vs. nominal Treasury data. 
    Currently raises NotImplementedError.
    """
    raise NotImplementedError


def load_treasury_sf(data_dir=DATA_DIR, raw=False):
    """
    Placeholder for Treasury futures implied risk-free rate minus OIS from Siriwardane et al.
    Currently raises NotImplementedError.
    """
    raise NotImplementedError


def load_treasury_swap():
    """
    Placeholder for loading Treasury-Swap data from Siriwardane et al.
    """
    raise NotImplementedError


name_map = {
    "raw_box_12m": "Box_06m",
    "raw_box_18m": "Box_12m",
    "raw_box_6m": "Box_18m",
    "raw_cal_dow": "Eq_SF_Dow",
    "raw_cal_ndaq": "Eq_SF_NDAQ",
    "raw_cal_spx": "Eq_SF_SPX",
    "raw_cds_bond_hy": "CDS_Bond_HY",
    "raw_cds_bond_ig": "CDS_Bond_IG",
    "raw_cip_aud": "CIP_AUD",
    "raw_cip_cad": "CIP_CAD",
    "raw_cip_chf": "CIP_CHF",
    "raw_cip_eur": "CIP_EUR",
    "raw_cip_gbp": "CIP_GBP",
    "raw_cip_jpy": "CIP_JPY",
    "raw_cip_nzd": "CIP_NZD",
    "raw_cip_sek": "CIP_SEK",
    "raw_tfut_10": "Treasury_SF_10Y",
    "raw_tfut_2": "Treasury_SF_02Y",
    "raw_tfut_20": "Treasury_SF_20Y",
    "raw_tfut_30": "Treasury_SF_30Y",
    "raw_tfut_5": "Treasury_SF_05Y",
    "raw_tips_treas_10": "TIPS_Treasury_10Y",
    "raw_tips_treas_2": "TIPS_Treasury_02Y",
    "raw_tips_treas_20": "TIPS_Treasury_20Y",
    "raw_tips_treas_5": "TIPS_Treasury_05Y",
    "raw_tswap_1": "Treasury_Swap_01Y",
    "raw_tswap_10": "Treasury_Swap_10Y",
    "raw_tswap_2": "Treasury_Swap_02Y",
    "raw_tswap_20": "Treasury_Swap_20Y",
    "raw_tswap_3": "Treasury_Swap_03Y",
    "raw_tswap_30": "Treasury_Swap_30Y",
    "raw_tswap_5": "Treasury_Swap_05Y",
}


def load_combined_spreads_wide(data_dir=DATA_DIR, raw=False, rename=True):
    """
    Load the wide-format arbitrage spreads from Siriwardane et al. 
    If not raw, only keeps the columns labeled 'raw_*' in the original dataset
    and optionally renames them.

    Returns:
    --------
    DataFrame with a date index and a variety of spread columns (CIP, CDS-Bond,
    Box, Equity SF, TIPS, Treasury-Futures, etc.).
    """
    # The code references an external link for the .dta file
    url = "https://www.dropbox.com/scl/fi/81jm3dbe856i7p17rjy87/arbitrage_spread_wide.dta?rlkey=ke78u464vucmn43zt27nzkxya&st=59g2n7dt&dl=1"
    df = pd.read_stata(url).set_index("date")

    if raw:
        return df.copy()
    else:
        columns_to_keep = list(name_map.keys())
        subset_df = df[columns_to_keep].copy()
        if rename:
            subset_df = subset_df.rename(columns=name_map)
        return subset_df.reindex(sorted(subset_df.columns), axis=1)


def load_combined_spreads_long(dat_dir=DATA_DIR, rename=True):
    """
    Load the long-format version of the Siriwardane et al. spreads. 
    If rename=True, replace 'full_trade' labels with more conventional names
    from name_map (omitting the 'raw_' prefix).
    """
    url = "https://www.dropbox.com/scl/fi/mv2oodkibhzli5ywdgxv7/arbitrage_spread_panel.dta?rlkey=ctzelvfie1nztlp7o24gvnzff&st=nnzdxv78&dl=1"
    df = pd.read_stata(url)

    if rename:
        # Convert 'raw_xxx' suffixes to friendlier names
        no_raw_map = {k.split("raw_")[1]: v for k, v in name_map.items()}
        df["full_trade"] = df["full_trade"].replace(no_raw_map)

    return df


def demo():
    """
    Demonstrates usage of some old vs. new data loading routines.
    """
    fxswap, fxbasis = read_usdjpyfxswap(config.DATA_DIR)
    tcf = read_treasury_cash_futures_bases(config.DATA_DIR)
    iss = read_interest_swap_spread_bases(config.DATA_DIR)
    df_old = read_combined_basis_file_OLD(config.DATA_DIR)
    df_new = load_combined_spreads_wide(data_dir=DATA_DIR)

    df_old.plot()
    df_new.columns
    # Example slice for TCF spreads in old data
    df_old.loc["2010":"2020", ["Treasury_SF_02Y", "Treasury_SF_05Y", "Treasury_SF_10Y", "Treasury_SF_30Y"]].plot()
    # Example slice in new data
    df_new.loc["2010":"2020", ["Treasury_SF_10Y", "Treasury_SF_02Y", "Treasury_SF_20Y", "Treasury_SF_30Y"]].plot()


if __name__ == "__main__":
    # As a placeholder, we do not run anything by default. 
    # You can invoke the code by calling 'demo()' or other relevant functions.
    pass
