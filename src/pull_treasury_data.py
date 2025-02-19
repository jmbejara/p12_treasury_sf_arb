#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pull_treasury_data.py

This module reads the raw Treasury Spot-Futures data from an Excel file and the
USD OIS Rates from a Stata file. It then performs initial cleaning and computes
the last business day of each month (for matching contract maturities). The cleaned
data is saved as an intermediate pickle file for further processing.

Usage:
    python pull_treasury_data.py
"""

import os
import pandas as pd

def get_last_business_day_df(dates_df):
    """Determine the last business day of each month."""
    dates_df['Year'] = dates_df['Date'].dt.year
    dates_df['Month'] = dates_df['Date'].dt.month
    last_dates = dates_df.groupby(['Year', 'Month'])['Date'].max().reset_index()
    last_dates['Mat_Day'] = last_dates['Date'].dt.day
    last_dates.rename(columns={'Month': 'Mat_Month', 'Year': 'Mat_Year'}, inplace=True)
    return last_dates[['Mat_Month', 'Mat_Year', 'Mat_Day']]

def pull_treasury_data():
    # Define input file paths
    data_file = os.path.join("input", "treasury_spot_futures.xlsx")
    ois_file = os.path.join("input", "USD_OIS_Rates.dta")
    
    # -------------------------
    # Step 1. Import date column to compute last business day for each month.
    df_dates = pd.read_excel(data_file, sheet_name="T_SF", skiprows=6, usecols="A", names=["Date"])
    df_dates['Date'] = pd.to_datetime(df_dates['Date'], errors='coerce')
    df_dates = df_dates.dropna(subset=["Date"]).sort_values("Date")
    last_day_df = get_last_business_day_df(df_dates)
    if len(last_day_df) > 0:
        # Drop the last observation if necessary (as in the original Stata code)
        last_day_df = last_day_df.iloc[:-1].reset_index(drop=True)
    
    # -------------------------
    # Step 2. Import the full Treasury Spot-Futures data.
    col_names = [
        "Date", 
        "Implied_Repo_1_10", "Vol_1_10", "Contract_1_10", "Price_1_10",
        "Implied_Repo_1_5",  "Vol_1_5",  "Contract_1_5",  "Price_1_5",
        "Implied_Repo_1_2",  "Vol_1_2",  "Contract_1_2",  "Price_1_2",
        "Implied_Repo_1_20", "Vol_1_20", "Contract_1_20", "Price_1_20",
        "Implied_Repo_1_30", "Vol_1_30", "Contract_1_30", "Price_1_30",
        "Implied_Repo_2_10", "Vol_2_10", "Contract_2_10", "Price_2_10",
        "Implied_Repo_2_5",  "Vol_2_5",  "Contract_2_5",  "Price_2_5",
        "Implied_Repo_2_2",  "Vol_2_2",  "Contract_2_2",  "Price_2_2",
        "Implied_Repo_2_20", "Vol_2_20", "Contract_2_20", "Price_2_20",
        "Implied_Repo_2_30", "Vol_2_30", "Contract_2_30", "Price_2_30"
    ]
    df = pd.read_excel(data_file, sheet_name="T_SF", skiprows=6, names=col_names)
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df = df.dropna(subset=["Date"]).copy()
    
    # Convert numeric-like columns to numbers
    numeric_cols = [col for col in df.columns if any(prefix in col for prefix in ["Implied_Repo", "Vol_", "Price_"])]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # -------------------------
    # Read the USD OIS Rates file.
    df_ois = pd.read_stata(ois_file)
    df_ois['date'] = pd.to_datetime(df_ois['date'])
    
    # Save the pulled data to an intermediate file
    intermediate_data = {
        "treasury_df": df,
        "ois_df": df_ois,
        "last_day_df": last_day_df
    }
    os.makedirs("intermediate", exist_ok=True)
    intermediate_file = os.path.join("intermediate", "treasury_intermediate.pkl")
    pd.to_pickle(intermediate_data, intermediate_file)
    print(f"Pulled data saved to {intermediate_file}")
    
if __name__ == '__main__':
    pull_treasury_data()
