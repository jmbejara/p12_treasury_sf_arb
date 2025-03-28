# %%

import pandas as pd
import polars as pl
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt
import os

# Set seaborn style
sns.set()

# Import project configurations
from settings import config
import load_bases_data

DATA_DIR = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")



# %% [markdown]
# # 02. Example Notebook with Dependencies
# 
# This notebook is designed demonstrate a number of goals:
# 
#   - The notebook is part of the automated analytical pipeline, as it is run programmatically by the build system, as in the dodo.py file.
#   - It is tracked by version control via Git. To avoid large files and the problems associated with non-text files, the notebook is stripped of its output. 
#   - In order to avoid re-running the notebook every time it changes (it changes often, even by the act of opening it) and to only rerun it if meaningful changes have been made, the build system only looks for changes in the plaintext version of the notebook. That is, the notebook is converted to a Python script via [nbconvert](https://nbconvert.readthedocs.io/en/latest/), which is often packaged with Jupyter.
#   Then, DoIt looks for changes to the Python version. If it detects a difference, then the notebook is re-run. (Note, that you could also convert to a Markdown file with 
#   [JupyText](https://github.com/mwouts/jupytext). However, this package is often not packaged with Jupyter.)
#   - Since we want to use Jupyter Notebooks for exploratory reports, we want to keep fully-computed versions of the notebook (with the output intact). However, earlier I said that I strip the notebook of its output before committing to version control. Well, to keep the output, every time PyDoit runs the notebook, it outputs an HTML version of the freshly run notebook and saves that HTML report in the `output` directory. That way, you will be able to view the finished report at any time without having to open Jupyter.

# %%
from settings import config


ois_file = f"{MANUAL_DATA_DIR}/OIS.xlsx"
data_file = f"{MANUAL_DATA_DIR}/treasury_spot_futures.xlsx"



# %%
df_dates = pd.read_excel(data_file, sheet_name="T_SF", usecols="A", skiprows=6, header=None)
df_dates.columns = ["Date"]
df_dates["Date"] = pd.to_datetime(df_dates["Date"])

# Compute month, year, and day
df_dates["Mat_Month"] = df_dates["Date"].dt.month
df_dates["Mat_Year"] = df_dates["Date"].dt.year
df_dates = df_dates.sort_values("Date").reset_index(drop=True)

# For each month, keep the last date (mimicking: keep if mofd(Date) != mofd(Date[_n+1]))
df_last = df_dates.groupby([df_dates["Mat_Year"], df_dates["Mat_Month"]], as_index=False).agg({"Date": "last"})

# Compute the last day of the month
df_last["Mat_Day"] = df_last["Date"].dt.day

# Drop duplicates and keep required columns
df_matday = df_last[["Date", "Mat_Month", "Mat_Year", "Mat_Day"]].copy()
output_file = os.path.join(DATA_DIR, "last_day_df.csv") 
# Save as CSV
df_matday.to_csv(output_file, index=False)

# Display the final DataFrame
df_matday.head()



# %%


# Load data from the Excel file
df = pd.read_excel(data_file, sheet_name="T_SF", skiprows=6, header=None)

# Define base column names
base_columns = ["Date"]
tenors = [10, 5, 2, 20, 30]  # Available tenors
versions = [1, 2]  # Nearby (1) and Deferred (2) contract versions

# Generate column names dynamically
col_names = ["Date"] + [
    f"{metric}_{v}_{tenor}" 
    for v in versions 
    for tenor in tenors 
    for metric in ["Implied_Repo", "Vol", "Contract", "Price"]
]

# Assign column names
df.columns = col_names

# Drop rows with missing dates
df = df.dropna(subset=["Date"])
df["Date"] = pd.to_datetime(df["Date"])

# Convert numeric columns
numeric_cols = [col for col in df.columns if col.startswith(("Implied_Repo", "Vol_", "Price_"))]
df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

# **Sort columns alphabetically while keeping "Date" first**
sorted_columns = ["Date"] + sorted([col for col in df.columns if col != "Date"])
df = df[sorted_columns]

# Display sorted DataFrame
df.head()

treasury_df = df.copy()
output_file = os.path.join(DATA_DIR, "treasury_df.csv") 
# Save as CSV
# Optionally, save it to a file for later use
treasury_df.to_csv(output_file, index=False)  # Save as CSV


# %%

# Load the Excel file
df_ois = pd.read_excel(ois_file, header=None)

# Drop the first 4 columns
df_ois = df_ois.iloc[:, 4:]

# Drop the top 3 rows and reset index
df_ois = df_ois.iloc[3:].reset_index(drop=True)

# Drop rows at index 1 and 2, then reset index again
df_ois = df_ois.drop(index=[1, 2]).reset_index(drop=True)

# Set the first row as the column headers, then drop it from the DataFrame
df_ois.columns = df_ois.iloc[0]
df_ois = df_ois[1:].reset_index(drop=True)

# Rename the first column to "Date"
df_ois.rename(columns={df_ois.columns[0]: "Date"}, inplace=True)

# Define the renaming mapping for OIS columns
rename_map = {
    "USSO1Z CMPN Curncy": "OIS_1W",
    "USSOA CMPN Curncy":  "OIS_1M",
    "USSOB CMPN Curncy":  "OIS_2M",
    "USSOC CMPN Curncy":  "OIS_3M",
    "USSOF CMPN Curncy":  "OIS_6M",
    "USSO1 CMPN Curncy":  "OIS_1Y",
    "USSO2 CMPN Curncy":  "OIS_2Y",
    "USSO3 CMPN Curncy":  "OIS_3Y",
    "USSO4 CMPN Curncy":  "OIS_4Y",
    "USSO5 CMPN Curncy":  "OIS_5Y",
    "USSO7 CMPN Curncy":  "OIS_7Y",
    "USSO10 CMPN Curncy": "OIS_10Y",
    "USSO15 CMPN Curncy": "OIS_15Y",
    "USSO20 CMPN Curncy": "OIS_20Y",
    "USSO30 CMPN Curncy": "OIS_30Y"
}

# Rename the OIS columns
df_ois.rename(columns=rename_map, inplace=True)

# Convert 'Date' column to datetime format
df_ois["Date"] = pd.to_datetime(df_ois["Date"], errors="coerce")

output_file = os.path.join(DATA_DIR, "ois_df.csv") 

# Save the cleaned data to CSV
df_ois.to_csv(output_file, index=False)

# Display the first few rows
df_ois.head()



