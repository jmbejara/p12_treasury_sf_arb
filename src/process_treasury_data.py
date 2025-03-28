# %%
import os
import sys
import pandas as pd

# Ensure 'src' is in sys.path
sys.path.append(os.path.abspath("./src"))  # Add 'src' to the path

# Now import config from settings.py
from settings import config  # <- FIXED

# Set data directory
DATA_DIR = config("DATA_DIR")
MANUAL_DATA_DIR = config("MANUAL_DATA_DIR")
OUTPUT_DIR = config("OUTPUT_DIR")

# Import required functions
from calc_treasury_data import calc_treasury
from calc_treasury_data import calc_treasury, parse_contract_date, interpolate_ois, rolling_outlier_flag


# %%
data_dir = DATA_DIR

treasury_file = os.path.join(data_dir, "treasury_df.csv")
ois_file = os.path.join(data_dir, "ois_df.csv")
last_day_file = os.path.join(data_dir, "last_day_df.csv")

# Load data
df = pd.read_csv(treasury_file)
df_ois = pd.read_csv(ois_file)
last_day_df = pd.read_csv(last_day_file)

print(last_day_df)

# Convert date columns to datetime format
df["Date"] = pd.to_datetime(df["Date"])
df_ois["Date"] = pd.to_datetime(df_ois["Date"])


# Run data processing
calc_treasury()





