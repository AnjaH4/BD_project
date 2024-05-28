import dask.dataframe as dd
import os
import pandas as pd
import matplotlib.pyplot as plt

# Define the path to the data folder containing the Parquet files
data_folder = 'data/parking-violations/'
parse_dates = ['Issue Date', 'Vehicle Expiration Date', 'Date First Observed']
# Load all Parquet files into a Dask DataFrame
df = dd.read_parquet(os.path.join(data_folder, '*.parquet'), parse_dates=parse_dates)

# Display basic information about the DataFrame
print(df.head())
print(df.info())