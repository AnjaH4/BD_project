import pandas as pd
import os

# Define the expected data types, excluding date columns
dtypes = {
    'Summons Number': 'Int64',
    'Plate ID': 'object',
    'Registration State': 'object',
    'Plate Type': 'object',
    'Violation Code': 'Int64',
    'Vehicle Body Type': 'object',
    'Vehicle Make': 'object',
    'Issuing Agency': 'object',
    'Street Code1': 'Int64',
    'Street Code2': 'Int64',
    'Street Code3': 'Int64',
    'Violation Location': 'Int64',
    'Violation Precinct': 'Int64',
    'Issuer Precinct': 'Int64',
    'Issuer Code': 'Int64',
    'Issuer Command': 'object',
    'Issuer Squad': 'object',
    'Violation Time': 'object',
    'Time First Observed': 'object',
    'Violation County': 'object',
    'Violation In Front Of Or Opposite': 'object',
    'House Number': 'object',
    'Street Name': 'object',
    'Intersecting Street': 'object',
    'Law Section': 'Int64',
    'Sub Division': 'object',
    'Violation Legal Code': 'object',
    'Days Parking In Effect': 'object',
    'From Hours In Effect': 'object',
    'To Hours In Effect': 'object',
    'Vehicle Color': 'object',
    'Unregistered Vehicle?': 'object',
    'Vehicle Year': 'Int64',
    'Meter Number': 'object',
    'Feet From Curb': 'float64',
    'Violation Post Code': 'object',
    'Violation Description': 'object',
    'No Standing or Stopping Violation': 'object',
    'Hydrant Violation': 'object',
    'Double Parking Violation': 'object'
}

# Specify date columns
parse_dates = ['Issue Date', 'Vehicle Expiration Date', 'Date First Observed']

column_mapping = {
    'Number': 'House Number',
    'Street': 'Street Name'
    # Add other mappings if needed
}

#files = ["2014", "2015", "2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024_april"]
files = ["Parking_Violations_Issued_-_Fiscal_Year_2024_20240524"]

for file in files:
    # Load the dataset with specified dtypes and parse_dates
    df = pd.read_csv(f"data\\parking-violations\\{file}.csv", dtype=dtypes, parse_dates=parse_dates)
    print(file)
    #print(df.head())

    # Rename columns according to the mapping
    df.rename(columns=column_mapping, inplace=True)

    for col, dtype in dtypes.items():
        if col in df.columns:
            if dtype == 'Int64':
                df[col] = df[col].fillna(-1).astype('int64')    # take care of nan values in integer columns
            elif dtype == 'object':
                df[col] = df[col].fillna('').astype(str)    # take care of nan values in object columns

    # Convert to Parquet
    print("Converting to Parquet...")
    df.to_parquet(f'data/{file}.parquet')  # , engine='pyarrow')
    #df.to_parquet(f'data\\{file}_nocomp.parquet', compression=None)

    # Convert to HDF5
    print("Converting to HDF5...")
    #df.to_hdf(f'data\\{file}_nocomp.h5', key='df', mode='w')
    df.to_hdf(f'data/{file}.h5', key='df', mode='w', complevel=9, complib='blosc') #, format="table")

    print("done")
    # Compare file sizes
    csv_size = os.path.getsize(f'data\\parking-violations\\{file}.csv')
    parquet_size = os.path.getsize(f'data/{file}.parquet')
    hdf_size = os.path.getsize(f'data/{file}.h5')

    # print size in MB
    print(f"CSV file size: {csv_size/1024/1024} MB")
    print(f"Parquet file size: {parquet_size / 1024 / 1024} MB")
    print(f"HDF5 file size: {hdf_size / 1024 / 1024} MB")
    print("---------------------------------------------------")