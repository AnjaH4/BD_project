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

files = ["2023_augmented", "2024_augmented", "2014_augmented", "2015_augmented", "2016_augmented", "2017_augmented", "2018_augmented", "2019_augmented", "2020_augmented", "2021_augmented", "2022_augmented"]
#files = ["2014_augmented"]
# files = ["Parking_Violations_Issued_-_Fiscal_Year_2024_20240524"]
CSV_PATH = "/d/hpc/projects/FRI/bigdata/students/mk75264/data/augmented"
HDF5_PATH = "/d/hpc/projects/FRI/bigdata/students/mk75264/data/augmented/hdf5"

#CSV_PATH = "/home/anjah/Documents/mag/BD/project/BD_project/data/augmented/augmented"
#HDF5_PATH = "/home/anjah/Documents/mag/BD/project/BD_project/data/augmented/augmented"


for file in files:
    # Load the dataset with specified dtypes and parse_dates
    df = pd.read_csv(f"{CSV_PATH}/{file}.csv", dtype=dtypes, parse_dates=parse_dates)
    print(file)
    # print(df.head())

    # Rename columns according to the mapping
    df.rename(columns=column_mapping, inplace=True)

    for col, dtype in dtypes.items():
        if col in df.columns:
            if dtype == 'Int64':
                df[col] = df[col].fillna(-1).astype('int64')  # take care of nan values in integer columns
            elif dtype == 'object':
                df[col] = df[col].fillna('').astype(str)  # take care of nan values in object columns

    print("Converting to HDF5...")
    df.to_hdf(f'{HDF5_PATH}/{file}.h5', key='df', mode='w', complevel=9, complib='blosc')  # , format="table")

    print("done")
