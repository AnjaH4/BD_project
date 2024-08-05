#transform csv files to hdf5 files
import glob
import pandas as pd

filepaths = glob.glob('/d/hpc/projects/FRI/bigdata/students/mk75264/data/augmented/*.csv')

for filepath in filepaths:
    # Read the augmented data
    print(f"Reading {filepath}")
    augmented_data = pd.read_csv(filepath)

    # Save the augmented data to a HDF5 file
    augmented_data.to_hdf(filepath.replace('.csv', '.h5'), key='data', mode='w')

