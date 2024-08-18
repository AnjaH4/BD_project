import glob
import pandas as pd

filepaths = glob.glob('/d/hpc/projects/FRI/bigdata/students/mk75264/data/augmented/*.parquet')

for filepath in filepaths:
    # Read the augmented data
    print(f"Reading {filepath}")
    augmented_data = pd.read_parquet(filepath)

    # Save the augmented data to a CSV file
    augmented_data.to_csv(filepath.replace('.parquet', '.csv'), index=False)
