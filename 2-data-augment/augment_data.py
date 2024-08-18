import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
#import dask.dataframe as dd
import dask.dataframe as dd
import os
import pandas as pd
import matplotlib.pyplot as plt
import json

DATA_DIR = "data/parking-violations"

# data directories
DATA_DIR = 'data/parking-violations/'
WEATHER_DIR = 'data/weather/'
AUGMENTED_DIR = 'data/augmented/'

SCHOOLS_DIR = 'data/schools/'


if __name__ == '__main__':
    # SCHOOL PART
    # Load school data
    schools = pd.read_csv(SCHOOLS_DIR + 'Public_School_Locations.csv')
    print(schools.head())
    print(schools.columns)

    # Number of unique boroughs
    unique_boroughs = schools.BORO.unique()
    print(f"School boroughs: {unique_boroughs}")

    # EDA:  nan data for boroughs, percentage of nan data
    #print(schools['BORO'].isna().sum())
    #print(len(schools))
    print(round(schools['BORO'].isna().sum() / len(schools) * 100, 2), "% of data is Nan")

    # Number of schools in each borough
    borough_school_counts = schools['BORO'].value_counts().to_dict()
    print(borough_school_counts)
        
    # Save the dictionary to a JSON file
    with open(SCHOOLS_DIR + 'borough_school_counts.json', 'w') as file:
        json.dump(borough_school_counts, file)

    print("Saved borough school counts to 'borough_school_counts.json'")

    # 
