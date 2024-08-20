import numpy as np
import matplotlib.pyplot as plt
import dask.dataframe as dd
import os
import json
from dask_ml.preprocessing import StandardScaler
from dask_ml.linear_model import LinearRegression
from dask.distributed import LocalCluster, Client
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt
import xgboost as xgb
import time
import pandas as pd
from memory_profiler import memory_usage
from sklearn.linear_model import SGDRegressor
from xgboost import plot_importance
from matplotlib import pyplot

# SETUP
# Cluster configuration
n_workers = 3
memory_limit = '9GB'
file_type = 'parquet'

save_pickle = True


time_dict = {
    "n_workers": n_workers,
    "memory_limit": memory_limit,
    "file_type": file_type
}
#DATA_DIR = 'data/parquet_files'
DATA_DIR = '/d/hpc/projects/FRI/bigdata/students/mk75264/data/augmented/parquet_files'  # Update this path accordingly
# data directories
WEATHER_DIR = 'data/weather/'
AUGMENTED_DIR = 'data/augmented/' # original + augmented data
SCHOOLS_DIR = 'data/schools/'
ATTRACTIONS_DIR = 'data/attractions/'
BUSINESS_DIR = 'data/businesses/'
EVENTS_DIR = 'data/events/'

def run_all():
    print("Reading parquet files...")
    start = time.time()
    df = dd.read_parquet(DATA_DIR + '/*.parquet', engine='pyarrow', )
    end = time.time()
    print("Time to read parquet files: ", end-start)

    time_dict['read'] = end - start

    # Necessary repartitioning
    df = df.repartition(npartitions=10)

    # Columns to keep
    columns_to_keep = [
        'Issue Date', 
        #'Plate ID', 
        #'Violation Code', 
        #'Registration State', #- for plotting
        #'Plate Type', 
        #'Vehicle Body Type', 
        #'Vehicle Color', 
        'Vehicle Make', 
        #'Issuing Agency', 
        #'Violation Time', #!! change
        #'Vehicle Year', 
        #'Time First Observed', 
        #'Violation County', # we have alread
        #'Street'
        #'n_tickets',
        'Schools',
        'Businesses',
        'Attractions',
        'AWND',
        'PRCP',
        'SNOW',
        'TMAX',
        'TMIN',
        'borough_normalized_events',
        'day_normalized_events',
        ]
    

    
    df = df[columns_to_keep]
    df = df.dropna()
    print(df.head())

    # keep only the top 10 vehicle makes
    top_vehicle_makes = df['Vehicle Make'].value_counts().nlargest(10).index
    df = df[df['Vehicle Make'].isin(top_vehicle_makes)]


    df = df.persist()
    df["n_tickets"] = 1
    #df.drop(columns=["PRCP"], inplace=True)

    print("Grouping by date and vehicle_make...")
    df = df.groupby(['Issue Date', 'Vehicle Make']).agg({
        'n_tickets': 'sum', 
        'Schools': 'mean',
        'Businesses': 'mean',
        'Attractions': 'mean',
        'AWND': 'mean',
        'PRCP': 'mean',
        'SNOW': 'mean',
        'TMAX': 'mean',
        'TMIN': 'mean',
        'borough_normalized_events': 'mean',
        'day_normalized_events': 'mean' 
    }).reset_index()

    df_pred = df.compute().reset_index() # compute the groupby
    print("df_pred: ", df_pred.head())

    df_pred.sort_values(by="n_tickets")

    df = df_pred

    # Convert the selected columns to correct type
    df['Issue Date'] = dd.to_datetime(df['Issue Date'], format="mixed", errors='coerce')
    # for hourly data:
    #df['Violation Time'] = df['Violation Time'].str.slice(stop=-1) + ' ' + df['Violation Time'].str.slice(start=-1).replace({'A': 'AM', 'P': 'PM'})
    #df['Violation Time'] = dd.to_datetime(df['Violation Time'], format='%I%M %p', errors='coerce')

    # Remove 'A' and 'P' from the end of the time, add ' AM' or ' PM' accordingly
    #df['violation_hour'] = df['Violation Time'].dt.hour

    # Create columns for day, month and year
    df['day'] = df['Issue Date'].dt.day
    df['month'] = df['Issue Date'].dt.month
    df['year'] = df['Issue Date'].dt.year
    df['day_of_week'] = df['Issue Date'].dt.dayofweek

    
    print("Augmenting data...")
    

    # Get dummies for the county
    df = pd.get_dummies(df, columns=['Vehicle Make']).astype(float)

    # PREDICTIONS
    X = df
    y = X.pop('n_tickets')

    print("Saving data...")

    # save df and X, y to pickle files
    if save_pickle:
        df.to_pickle('data/ml/df_vehicle_make.pkl')
        X.to_pickle('data/ml/X_vehicle_make.pkl')
        y.to_pickle('data/ml/y_vehicle_make.pkl')

    # Choose the start and end date
    split_date = '2019-07-01'

    X_train, X_test = X.loc[:split_date], X.loc[split_date:]
    y_train, y_test = y.loc[:split_date], y.loc[split_date:]

    X_train = X_train.fillna(X_train.mean()) 
    X_test = X_test.fillna(X_test.mean())
    # or:
    #X_train = X_train.fillna(0) 
    #X_test = X_test.fillna(0)

    # Save feature names to later plot feature importance
    feature_names = [f'{name}' for name in X_train.columns]

    print("Starting the machine learning models...")
    # Trasnsorming to dask arrays
    X_train = dd.from_pandas(X_train, npartitions=10)
    X_test = dd.from_pandas(X_test, npartitions=10)
    y_train = dd.from_pandas(y_train, npartitions=10)
    y_test = dd.from_pandas(y_test, npartitions=10)

    X_train = X_train.to_dask_array()
    X_test = X_test.to_dask_array()
    y_train = y_train.to_dask_array()
    y_test = y_test.compute().values

    # Scaling the data
    scaler_train = StandardScaler()
    scaler_test = StandardScaler()

    X_train = scaler_train.fit_transform(X_train)
    X_test = scaler_test.fit_transform(X_test)


    def run_xgboost():
        print("XGBoost results:")
        start_time = time.time()

        clf = xgb.XGBRegressor()
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        print("RMSE: ", rmse)
        end_time = time.time()
        print(f"Time for fit and predict with XGBoost: {end_time - start_time:.2f} seconds")
        time_dict['xgboost'] = end_time - start_time

        pyplot.show()

    def run_linear_regression():
        start_time = time.time()

        clf = LinearRegression() #fit_intercept=0
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        y_pred = y_pred.compute()

        rmse = np.sqrt(mean_squared_error(y_test, y_pred))

        end_time = time.time()

        print("LR RMSE: ", rmse)
        print(f"Time to fit and predict for Linear Regression: {end_time - start_time:.2f} seconds")
        time_dict['linear_regression'] = end_time - start_time
    
    def run_sgd():
        start_time = time.time()
        clf = SGDRegressor()  # fit_intercept=0 if needed
        for i in range(10):
            clf.partial_fit(X_train, y_train)

        y_pred = clf.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))

        end_time = time.time()
        print("SGD RMSE: ", rmse)
        print(f"Time to fit and predict for SGDRegressor: {end_time - start_time:.2f} seconds")
        time_dict['sgd'] = end_time - start_time

    # Run all models
    run_linear_regression()
    run_xgboost()
    run_sgd()

if __name__ == '__main__':
    print("Starting the machine learning...")
    cluster = LocalCluster(n_workers=n_workers, memory_limit=memory_limit)
    client = Client(cluster)
    print(client)
    print("Cluster created")

    start_time = time.time()
    mem_usage = memory_usage(run_all(), max_usage=True)
    end_time = time.time()

    print(f"Total time: {end_time - start_time:.2f} seconds")
    print(f"Peak memory usage: {mem_usage[0]:.2f} MB")
    time_dict['total'] = end_time - start_time
    time_dict['memory_usage'] = mem_usage

    # save time_dict to json
    with open(f'data/ml/time_dict_vehicle_make_w_{n_workers}_mem_{memory_limit}.json', 'w') as f:
        json.dump(time_dict, f)


    