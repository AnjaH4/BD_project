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
        #'Vehicle Make', 
        #'Issuing Agency', 
        #'Violation Time', #!! change
        #'Vehicle Year', 
        #'Time First Observed', 
        'Violation County', # we have alread
        #'Street'
        #'PRCP'
    ]
    df = df[columns_to_keep]
    df = df.dropna()
    print(df.head())

    # TODO: Here add appropriate locations

    df = df.persist()
    df["n_tickets"] = 1
    #df.drop(columns=["PRCP"], inplace=True)

    print("Grouping by date and county...")
    df = df.groupby(["Issue Date", "Violation County"]).sum()

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

    df = df.dropna()

    def augment_data(df):
        boroughs_dict = {'QNS': 'Q','QN' : 'Q', 'KINGS': 'K', 'BK' : 'K', 'BRONX':'BX', 'MN' : 'NY','ST' : 'R' }
        # SCHOOLS
        with open(os.path.join(SCHOOLS_DIR, f'borough_school_counts.json'), 'r') as f:
            schools_dict = json.load(f)

        schools_dict['NY'] = schools_dict.pop('M')
        schools_dict['BX'] = schools_dict.pop('X')
        # Add the number of schools in the borough where the violation occurred
        df['Schools'] = df['Violation County'].map(schools_dict)

        # EVENTS
        events_df = pd.read_csv(os.path.join(EVENTS_DIR, f'events_count.csv'))
        # Change the date column to datetime
        events_df['date'] = pd.to_datetime(events_df['date'])
        events_df.rename(columns={'date': 'event_date', 'borough': 'event_borough'}, inplace=True)

        # BUSINESSES
        with open(os.path.join(BUSINESS_DIR, f'borough_business_counts.json'), 'r') as f:
            business_dict = json.load(f)
        # Add the number of businesses in the borough where the violation occurred
        df['Businesses'] = df['Violation County'].map(business_dict)

        # ATTRACTIONS
        with open(os.path.join(ATTRACTIONS_DIR, f'borough_attractions_counts.json'), 'r') as f:
            attractions_dict = json.load(f)
        # Add the number of attractions in the borough where the violation occurred
        df['Attractions'] = df['Violation County'].map(attractions_dict)

        # WEATHER
        weather_names = os.listdir(WEATHER_DIR)
        weather_names = [file for file in weather_names if file.endswith('.csv')]
        # Read them all in one dataframe
        weather_df = pd.concat([pd.read_csv(os.path.join(WEATHER_DIR, file)) for file in weather_names])
        weather_df['DATE'] = pd.to_datetime(weather_df['DATE'])
        weather_df.sort_values(by='DATE')

        # Merge the parking violations data with the weather data
        merged_df = pd.merge(df, weather_df, left_on='Issue Date', right_on='DATE', how='left')

        # Drop the DATE column from weather data as it's redundant
        merged_df = merged_df.drop(columns=['DATE', 'TAVG', 'STATION'])

        # 5. EVENTS: # merge on both date and borough, but keep only the count column
        merged_df = pd.merge(merged_df, events_df, left_on=['Issue Date', 'Violation County'], right_on=['event_date', 'event_borough'], how='left')
        merged_df.drop(columns=[ 'event_date', 'event_borough', 'events_count', 'total_events_by_borough', 'total_events_by_day'], inplace=True)
        merged_df.set_index('Issue Date', inplace=True)
        merged_df.sort_index(inplace=True) # important for time series data

        return merged_df
    
    print("Augmenting data...")
    merged_df = augment_data(df)
    df = merged_df
    del merged_df

    # Get dummies for the county
    df = pd.get_dummies(df, columns=['Violation County']).astype(float)

    # PREDICTIONS
    X = df
    y = X.pop('n_tickets')

    print("Saving data...")

    # save df and X, y to pickle files
    if save_pickle:
        df.to_pickle('data/ml/df.pkl')
        X.to_pickle('data/ml/X.pkl')
        y.to_pickle('data/ml/y.pkl')

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

    def plot_predictions(y_test, y_pred, model_name, file_type, title):
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.scatter(y_test, y_pred, color='blue', alpha=0.5) 
        ax.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2, color='red')
        ax.set_xlabel("True Values")  
        ax.set_ylabel("Predicted Values")
        ax.set_title(title)  
        ax.grid(True)
        fig.tight_layout()
        plt.savefig(f"plots/results_{model_name}_{file_type}.png")
        plt.show()

    def run_xgboost():
        print("XGBoost results:")
        start_time = time.time()

        clf = xgb.XGBRegressor()
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        plot_predictions(y_test, y_pred, "XGBoost", file_type=file_type, title=f"XGBoost RMSE: {rmse :.2f}")
        print("RMSE: ", rmse)
        end_time = time.time()
        print(f"Time for fit and predict with XGBoost: {end_time - start_time:.2f} seconds")
        time_dict['xgboost'] = end_time - start_time

        # Feature importance
        clf.get_booster().feature_names = feature_names
        plot_importance(clf.get_booster(), importance_type='weight')
        plt.savefig(f"plots/feature_importance_daily_tickets_{split_date}_{file_type}.png", dpi=300)
        pyplot.show()

    def run_linear_regression():
        start_time = time.time()

        clf = LinearRegression() #fit_intercept=0
        clf.fit(X_train, y_train)
        y_pred = clf.predict(X_test)
        y_pred = y_pred.compute()

        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        plot_predictions(
            y_test, y_pred, "LinearRegression", file_type=file_type, title=f"LR RMSE: {rmse:.2f}"
        )

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
        plot_predictions(
            y_test, y_pred, "SGDRegressor", file_type=file_type, title=f"SGD RMSE: {rmse:.2f}"
        )

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
    with open(f'data/ml/time_dict_w_{n_workers}_mem_{memory_limit}.json', 'w') as f:
        json.dump(time_dict, f)


    