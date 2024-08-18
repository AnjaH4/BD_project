import os
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from datetime import datetime

def main(args):
    # Set up Dask cluster and client
    cluster = LocalCluster(n_workers=1, threads_per_worker=4, memory_limit="1GB")
    client = Client(cluster)

    # Read HDF5 files into a Dask DataFrame
    df = dd.read_hdf(os.path.join(args.path, "*.h5"), key='df').sample(frac=0.0001, random_state=42)

    # Time the data processing
    start = datetime.now()
    print("Start time:", start)

    # Compute the top 10 states by count
    top_10_states = df['Registration State'].compute()

    # Extract the top 10 states
    #top_10_states_list = top_10_states.index.tolist()

    # Filter the Dask DataFrame for the top 10 states
    #df_top_10_dask = df[df['Registration State'].isin(top_10_states_list)]

    # Convert to Pandas DataFrame for further analysis
    df_top_10 = df_top_10_dask.compute()

    end = datetime.now()
    time = end - start

    print(f"Time to get data points for top 10 states: {time}")

    # Close the Dask client
    client.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Process HDF5 files with Dask.")
    parser.add_argument('-p', dest='path',type=str, help='Path to the HDF5 files')
    args = parser.parse_args()
    main(args)