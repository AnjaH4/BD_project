import os
from dask.distributed import Client
from dask.distributed import LocalCluster
import dask.dataframe as dd
from datetime import datetime
import argparse

def main(args):
    cluster = LocalCluster(n_workers=6,  threads_per_worker=4, memory_limit="10GB")
    client = Client(cluster)

    df = dd.read_parquet(os.path.join(args.path, "*.parquet")).sample(frac=0.0000000001, random_state=42)

    #time to read the data
    start = datetime.now()
    print(start)
    #ram usage for computation
    #ram_usage = df.memory_usage(deep=True).sum().compute()
    #ram needed for the following computation
    top_10_states = df['Registration State'].value_counts().nlargest(10).compute()
    # Extract the top 10 states
    top_10_states_list = top_10_states.index.tolist()
    # Filter the Dask DataFrame for the top 10 states
    df_top_10_dask = df[df['Registration State'].isin(top_10_states_list)]
    # Convert to Pandas DataFrame for plotting
    df_top_10 = df_top_10_dask.compute()
    end = datetime.now()
    time = end - start

    print(f"Time get data points for top 10 states: {time}")




if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', dest='path', type=str, default=None)
    args = parser.parse_args()



    main(args)







