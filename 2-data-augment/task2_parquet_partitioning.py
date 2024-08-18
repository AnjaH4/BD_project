from dask.distributed import Client, LocalCluster
import dask.dataframe as dd
import pyarrow as pa
import pyarrow.parquet as pq
import gc

def main():
    cluster = LocalCluster(n_workers=4, threads_per_worker=1, memory_limit="2GB")
    client = Client(cluster)

    base_path = "/d/hpc/projects/FRI/bigdata/students/mk75264/data/augmented"
    glob_pattern = f"{base_path}/*.csv"  # Find all CSV files
    
    ddf = dd.read_csv(glob_pattern, assume_missing=True, dtype="object", blocksize="64MB")

    ddf.to_parquet(
        f"{base_path}/parquet_files/", engine="pyarrow", overwrite=True, compression="snappy"
    )

    client.close()
    gc.collect()

if __name__ == '__main__':
    main()

