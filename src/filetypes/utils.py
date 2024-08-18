import polars as pl
#import vaex


def transform_parquet(file):
    parquet_file = file.split('/')[-1].replace('.csv', '.parquet')
    #parquet_file = 'data.parquet'
    df = pl.scan_csv(file, ignore_errors=True)
    df.sink_parquet(('parquet/' + parquet_file), compression='snappy', row_group_size=100_000)
    