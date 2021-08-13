import pyarrow.parquet as pq

table2 = pq.read_table('tracks.parquet')

parquet_file = pq.ParquetFile('tracks.parquet')

for batch in parquet_file.iter_batches(2):
    print("hi")


