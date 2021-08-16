import argparse

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import csv
from pyarrow.csv import ConvertOptions


def parse_args():
    parser = argparse.ArgumentParser(description='Convert CSV to Parquet')
    parser.add_argument('-c', '--csv', help="the path to the csv file to be converted.")
    return parser.parse_args()


def convert(csv_file):
    out_file = str(csv_file).split('.')[0] + '.parquet'
    convert_opts = ConvertOptions(timestamp_parsers=['%Y-%m-%d'], column_types={'date': pa.date32()})

    reader = csv.open_csv(csv_file, convert_options=convert_opts)

    writer = pq.ParquetWriter(csv_file, reader.schema)

    table = pa.Table.from_batches(reader, reader.schema)
    writer.write_table(table)


if __name__ == '__main__':
    args = parse_args()

    if args.csv is None:
        print("Where's the CSV file dude")
        quit()

    convert(args.csv)
