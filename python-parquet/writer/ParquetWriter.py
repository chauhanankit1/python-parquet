import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


def write(df,path='output.parquet'):
    table = pa.Table.from_pandas(df, preserve_index=True)
    pq.write_table(table, path, compression='snappy')


def read(path):
    df = pd.read_parquet(path, engine='pyarrow')
    return df


def main():
    print("In Python writer file")
    path = '~/Desktop/Downloads/avgdmd.parquet'
    df = read(path)
    print("Writing df to python-parquet file")
    write(df)


if __name__ == '__main__':
    main()
