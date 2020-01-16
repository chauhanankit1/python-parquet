from functools import partial
from storefact import get_store_from_url
from kartothek.io.eager import read_table
from kartothek.io.iter import read_dataset_as_dataframes__iterator
from kartothek.io.dask.delayed import read_dataset_as_delayed
import pandas as pd
import dask

percent_encoded_sas_token = ""
store_factory = partial(get_store_from_url,
                        f"hazure://karthotekfiledump:+{percent_encoded_sas_token}+@ktkfiles?use_sas=true&create_if_missing=false")
# Approach 1
# read all at once
df = read_table(dataset_uuid="order_proposals_a6e8aef43203", store=store_factory, table="order_proposals")
# write aggregated df to disk
df.to_parquet('sample.parquet', engine='pyarrow')

# Approach 2
# read iteratively
df_frames = pd.DataFrame()
for partition_index, df_dict in enumerate(
        read_dataset_as_dataframes__iterator(dataset_uuid="order_proposals_a6e8aef43203", store=store_factory)):
    # print(f"Partition #{partition_index}")
    for table_name, table_df in df_dict.items():
        # print(f"Table: {table_name}. Data: \n{table_df}")
        df_frames = df_frames.append(table_df)
# write aggregated df to disk
df_frames.to_parquet('sample1.parquet', engine='pyarrow')

# Approach 3
# read with dask.delayed
print("Dask Delayed execution !")
df_delayed = pd.DataFrame()
tasks = read_dataset_as_delayed(dataset_uuid="order_proposals_a6e8aef43203", store=store_factory)
for task in tasks:
    data = dask.compute(task)
    for x, y in data[0].items():
        df_delayed = df_delayed.append(y)

# write aggregated df to disk
df_delayed.to_parquet('sample2.parquet', engine='pyarrow')
