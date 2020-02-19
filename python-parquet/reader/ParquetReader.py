from functools import partial
from tempfile import TemporaryDirectory
from storefact import get_store_from_url
from kartothek.io.eager import read_table


dataset_dir = TemporaryDirectory()
store_factory = partial(get_store_from_url, "hfs:///Users/1019021/Learn/python-python-parquet/resources")

print(read_table("order_proposals_a6e8aef43203", store_factory, table="order_proposals"))