from contextlib import contextmanager
import sys
import pandas as pd

if ((3, 0) <= sys.version_info <= (3, 9)):
    from urllib.parse import urlparse
elif ((2, 0) <= sys.version_info <= (2, 9)):
    from urlparse import urlparse
from turbodbc import Megabytes, connect, make_options
DB_URI = "exa+turbodbc://<user>:<pswd>@demodb.exasol.com?driver=Exasol_latest&autocommit=True"

@contextmanager
def exasol_connection(db_uri, commit=True):
    db_params = urlparse(db_uri)
    options = make_options(read_buffer_size=Megabytes(100))
    connection = connect(
        driver="EXAODBC",
        EXAUID=db_params.username,
        EXAPWD=db_params.password,
        EXAHOST=db_params.hostname,
        turbodbc_options=options,
    )
    yield connection
    if commit:
        connection.commit()

with exasol_connection(DB_URI) as con:
    cursor = con.cursor()
    cursor.execute("SELECT * FROM RETAIL.SALES LIMIT 1000 ")
    result = cursor.fetchallarrow(strings_as_dictionary=True)
    df = result.to_pandas(date_as_object=True)
    df1 = pd.DataFrame(df)
    print(df1.shape)
    print(df1.size)
    print(df1.tail())
