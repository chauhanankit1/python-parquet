from vascomodel import *
import pandas as pd

from exasol.ExaTest import exasol_connection

DB_URI = ""

tables = get_vasco_tables("S_B219404C_CF34_47BA_A4E2_698ED9E7A16C_CORE",
                          "S_B219404C_CF34_47BA_A4E2_698ED9E7A16C_MINING")

query = VascoSelect(
    columns=[tables.unittypes.c.uy_id,tables.unittypes.c.uy_name],
    from_obj=tables.unittypes
)

print(raw_sql(query))

with exasol_connection(DB_URI) as con:
    cursor = con.cursor()
    cursor.execute(raw_sql(query))
    result = cursor.fetchallarrow(strings_as_dictionary=True)
    df = result.to_pandas(date_as_object=True)
    df1 = pd.DataFrame(df)
    print(df1.shape)
    print(df1.size)
    print(df1.tail())
