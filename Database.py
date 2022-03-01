import pyodbc
import sqlalchemy
from sqlalchemy.engine import URL
import pandas as pd


df = pd.DataFrame([["test from dataframe"], ["pandas will work"]], columns=["data"])

connectionString = "DRIVER={SQL Server};SERVER=CRM22G3;DATABASE=master;"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connectionString})
engine = sqlalchemy.create_engine(connection_url)

# write the DataFrame to a table in the sql database

with engine.connect() as con:
    df.to_sql("Test", engine, if_exists="append")

    res = engine.execute("SELECT * FROM dbo.Test;")
    for i in res:
        print(i)


# conn = pyodbc.connect(
#     driver="{SQL Server}",
#     host="CRM22G3",
#     database="master",
#     trusted_connection="yes",
# )
# conn = pyodbc.connect("DRIVER={SQL Server};SERVER=CRM22G3;DATABASE=master;")


# cursor = conn.cursor()

# cursor.execute(
#     """
# insert into dbo.Test (data)
# values ('input from python'),
#     ('and a second as well');
# """
# )

# conn.commit()

# cursor.execute("SELECT * FROM dbo.Test;")

# for i in cursor:
#     print(i)
