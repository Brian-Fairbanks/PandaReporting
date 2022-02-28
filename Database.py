import pyodbc

conn = pyodbc.connect(
    driver="{SQL Server}",
    host="CRM22G3",
    database="master",
    trusted_connection="yes",
)

cursor = conn.cursor()

cursor.execute(
    """
insert into dbo.Test (data)
values ('input from python'),
    ('and a second as well');
"""
)

conn.commit()

cursor.execute("SELECT * FROM dbo.Test;")

for i in cursor:
    print(i)
