import pyodbc
import sqlalchemy
from sqlalchemy.engine import URL
import pandas as pd
from pandasgui import show


class SQLDatabase:
    """a connection to a SQL Database, and associated functions for insertion of required data"""

    def __init__(self):
        connectionString = "DRIVER={SQL Server};SERVER=CRM22G3;DATABASE=master;"
        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": connectionString}
        )
        self.engine = sqlalchemy.create_engine(connection_url)

    # write the DataFrame to a table in the sql database

    def insertToFireIncident(self, df):
        # get array of unique incident numbers
        uniqueIncidents = df[
            [
                "Incident_Number",
                "Calltaker_Agency",
                "Address_of_Incident",
                "City",
                "Jurisdiction",
                "Response_Area",
                "Problem",
                "Incident_Type",
                "Response_Plan",
                "Priority_Description",
                "Alarm_Level",
                "Map_Info",
                "X_Long",
                "Y_Lat",
                "ESD02_Shift",
                "Phone_Pickup_Time",
                "Call_Entered_in_Queue",
                "First_Unit_Assigned",
                "First_Unit_Enroute",
                "First_Unit_Staged",
                "First_Unit_Arrived",
                "Call_Closed",
                "Last_Unit_Cleared",
                "Incident_Call_Disposition",
                "Incident_Call_Reason",
                "EMS_Incident_Numbers",
                "IsESD17",
                "isETJ",
                "isCOP",
                "People_Per_Mile",
                "Population_Classification",
                "Closest_Station",
            ]
        ]
        uniqueIncidents = uniqueIncidents.drop_duplicates(subset=["Incident_Number"])
        show(uniqueIncidents)
        uniqueIncidents.to_sql(
            "FireIncidents", self.engine, if_exists="append", index=False
        )

    def insertTest(self, df):
        df.to_sql("Test", self.engine, if_exists="append", index=False)
        res = self.engine.execute("SELECT * FROM dbo.Test;")
        for i in res:
            print(i)


# =================================================================
#      Testing Code
# =================================================================
if __name__ == "__main__":
    # set up simple dataframe to test insertion
    df = pd.DataFrame([["test from dataframe"], ["pandas will work"]], columns=["data"])

    db = SQLDatabase()
    db.insertTest(df)

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
