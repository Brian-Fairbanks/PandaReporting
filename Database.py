import pyodbc
import sqlalchemy
from sqlalchemy.engine import URL
import pandas as pd
from pandasgui import show
from tqdm import tqdm


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
                "Distance_to_S01_in_miles",
                "Distance_to_S02_in_miles",
                "Distance_to_S03_in_miles",
                "Distance_to_S04_in_miles",
                "Distance_to_S05_in_miles",
                "Distance_to_S06_in_miles",
                "Distance_to_S07_in_miles",
                "Distance_to_S08_in_miles",
                "Distance_to_S09_in_miles",
            ]
        ]
        uniqueIncidents = uniqueIncidents.drop_duplicates(subset=["Incident_Number"])
        # show(uniqueIncidents)

        # will not work, as pandas cannot upsert over primary keys
        # uniqueIncidents.to_sql(
        #     "FireIncidents", self.engine, if_exists="append", index=False
        # )

        self.insertToTable(uniqueIncidents, "FireIncidents")

    def insertToFireUnits(self, df):
        # get array of unique incident numbers
        unitCalls = df[
            [
                "Incident_Number",
                "Unit",
                "Station",
                "Status",
                "Department",
                "Frontline_Status",
                "Location_At_Assign_Time",
                "First_Assign",
                "FirstArrived",
                "Unit_Assigned",
                "Unit_Enroute",
                "Unit_Staged",
                "Unit_Arrived",
                "Unit_Cleared",
                "Unit_Disposition",
                "Unit_Cancel_Reason",
                "Unit_Type",
                "Bucket_Type",
                "Concurrent_Usage",
                "Assigned_at_Station",
            ]
        ]
        # unitCalls = unitCalls.drop_duplicates(subset=["Incident_Number"])
        unitCalls["First_Assign"] = unitCalls["First_Assign"] == "Yes"
        unitCalls["FirstArrived"] = unitCalls["FirstArrived"] == "Yes"
        show(unitCalls)
        self.insertToTable(unitCalls, "FireUnits")

    # since pandas to_sql will not allow for upserts on primary key violations
    # we will go ahead and write a much slower alternative
    def insertToTable(self, df, table):
        """Dataframe: df - entire table
        String: table - name"""
        # iterate through every row of the dataframe and upsert into the database
        # df.apply(lambda row: self.upsert(row, table), axis=1)

        # with tqdm(total=len(df), desc=f"Inserting into: {table}") as loading:
        skipped = []
        for i in tqdm(range(len(df)), desc=f"Inserting into: {table}"):
            # print(df.iloc[i][0])
            try:
                df.iloc[i : i + 1].to_sql(
                    name=table, if_exists="append", con=self.engine, index=False
                )
            except sqlalchemy.exc.IntegrityError:
                skipped.append(f"{df.iloc[i][0]} - {df.iloc[i][1]}")
                pass
        if len(skipped) > 0:
            print(
                f"Incidents skipped Due to Existing Primary Keys: {len(skipped)}/{len(df)}\n{skipped}"
            )

    # testing
    def insertTest(self, df):
        self.insertToTable(df, "Test")
        # df.to_sql("Test", self.engine, if_exists="append", index=False)
        # res = self.engine.execute("SELECT * FROM dbo.Test;")
        # for i in res:
        #     print(i)

    # main reason for this... run all included functions
    def insertDF(self, df):
        self.insertToFireIncident(df)
        self.insertToFireUnits(df)


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
