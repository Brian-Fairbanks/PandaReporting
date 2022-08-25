import pyodbc
import sqlalchemy
from sqlalchemy.engine import URL
import pandas as pd
# from pandasgui import show
from tqdm import tqdm

from dotenv import load_dotenv
from os import getenv


class SQLDatabase:
    """a connection to a SQL Database, and associated functions for insertion of required data"""

    def __init__(self):
        load_dotenv()
        drvr = getenv("DBDRVR")
        srvr = getenv("DBSRVR")
        dtbs = getenv("DBDTBS")

        connectionString = f"DRIVER={drvr};SERVER={srvr};DATABASE={dtbs};"
        print(connectionString)
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
                # "Response_Area",
                "AFD_Response_Box",
                "Problem",
                "Incident_Type",
                "Response_Plan",
                "Priority_Description",
                "Alarm_Level",
                "Map_Info",
                "X_Long",
                "Y_Lat",
                "ESD02_Shift",
                "call_delayed",
                "INC_Staged_As_Arrived",
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
                "is_walkup",
                "Incident_Call_Count",
                "Incident_ERF_Time",
                "Force_At_ERF_Time_of_Close",
            ]
        ]
        uniqueIncidents = uniqueIncidents.drop_duplicates(subset=["Incident_Number"])
        # show(uniqueIncidents)

        # will not work, as pandas cannot upsert over primary keys
        # uniqueIncidents.to_sql(
        #     "FireIncidents", self.engine, if_exists="append", index=False
        # )

        self.insertToTable(uniqueIncidents, "FireIncidents")

    def insertToEMSIncident(self, df):
        # get array of unique incident numbers
        uniqueIncidents = df[
            [
                "Incident_Number",
                "Incident_Status",
                "Calltaker_Agency",
                "Address_of_Incident",
                "Location_Name",
                "Apartment",
                "City",
                "State",
                "Zip",
                "County",
                "Jurisdiction",
                "AFD_Response_Box",
                "Problem",
                "Incident_Type",
                "Response_Plan",
                "Base_Response#",
                "Priority",
                "Priority_Description",
                "Priority_Description_Orig",
                "Map_Info",
                "X_Long",
                "Y_Lat",
                "ESD02_Shift",
                "call_delayed",
                "INC_Staged_As_Arrived",
                "Phone_Pickup_Time",
                "Ph_PU_Date",
                "Call_Entered_in_Queue",
                "First_Unit_Assigned",
                "First_Unit_Enroute",
                "First_Unit_Staged",
                "First_Unit_Arrived",
                "Call_Closed",
                "Last_Unit_Cleared",
                "Incident_Call_Disposition",
                "EMD_Code",
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
                "is_walkup",
                "Incident_Call_Count",
                "Incident_ERF_Time",
                "Force_At_ERF_Time_of_Close",
            ]
        ]
        uniqueIncidents = uniqueIncidents.drop_duplicates(subset=["Incident_Number"])
        # show(uniqueIncidents)

        self.insertToTable(uniqueIncidents, "EMSIncidents")

    def insertToFireUnits(self, df):
        # get array of unique incident numbers
        unitCalls = df[
            [
                "Incident_Number",
                "Unit",
                "Station",
                "Status",
                "Response_Status",
                "Department",
                "Frontline_Status",
                "Location_At_Assign_Time",
                "First_Assign",
                "FirstArrived",
                "First_Arrived_Esri",
                "UNIT_Staged_As_Arrived",
                "Unit_Assigned",
                "Unit_Enroute",
                "Unit_Staged",
                "Unit_Arrived",
                "Unit_Cleared",
                "Unit_Disposition",
                "Unit_Cancel_Reason",
                "Unit_Type",
                "Bucket_Type",
                "Assigned_at_Station",
                "Is_Closest_Station",
                "Unit_Usage_At_Time_of_Alarm",
                "Time_0_Active",
                "Time_1_Active",
                "Time_2_Active",
                "Time_3_Active",
                "Time_4_Active",
                "Time_5_Active",
                "Time_6_Active",
                "Time_7_Active",
                "Time_8_Active",
                "Time_9_Active",
                "Single_vs_Multi_Units_ONSC",
            ]
        ]
        # replace all instances of "yes" and "no" with "0,1"
        unitCalls.replace("Yes", 1, inplace=True)
        unitCalls.replace("No", 0, inplace=True)

        # show(unitCalls)
        self.insertToTable(unitCalls, "FireUnits")

    def insertToEMSUnits(self, df):
        # get array of unique incident numbers
        unitCalls = df[
            [
                "Station",
                "Status",
                "Response_Status",
                "Incident_Number",
                "Unit",
                "Department",
                "Frontline_Status",
                "Location_At_Assign_Time",
                "Longitude_at_Assign",
                "Latitude_at_Assign",
                "Primary_Flag",
                "FirstArrived",
                "First_Arrived_Esri",
                "UNIT_Staged_As_Arrived",
                "Unit_Assigned",
                "Unit_Enroute",
                "Unit_Staged",
                "Unit_Arrived",
                "At_Patient",
                "Delay_Avail",
                "Unit_Cleared",
                "Unit_Disposition",
                "Unit_Type",
                "Bucket_Type",
                "Assigned_at_Station",
                "Is_Closest_Station",
                "Unit_Usage_At_Time_of_Alarm",
                "Time_0_Active",
                "Time_1_Active",
                "Time_2_Active",
                "Time_3_Active",
                "Time_4_Active",
                "Time_5_Active",
                "Time_6_Active",
                "Time_7_Active",
                "Time_8_Active",
                "Time_9_Active",
                "Transport_Count",
                "Destination_Name",
                "Destination_Address",
                "Destination_City",
                "Destination_State",
                "Destination_Zip",
                "Time_Depart_Scene",
                "Time_At_Destination",
                "Time_Cleared_Destination",
                "Transport_Mode",
                "Transport_Protocol",
                "Single_vs_Multi_Units_ONSC",
            ]
        ]
        # replace all instances of "yes" and "no" with "0,1"
        unitCalls.replace("Yes", 1, inplace=True)
        unitCalls.replace("No", 0, inplace=True)
        # unitCalls = unitCalls.drop_duplicates(subset=["Incident_Number"])
        # unitCalls["First_Assign"] = unitCalls["First_Assign"] == "Yes"
        # unitCalls["FirstArrived"] = unitCalls["FirstArrived"] == "Yes"
        # show(unitCalls)
        self.insertToTable(unitCalls, "EMSUnits")

    # since pandas to_sql will not allow for upserts on primary key violations
    # we will go ahead and write a much slower alternative
    def insertToTable(self, df, table):
        """Dataframe: df - entire table
        String: table - name"""
        # iterate through every row of the dataframe and upsert into the database
        # df.apply(lambda row: self.upsert(row, table), axis=1)

        # with tqdm(total=len(df), desc=f"Inserting into: {table}") as loading:
        skipped = []
        errored = []
        errorRows = []
        for i in tqdm(range(len(df)), desc=f"Inserting into: {table}"):
            # print(df.iloc[i][0])
            try:
                df.iloc[i : i + 1].to_sql(
                    name=table, if_exists="append", con=self.engine, index=False
                )
            except sqlalchemy.exc.IntegrityError as err:
                inc = df.iloc[i]["Incident_Number"]
                skipped.append(f"{inc} - {err}")
                errorRows.append(i)
            except sqlalchemy.exc.ProgrammingError as err:
                inc = df.iloc[i]["Incident_Number"]
                errored.append(f"{inc} - {err}")
                errorRows.append(i)
            except sqlalchemy.exc.DataError as err:
                inc = df.iloc[i]["Incident_Number"]
                errored.append(f"{inc} - {err}")
                errorRows.append(i)
            except Exception as err:
                print(" - Something went wrong! - \n", err)

        if len(skipped) > 0:
            print(
                f"Incidents skipped Due to Existing Primary Keys: {len(skipped)}/{len(df)}\n{skipped}"
            )
        if len(errorRows) > 0:
            import datetime

            with open(
                "{0} Write Errors - {1}.xlsx".format(
                    table, (datetime.datetime.now()).strftime("%y-%m-%d_%H-%M")
                ),
                "w",
            ) as f:
                f.write(
                    f"===== Integrety Error =====\n{skipped}\n\n===== Data Error =====\n{errored}"
                )

            print(
                f"==== Errors importing the following {len(errorRows)} Incidents - \n"
            )
            print(*errored, sep="\n\n")
            print("\n==========================================================\n")
            # Write errors to file
            writer = pd.ExcelWriter(
                "{0} Write Errors - {1}.xlsx".format(
                    table, (datetime.datetime.now()).strftime("%y-%m-%d_%H-%M")
                ),
                engine="xlsxwriter",
                datetime_format="mm/dd/yyyy hh:mm:ss",
                date_format="mm/dd/yyyy",
            )
            df.iloc[errorRows].to_excel(writer)
            writer.save()

    # testing
    def insertTest(self, df):
        self.insertToTable(df, "Test")
        # df.to_sql("Test", self.engine, if_exists="append", index=False)
        # res = self.engine.execute("SELECT * FROM dbo.Test;")
        # for i in res:
        #     print(i)

    # main reason for this... run all included functions
    def insertDF(self, df):
        if df.loc[0, "Data_Source"] == "ems":
            self.insertToEMSIncident(df)
            self.insertToEMSUnits(df)
        else:
            self.insertToFireIncident(df)
            self.insertToFireUnits(df)

        return None

    def retreiveDF(self, query, dateFields):

        sql_df = pd.read_sql(
            query,
            con=self.engine,
            parse_dates=dateFields,
        )

        return sql_df


# =================================================================
#      Testing Code
# =================================================================
if __name__ == "__main__":
    # set up simple dataframe to test insertion
    # df = pd.DataFrame([["test from dataframe"], ["pandas will work"]], columns=["data"])

    db = SQLDatabase()
    df = db.retreiveDF()
    # db.insertTest(df)

    # cursor = conn.cursor()

    # cursor.execute(

    # conn.commit()

    # cursor.execute("SELECT * FROM dbo.Test;")

    # for i in cursor:
    #     print(i)
