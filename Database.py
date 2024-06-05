import sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import numpy as np
import json
from sys import exit
import re
from ServerFiles import setup_logging
import traceback

# from pandasgui import show
from tqdm import tqdm

from dotenv import load_dotenv, find_dotenv
from os import getenv


from datetime import datetime

logger = setup_logging("Database.log")


class SQLDatabase:
    """a connection to a SQL Database, and associated functions for insertion of required data"""

    def which_database_to_use(self, dtbs):
        config_file_location = ".\\data\\Lists\\TestDatabase.json"
        # Test Database is a JSON object with 2 fields:
        #     "use_test_database": true,
        #     "test_database_name":"UnitRunDataTest"
        try:
            with open(config_file_location, "r") as file:
                config = json.load(file)

            if config["use_test_database"]:
                print(f"Testing Database is Enabled ({config_file_location})")
                return config["test_database_name"]
        except Exception as e:
            logger.warning("Test Database Config not found at: {config_file_location}")

        # Use specific database if one was passed
        if dtbs != "":
            return getenv(dtbs)

        return getenv("DBDTBS")

    def __init__(self, useDTBS=""):
        load_dotenv(find_dotenv())
        drvr = getenv("DBDRVR")
        srvr = getenv("DBSRVR")
        dtbs = self.which_database_to_use(useDTBS)
        if drvr == None or srvr == None or dtbs == None:
            print("Error: Database connection not read from .env")
            exit()

        connectionString = f"DRIVER={drvr};SERVER={srvr};DATABASE={dtbs};"
        print(connectionString)
        connection_url = URL.create(
            "mssql+pyodbc", query={"odbc_connect": connectionString}
        )
        self.engine = sqlalchemy.create_engine(connection_url)

    def close(self):
        if self.engine:
            self.engine.dispose()

    def insertToRawEMS(self, df):
        self.insertToTable(df, "RawEMS")
        return None

    def insertToRawFire(self, df):
        prepreprocess = {
            # Aug 28 2023, dispatch renamed a file.  Fixing that here.
            "Alarm_Level": "Alarm Level",
        }
        df = df.rename(columns=prepreprocess, errors="ignore")
        self.insertToTable(df, "RawFire")
        return None

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
                "Block_ID",
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
                "Response_Area",
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
                "Block_ID",
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
        def getIncName(df):
            for name in ["Incident_Number", "Master_Incident_Number", "Incident"]:
                if name in df.columns:
                    return name

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
                inc = df.iloc[i][getIncName(df)]
                skipped.append(f"{inc} - {err}")
                errorRows.append(i)
            except sqlalchemy.exc.ProgrammingError as err:
                inc = df.iloc[i][getIncName(df)]
                errored.append(f"{inc} - {err}")
                errorRows.append(i)
            except sqlalchemy.exc.DataError as err:
                inc = df.iloc[i][getIncName(df)]
                errored.append(f"{inc} - {err}")
                errorRows.append(i)
            except Exception as err:
                print(" - Something went wrong! - \n", err)

        if len(skipped) > 0:
            print(
                f"Incidents skipped Due to Existing Primary Keys: {len(skipped)}/{len(df)}"
            )
            if len(skipped) != len(df):
                for line in skipped:
                    print(f"{line}\n")
            else:
                print("Every row failed.  Has this file already been processed?")
        if len(errorRows) > 0:
            import datetime

            with open(
                "../Logs/{0} Write Errors - {1}.xlsx".format(
                    table, (datetime.datetime.now()).strftime("%y-%m-%d_%H-%M")
                ),
                "w",
            ) as f:
                f.write(
                    f"===== Data Error =====\n{errored}\n\n\n===== Integrety Error =====\n{skipped}\n\n"
                )

            print(
                f"==== Errors importing the following {len(errorRows)} Incidents - \n"
            )
            for line in errored:
                print(f"{line}\n")
            print("\n==========================================================\n")
            # Write errors to file
            writer = pd.ExcelWriter(
                "../Logs/{0} Write Errors - {1}.xlsx".format(
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

    def check_simple_errors(self, err):
        err_str = str(err)
        
        # Regex to catch "String or binary data would be truncated" errors
        truncate_match = re.search(
            r"String or binary data would be truncated in table \\'([^']+)\\', column \\'([^']+)\\'",
            err_str,
        )
        if truncate_match:
            table, column = truncate_match.groups()
            return f"{table}[{column}] would be truncated. Consider expanding to greater size."
        
        # Regex to catch "Cannot insert the value NULL into column" errors
        null_match = re.search(
            r"Cannot insert the value NULL into column '([^']+)', table '([^']+)'; column does not allow nulls. UPDATE fails.",
            err_str,
        )
        if null_match:
            column, table = null_match.groups()
            return f"{table}[{column}] Cannot be NULL. Unit/Incident Skipped."
        
        return None

    def format_sql_values(self, row):
        formatted = []
        for item in row:
            if isinstance(item, bool):
                formatted.append('1' if item else '0')

            # Check if the item is list or dict and handle as JSON
            if isinstance(item, (list, dict)):
                # Serialize list or dictionary to JSON string
                json_str = json.dumps(item).replace("'", "''")
                formatted.append(f"'{json_str}'")
                continue

            # Handle numpy arrays, if any, assuming they are not meant to be here
            if isinstance(item, np.ndarray):
                if item.size == 1:
                    item = item[0]  # Convert single-element arrays to scalars
                else:
                    logger.error(
                        "Item is an array with more than one element. Only single-element arrays are handled."
                    )
                    continue

            # Normal null check and handling for scalar values
            if pd.isnull(item):
                formatted.append("NULL")
            elif isinstance(item, str):
                # Escape single quotes and handle line breaks
                escaped_item = (
                    item.replace("'", "''").replace("\n", "| ").replace("\r", "| ")
                )  # Replace line breaks with spaces
                formatted.append(f"'{escaped_item}'")
            elif isinstance(item, pd.Timestamp):
                formatted.append(f"'{item.strftime('%Y-%m-%d %H:%M:%S')}'")
            else:
                formatted.append(str(item))

        return ", ".join(formatted)


    # main reason for this... run all included functions
    def insertDF(self, df):
        if df.loc[0, "Data_Source"] == "ems":
            self.insertToEMSIncident(df)
            self.insertToEMSUnits(df)
        else:
            self.insertToFireIncident(df)
            self.insertToFireUnits(df)

        return None

    def insertRaw(self, df, type):
        if type == "ems":
            # DONT manipulate the DF - keep the function pure!
            temp = df
            temp["PandasIndex"] = temp.index
            self.insertToRawEMS(df)
        else:
            self.insertToRawFire(df)

    def retrieve_df(self, query, date_fields):
        """
        Retrieves data from the database and returns it as a pandas DataFrame.

        Parameters:
            query (str): SQL query to be executed.
            date_fields (list of str): List of column names that should be parsed as dates.

        Returns:
            pandas.DataFrame: Data retrieved from the database.
        """
        try:
            print("Pulling data from Database")
            sql_df = pd.read_sql(query, con=self.engine, parse_dates=date_fields)
            print(f"Data successfully retrieved: {len(sql_df)} rows.")
        except Exception as e:
            print(f"An error occurred while pulling data: {e}")
            return pd.DataFrame()  # Return an empty DataFrame in case of an error

        return sql_df

    def RunFireEMSLink(self, date):
        print("Updating Fire EMS Link table for the last month...")
        fire_ems_link_procedure = f"exec linkFireEMS @lastRunDate='{date}';"
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(fire_ems_link_procedure)
        return None

    def RunConcurrencyUpdate(self, date, date_end):
        print("Updating Concurrency table for the last month...")
        concurrency_procedure = (
            f"EXEC GenerateEmergencyResponseSummary '{date}', '{date_end}';"
        )
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(concurrency_procedure)
        return None



    # REWRITES
    #==========================================================================================================================================
    def insert_dataframe(self, df, table_name, primary_keys):
        """
        Upserts a DataFrame into the specified table with error handling and logging failures.

        Parameters:
            df (pandas.DataFrame): DataFrame to upsert.
            table_name (str): Table name to upsert into.
            primary_keys (list): List of column names to be used as primary keys.
        """
        # Converting boolean columns to 1/0 for SQL Server BIT type compatibility
        bool_columns = [col for col, dtype in df.dtypes.items() if dtype == "bool"]
        for col in bool_columns:
            df[col] = df[col].astype(int)

        Session = sessionmaker(bind=self.engine)
        session = Session()

        failed_rows = []
        for index, row in tqdm(df.iterrows(), total=len(df), desc="Upserting rows"):
            values = self.format_sql_values(row.values)
            primary_key_condition = " AND ".join(
                [f"target.[{pk}] = source.[{pk}]" for pk in primary_keys]
            )
            update_set = ", ".join(
                [
                    f"target.[{col}] = source.[{col}]"
                    for col in df.columns
                    if col not in primary_keys
                ]
            )
            insert_cols = ", ".join([f"[{col}]" for col in df.columns])
            insert_vals = values

            stmt = f"""
            MERGE INTO {table_name} AS target
            USING (SELECT * FROM (VALUES ({insert_vals})) AS s ({insert_cols}))
            AS source ON {primary_key_condition}
            WHEN MATCHED THEN
                UPDATE SET {update_set}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals});
            """
            try:
                session.execute(stmt)
                session.commit()
            except Exception as e:
                simple_error = self.check_simple_errors(e)
                if simple_error:
                    logger.error(f"Simple Error for row {index}: {simple_error}")
                else:
                    logger.error(f"Failed to insert/update row {index}: {e}")
                    # logger.error(f"SQL Statement: {stmt}")  # Log the full SQL statement
                failed_rows.append(index)

        if failed_rows:
            logger.info(f"Failed rows are logged. Row indices: {failed_rows}")
        session.close()

    def insertESOBasic(self, df):
        """
        Inserts or updates entries into the 'Basic' table using 'IncidentId' as the primary key.
        """
        self.insert_dataframe(df, "Basic", ["IncidentId"])

    def UpsertRaw(self, df, type):
        if type == "ems":
            # DONT manipulate the DF - keep the function pure!
            temp = df
            temp["PandasIndex"] = temp.index
            self.insert_dataframe(df, "RawEMS", ["Incident", "Unit", "Assigned"])
        else:
            self.insert_dataframe(df, "RawFire", ["Master_Incident_Number", "Radio_Name", "Unit Time Assigned"])

    
    def new_insert_DF(self, df, data_source):
        if data_source == "ems":
            try: self.new_insertToEMSIncident(df)
            except Exception as e:
                tb = traceback.format_exc()  # This captures the entire traceback as a string
                logger.error(f"Failed: to insert into EMS Incidents: {tb}")

            try: self.new_insertToEMSUnits(df)
            except Exception as e:
                tb = traceback.format_exc()  # This captures the entire traceback as a string
                logger.error(f"Failed: to insert into EMS Units: {tb}")
            
        else:
            try: self.new_insertToFireIncident(df)
            except Exception as e:
                tb = traceback.format_exc()  # This captures the entire traceback as a string
                logger.error(f"Failed: to insert into Fire Incidents: {tb}")

            try: self.new_insertToFireUnits(df)
            except Exception as e:
                tb = traceback.format_exc()  # This captures the entire traceback as a string
                logger.error(f"Failed: to insert into Fire Units: {tb}")
            
            

        return None

    def new_insertToFireIncident(self, df):
        # get array of unique incident numbers
        required_columns = [
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
            "Block_ID",
        ]

        logger.info("Checking against column list")

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing columns in DataFrame: {missing_columns}")
            return  # Optionally return here or handle the missing columns differently

        logger.info("Constructing Incident List")
        uniqueIncidents = df[required_columns]
        uniqueIncidents = uniqueIncidents.drop_duplicates(subset=["Incident_Number"])

        logger.info("Inserting Unique incidents")
        self.insert_dataframe(uniqueIncidents, "FireIncidents", ["Incident_Number"])

    def new_insertToEMSIncident(self, df):
        # get array of unique incident numbers
        required_columns = [
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
            "Response_Area",
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
            "Block_ID",
        ]
        logger.info("checking against column list")
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Missing columns in DataFrame: {missing_columns}")
            return  # Optionally return here or handle the missing columns differently

        logger.info("Constructing Incident List")
        uniqueIncidents = df[required_columns]
        uniqueIncidents = uniqueIncidents.drop_duplicates(subset=["Incident_Number"])
        # show(uniqueIncidents)

        logger.info("Inserting unique incident list")
        # self.insertToTable(uniqueIncidents, "EMSIncidents")
        self.insert_dataframe(uniqueIncidents, "EMSIncidents", ["Incident_Number"])

    def special_conversions(self, df):
        # Special handling for 'Is_Closest_Station' if it exists in DataFrame
        if 'Is_Closest_Station' in df.columns:
            df['Is_Closest_Station'] = df['Is_Closest_Station'].replace({True: 1, False: 0})
        return(df)

    def new_insertToFireUnits(self, df):
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
        # self.insertToTable(unitCalls, "FireUnits")
        unitCalls = self.special_conversions(unitCalls)
        self.insert_dataframe(unitCalls, "FireUnits", ["Incident_Number","Unit","Unit_Assigned"])

    def new_insertToEMSUnits(self, df):
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
        # self.insertToTable(unitCalls, "EMSUnits")
        unitCalls = self.special_conversions(unitCalls)
        self.insert_dataframe(unitCalls, "EMSUnits", ["Incident_Number","Unit","Unit_Assigned"])

    # ======================================================================================
    # Google Form Insertions
    # ======================================================================================
    # def insertForm(self, json):
    #     from pandasgui import show

    #     df = pd.json_normalize(json["responses"])
    #     show(df)


# =================================================================
#      Testing Code
# =================================================================
if __name__ == "__main__":
    # set up simple dataframe to test insertion
    # df = pd.DataFrame([["test from dataframe"], ["pandas will work"]], columns=["data"])
    exit()
    db = SQLDatabase()
    # df = db.retreive_df()
    # db.insertTest(df)

    # cursor = conn.cursor()

    # cursor.execute(

    # conn.commit()

    # cursor.execute("SELECT * FROM dbo.Test;")

    # for i in cursor:
    #     print(i)
    from datetime import datetime, timedelta

    today = datetime.now()
    today = today.replace(hour=0, minute=0, second=0, microsecond=0)
    one_month_ago = today - timedelta(days=30)
    one_month_ago = one_month_ago.replace(hour=0, minute=0, second=0, microsecond=0)
    db.RunConcurrencyUpdate(one_month_ago, today)
