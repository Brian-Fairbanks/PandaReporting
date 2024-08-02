import time
from arcgis import GIS
from dotenv import load_dotenv
from os import getenv, remove, path
import datetime
from ServerFiles import setup_logging, get_base_dir
import logging

logName = f"Esri_Export-{(datetime.datetime.now()).strftime('%y-%m-%d_%H-%M')}.log"
logger = setup_logging(logName)

base_dir = get_base_dir()

arc_gis_csv_id = "620a5cd3ccdd4b39908158b3cb87e3b5"

temp_CSV_file = path.join(base_dir, "EMSFireRunData.csv")

esri_Export_Query = "SELECT * FROM [dbo].[v_esri_export-Query-Filtered] where [Phone_Pickup_Time] >= '01/01/2020'"

EsriTableArray = [
    "Incident_Number",
    "Data_Source",
    "Status",
    "Unit",
    "First_Arrived_Esri",
    "Unit_Disposition",
    "Incident_Type",
    "City",
    "Phone_Pickup_Time",
    "Problem",
    "Jurisdiction",
    "Response_Plan",
    "Priority_Description",
    "1st_Assigned_to_1st_Enroute",
    "1st_Enroute_to_1st_Arrived",
    "1st_Assigned_to_1st_Arrived",
    "Phone_Pickup_to_Call_Closed",
    "Phone_Pickup_to_1st_Arrived",
    "Unit_Assigned_to_Enroute",
    "Unit_Enroute_to_Arrived",
    "Unit_Assigned_to_Arrived",
    "Unit_Assign_to_Cleared",
    "Phone_Pickup_to_Unit_Arrived",
    "Population_Classification",
    "ESD02_Shift",
    "Station",
    "Month",
    "Year",
    "Calls_Over_20_Min",
    "X_Long",
    "Y_Lat",
    "IsESD17",
    "isETJ",
    "isCOP",
    "Bucket_Type",
    "Time_0_Active",
    "Time_1_Active",
    "Time_2_Active",
    "Time_3_Active",
    "Time_4_Active",
    "Time_5_Active",
    "Incident_ERF_Time",
    "Incident_Call_Disposition",
    "In_Queue_to_Unit_Assigned",
    "Phone_Pickup_to_Unit_Assigned",
    "Unit_Arrived_to_Cleared",
    "Phone_Pickup_to_In_Queue",
    "In_Queue_to_1st_Assigned",
    "Phone_Pickup_to_1st_Assigned",
    "1st_Arrived_to_Call_Closed",
    "Response_Status",
    "Incident_Call_Count",
    "People_Per_Mile",
    "Ph_PU_to_Unit_Arrive_Time_Intervals",
    "Unit_Response_Single_vs_Multi_Response_Count",
    "Unit_Usage_At_Time_of_Alarm",
    "AFD_Response_Box",
    "Transport_Mode",
    "Transport_Protocol",
    "Depart_to_At_Destination",
    "Depart_to_Cleared_Destination ",
    "Department",
    "call_delayed",
    "Unit_Type",
    "Frontline_Status",
    "EMD_Code",
    "is_Last_Month",
    "Subdivision",
]


def getFormattedTable():
    from Database import SQLDatabase

    try:
        db = SQLDatabase()
        df = db.retrieve_df(
            esri_Export_Query,
            [
                "Phone_Pickup_Time",
            ],
        )

        # BIT auto converted to BOOLEAN, but exists in ESRI as INTEGER
        logger.info("Converting boolean to INTS for ESRI")
        for col in [
            "IsESD17",
            "isETJ",
            "isCOP",
            "is_walkup",
            "call_delayed",
            "INC_Staged_As_Arrived",
            "UNIT_Staged_As_Arrived",
            "is_Last_Month",
        ]:
            df[col] = df[col].astype(int)
        return df
    except Exception as e:
        logger.info("  - Process Failed!  - Error in Database Extraction - Please check the logs.")
        logging.exception("Exception found in Database Extraction")
        exit(1)

class EsriDatabase:
    """A connection to a SQL Database, and associated functions for insertion of required data"""

    def __init__(self):
        pass

    def connect(self):
        load_dotenv()
        # config = {"AGORGURL", AGUSERNAME, AGPASSWORD}
        your_org_url = getenv("AGORGURL")
        username = getenv("AGUSERNAME")
        password = getenv("AGPASSWORD")

        self.gis = GIS(your_org_url, username, password)

    def empty(self):
        self.tbl.manager.truncate()  # truncate table

    def appendDF(self, df):
        adds = df.spatial.to_featureset()
        self.tbl.edit_features(adds=adds)

    def overwriteDF(self, df):
        self.empty()
        self.appendDF(df)
    # ===============================================================================================================
    #  this is the data provided by ESRI
    #  It will upload a CSV file drectly to ARCGIS online
    #  and then PUBLISH the csv file to a new feature layer,
    #  overwriting the previous one and retaining dashboards created from it.
    # ===============================================================================================================
    def publishCSV(self):
        df = getFormattedTable()
        logger.info("Writing Temporary CSV file...")

        with open(temp_CSV_file, 'w', newline='') as file:
            df.to_csv(file, index=False)
            file.close

        logger.info("Sending to ESRI")

        try:
            logger.info("  - Finding Existing file")
            csv_item = self.gis.content.get(arc_gis_csv_id)

            item_props = {
                "type": csv_item.type,
                "description": csv_item.description,
                "tags": csv_item.tags,
                "typeKeywords": csv_item.typeKeywords,
                "title": csv_item.title,
            }
        except Exception as e:
            logger.error("  - Process Failed!  - Could not get current item from ESRI")
            remove(temp_CSV_file)
            logging.exception("Exception from CSV File Upload")
            exit(1)

        try:
            logger.info("  - Pushing File to Esri")
            csv_item.update(
                item_props,
                data=temp_CSV_file,
            )

            logger.info("  - Complete!")
        except Exception as e:
            logger.error("  - Process Failed!  - Error uploading CSV file to ESRI. Removing Temporary CSV file")
            remove(temp_CSV_file)
            logging.exception("Exception from CSV File Upload")
            exit(1)

        logger.info("Publishing CSV to Feature Layer...")
        try:
            analyze_csv = self.gis.content.analyze(item=csv_item)
            pp = analyze_csv["publishParameters"]

            pp["type"] = csv_item.type
            pp["name"] = csv_item.title
            pp["locationType"] = "coordinates"
            pp["latitudeFieldName"] = "Y_Lat"
            pp["longitudeFieldName"] = "X_Long"
            pp["coordinateFieldType"] = "LatitudeAndLongitude"

            csv_item.publish(publish_parameters=pp, overwrite=True)
        except Exception as e:
            logger.error(f"  - Process Failed!  - Error Publishing CSV on ESRI Portal\n{e}")
            logging.exception("Exception in ESRI CSV Publishing")
            remove_with_retry(temp_CSV_file)
            exit(1)

        logger.info("Feature Layer Successfully Updated - releasing Connection to ARCGIS")
        del csv_item

        logger.info("Removing Temporary CSV file.")
        remove_with_retry(temp_CSV_file)


def remove_with_retry(file_path, retries=3, delay=1):
    """Attempt to remove a file, with retries if it is being used by another process."""
    try:
        for _ in range(retries):
            try:
                remove(file_path)
                return
            except Exception as e:
                logging.error(f"Attempt to remove file failed: {e}")
                time.sleep(delay)
        # Final attempt without catching exception to propagate if all retries failed
        remove(file_path)
    except Exception as e:
        logging.error(f"Failed to remove CSV: {e}")
        pass

def appendFeatureLayer():
    df = getFormattedTable()
    esriDF.appendDF(df)

def formatDFForEsri(df):
    emsOnlyCols = [
        "Transport_Mode",
        "Transport_Protocol",
        "Depart_to_At_Destination",
        "Depart_to_Cleared_Destination ",
        "EMD_Code",
    ]
    for col in emsOnlyCols:
        if col not in df.columns:
            df[col] = None
    df = df[EsriTableArray]
    return df

if __name__ == "__main__":
    # Remove the file at the start if it exists
    if path.exists(temp_CSV_file):
        try:
            remove(temp_CSV_file)
            logger.info(f"Removed existing file {temp_CSV_file} at the start")
        except Exception as e:
            logger.error(f"Failed to remove existing file {temp_CSV_file} at the start: {e}")
            exit(1)
    esriDF = EsriDatabase()
    esriDF.connect()
    esriDF.publishCSV()
