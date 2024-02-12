from arcgis import GIS
from dotenv import load_dotenv
from os import getenv, remove
import datetime

import logging

# set up logging folder
writePath = "../Logs"

# logging setup - write to output file as well as printing visably
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()
logger.addHandler(
    logging.FileHandler(
        f"{writePath}/Esri_Export-{(datetime.datetime.now()).strftime('%y-%m-%d_%H-%M')}.log",
        "a",
    )
)
print = logger.info

# LOG_FILENAME = (
#     "../Logs/Esri_{(datetime.datetime.now()).strftime('%y-%m-%d_%H-%M')}.txt"
# )
# logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)

# from pandasgui import show

arc_gis_csv_id = "620a5cd3ccdd4b39908158b3cb87e3b5"
# arc_gis_csv_id = "0549a85434524c20bdc195765480b5a7"

temp_CSV_file = r".\\EMSFireRunData.csv"
# temp_CSV_file = r".\\UnitRunData.csv"

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
        df = db.retreiveDF(
            esri_Export_Query,
            [
                "Phone_Pickup_Time",
            ],
        )

        # BIT auto converted to BOOLEAN, but exists in ESRI as INTEGER
        print("Converting boolean to INTS for ESRI")
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
    except:
        print(
            "  - Process Failed!  - Error in Database Extraction - Please check the logs."
        )
        logging.exception("Exception found in Database Extraction")
        exit(1)


class EsriDatabase:
    """a connection to a SQL Database, and associated functions for insertion of required data"""

    def __init__(self):
        pass

    def connect(self):
        load_dotenv()
        # config = {"AGORGURL", AGUSERNAME, AGPASSWORD}
        your_org_url = getenv("AGORGURL")
        username = getenv("AGUSERNAME")
        password = getenv("AGPASSWORD")

        self.gis = GIS(your_org_url, username, password)
        # table_url = "https://services9.arcgis.com/dcfjRs7Bq0KG7jYq/arcgis/rest/services/Esri_Auto_Import_from_Python_Test/FeatureServer/0"        #Test Table URL
        # table_url = "https://services9.arcgis.com/dcfjRs7Bq0KG7jYq/arcgis/rest/services/Fire_EMS_Run_Data/FeatureServer/0"
        # table_url = "https://services9.arcgis.com/dcfjRs7Bq0KG7jYq/arcgis/rest/services/EMS_Fire_Run_Data/FeatureServer/0"
        # self.tbl = FeatureLayer(table_url, gis=self.gis)  # works for tables

    def empty(self):
        self.tbl.manager.truncate()  # truncate table

    def appendDF(self, df):
        # df = formatDFForEsri(df)
        # show(df)
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
        # Create CSV File from Database
        # from timer import Timer

        # csvTimer = Timer("Timing query request to CSV write")
        # csvTimer.start()

        df = getFormattedTable()
        print("Writing Temporary CSV file...")
        df.to_csv(temp_CSV_file, index=False)

        # csvTimer.end()

        # Send the file to ESRI
        print("Sending to ESRI")

        try:
            print("  - Finding Existing file")
            csv_item = self.gis.content.get(arc_gis_csv_id)

            item_props = {
                "type": csv_item.type,
                "description": csv_item.description,
                "tags": csv_item.tags,
                "typeKeywords": csv_item.typeKeywords,
                "title": csv_item.title,
            }
        except:
            print("  - Process Failed!  - Could not get current item from ESRI")
            remove(temp_CSV_file)
            logging.exception("Exception from CSV File Upload")
            exit(1)

        try:
            print("  - Pushing File to Esri")
            csv_item.update(
                item_props,
                data=temp_CSV_file,
            )

            print("  - Complete!")
        except:
            print(
                "  - Process Failed!  - Error uploading CSV file to ESRI.  Removing Temporary CSV file"
            )
            remove(temp_CSV_file)
            logging.exception("Exception from CSV File Upload")
            exit(1)

        print("Removing Temporary CSV file.")
        # delete the file if it fails or finishes!
        remove(temp_CSV_file)

        print("publishing CSV to Feature Layer... ")
        try:
            analyze_csv = self.gis.content.analyze(item=csv_item)
            pp = analyze_csv["publishParameters"]

            # Overwrite some of these parameters to force coordinate selections
            pp["type"] = csv_item.type
            pp["name"] = csv_item.title
            pp["locationType"] = "coordinates"
            pp["latitudeFieldName"] = "Y_Lat"
            pp["longitudeFieldName"] = "X_Long"
            pp["coordinateFieldType"] = "LatitudeAndLongitude"

            csv_item.publish(publish_parameters=pp, overwrite=True)
        except Exception as e:
            print(f"  - Process Failed!  - Error Publishing CSV on ESRI Portal")
            print(e)
            logging.exception("Exception in ESRI CSV Publishing")
            exit(1)

        print("Feature Layer Succesfully Updated")

    # end publishCSV


# End Class


# ===============================================================================================================
#  this is the data mostly put together myself
#  It will create a Dataframe, pulled from our database
#  then overwrite and push this data directly to a particular feature layer
# ===============================================================================================================


def appendFeatureLayer():
    # csv_file = r"C:\\Users\\bfairbanks\\Desktop\\esriDF.csv"
    # df = GeoAccessor.from_table(csv_file)

    df = getFormattedTable()
    # show(df)
    esriDF.appendDF(df)


# end of class


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


# read from CSV file
# =============================================================
# casos_csv = r"C:\\Users\\bfairbanks\\Desktop\\test.csv"
# df = GeoAccessor.from_table(casos_csv)

# using existing dataframe
# ==============================================================
# d = {
#     "name": ["test3", "test4", "test5"],
#     "type": ["int", "int", "bool"],
#     "val": ["32", "64", "False"],
#     "inc": ["94481", "34643", "39103"],
# }
# df = pd.DataFrame(data=d)
# df = GeoAccessor.from_df(odf)

# using analyzeFire
# ==============================================================
# adds = df.spatial.to_featureset()
# tbl.edit_features(adds=adds)

if __name__ == "__main__":
    esriDF = EsriDatabase()
    esriDF.connect()

    # esriDF.appendFeatureLayer()
    esriDF.publishCSV()
