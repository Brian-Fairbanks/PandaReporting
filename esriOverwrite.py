import pandas as pd
from arcgis import GIS
from arcgis.features import GeoAccessor, FeatureLayer
from dotenv import load_dotenv
from os import getenv

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
]


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

        gis = GIS(your_org_url, username, password)
        # table_url = "https://services9.arcgis.com/dcfjRs7Bq0KG7jYq/arcgis/rest/services/Esri_Auto_Import_from_Python_Test/FeatureServer/0"        #Test Table URL
        table_url = "https://services9.arcgis.com/dcfjRs7Bq0KG7jYq/arcgis/rest/services/Fire_EMS_Run_Data/FeatureServer/0"
        self.tbl = FeatureLayer(table_url, gis=gis)  # works for tables

    def empty(self):
        self.tbl.manager.truncate()  # truncate table

    def appendDF(self, df):
        df = formatDFForEsri(df)
        adds = df.spatial.to_featureset()
        self.tbl.edit_features(adds=adds)

    def overwriteDF(self, df):
        self.empty()
        self.appendDF(df)


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

    csv_file = r"C:\\Users\\bfairbanks\\Desktop\\esriDF.csv"
    df = GeoAccessor.from_table(csv_file)

    esriDF.appendDF(df)
