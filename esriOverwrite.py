from pickle import TRUE
from tkinter import FALSE
import pandas as pd
from arcgis import GIS
from arcgis.features import GeoAccessor, FeatureLayer
from dotenv import load_dotenv
from os import getenv

# from pandasgui import show

esri_Export_Query = """
Select * from (
	(SELECT 
		EI."Incident_Number",
		'ems' as Data_Source,
		EU."Status",
		EI."Problem",
		EI."Incident_Type",
		EI."Response_Plan",
		EI."Priority_Description",
		EI."EMD_Code",
		EI."Incident_Call_Disposition",
		EI."Incident_Call_Count",

		-- Incident Location
		EI."X_Long",
		EI."Y_Lat",
		EI."City",
		EI."Jurisdiction",
		EI."AFD_Response_Box",
		EI."Population_Classification",
		EI."People_Per_Mile",
		EI."IsESD17",
		EI."isETJ",
		EI."isCOP",	
		EI.[Closest_Station],
		EI.[Distance_to_S01_in_miles],
		EI.[Distance_to_S02_in_miles],
		EI.[Distance_to_S03_in_miles],
		EI.[Distance_to_S04_in_miles],
		EI.[Distance_to_S05_in_miles],
		EI.[Distance_to_S06_in_miles],
		EI.[Distance_to_S07_in_miles],
		EI.[Distance_to_S08_in_miles],
		EI.[Distance_to_S09_in_miles],
		EU.[Is_Closest_Station],
		EI.[is_walkup],

		-- Incident Times
		MONTH(EI.Phone_Pickup_Time) as "Month",
		YEAR(EI.Phone_Pickup_Time) as "Year",
		EI."Phone_Pickup_Time",
		EI."call_delayed",
		EI."ESD02_Shift",

		-- Incident Time Deltas
		case
			WHEN datediff(s, EI.[Phone_Pickup_Time] , EI.[Call_Entered_in_Queue])> 0 THEN datediff(s, EI.[Phone_Pickup_Time] , EI.[Call_Entered_in_Queue])
			ELSE NULL
			END as Phone_Pickup_to_In_Queue,
		case
			WHEN datediff(s, EI.[Call_Entered_in_Queue] , EI.[First_Unit_Assigned])> 0 THEN datediff(s, EI.[Call_Entered_in_Queue] , EI.[First_Unit_Assigned])
			ELSE NULL
			END as In_Queue_to_1st_Assigned,
		datediff(s, EI.[Phone_Pickup_Time] , EI.[First_Unit_Assigned]) as Phone_Pickup_to_1st_Assigned,
		datediff(s, EI.First_Unit_Assigned , EI.First_Unit_Enroute) as "1st_Assigned_to_1st_Enroute",
		EI.[INC_Staged_As_Arrived],
		case
			WHEN EI.[INC_Staged_As_Arrived]=1 THEN datediff(s, EI.[First_Unit_Enroute] , EI.[First_Unit_Staged])
			ELSE datediff(s, EI.[First_Unit_Enroute] , EI.[First_Unit_Arrived])
			END as "1st_Enroute_to_1st_Arrived",
		case
			WHEN EI.[INC_Staged_As_Arrived]=1 THEN datediff(s, EI.[First_Unit_Assigned] , EI.[First_Unit_Staged])
			ELSE datediff(s, EI.[First_Unit_Assigned] , EI.[First_Unit_Arrived])
			END as "1st_Assigned_to_1st_Arrived",
		case
			WHEN EI.[INC_Staged_As_Arrived]=1 THEN datediff(s, EI.[First_Unit_Staged], EI.[Call_Closed])
			ELSE datediff(s, EI.[First_Unit_Arrived] , EI.[Call_Closed])
			END as "1st_Arrived_to_Call_Closed",
		case
			WHEN EI.[INC_Staged_As_Arrived]=1 THEN datediff(s, EI.[Phone_Pickup_Time], EI.[Call_Closed]) 
			ELSE datediff(s, EI.[Phone_Pickup_Time] , EI.[First_Unit_Arrived]) 
			END as "Phone_Pickup_to_1st_Arrived",
		--datediff(s, EI.[First_Unit_Enroute] , EI.[First_Unit_Arrived]) as "1st_Enroute_to_1st_Arrived", -- ````````
		--datediff(s, EI.[First_Unit_Assigned] , EI.[First_Unit_Arrived]) as "1st_Assigned_to_1st_Arrived",--````````
		--datediff(s, EI.[First_Unit_Arrived] , EI.[Call_Closed]) as "1st_Arrived_to_Call_Closed",--`````````````````
		--datediff(s, EI.[Phone_Pickup_Time] , EI.[First_Unit_Arrived]) as "Phone_Pickup_to_1st_Arrived",--``````````
		datediff(s, EI.[Phone_Pickup_Time] , EI.[Call_Closed]) as "Phone_Pickup_to_Call_Closed",

		EI."Incident_ERF_Time",

		-- Unit Information
		EU."Unit",
		EU."Unit_Type",
		EU."Bucket_Type",
		EU."Station",
		EU."Department",	
		EU."Frontline_Status",
		EU."Response_Status",
		EU."Unit_Disposition",
		EU."Transport_Count",
		EU."Transport_Mode",
		EU."Transport_Protocol",
		EU."First_Arrived_Esri",

		-- Unit Time Deltas
		case
			WHEN datediff(s, EI.[Call_Entered_in_Queue] , EU.[Unit_Assigned])> 0 THEN datediff(s, EI.[Call_Entered_in_Queue] , EU.[Unit_Assigned])
			ELSE NULL
			END as n_Queue_to_Unit_Assigned,
		datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Assigned]) as Phone_Pickup_to_Unit_Assigned,
		datediff(s, EU.[Unit_Assigned] , EU.[Unit_Enroute]) as "Unit_Assigned_to_Enroute",
		EU.[UNIT_Staged_As_Arrived],
		case
			WHEN EU.[UNIT_Staged_As_Arrived]=1 THEN datediff(s, EU.[Unit_Enroute] , EU.[Unit_Staged])
			ELSE datediff(s, EU.[Unit_Enroute] , EU.[Unit_Arrived])
			END as "Unit_Enroute_to_Arrived",
		case
			WHEN EU.[UNIT_Staged_As_Arrived]=1 THEN datediff(s, EU.[Unit_Assigned] , EU.[Unit_Staged])
			ELSE datediff(s, EU.[Unit_Assigned] , EU.[Unit_Arrived])
			END as "Unit_Assigned_to_Arrived",
		case
			WHEN EU.[UNIT_Staged_As_Arrived]=1 THEN datediff(s, EU.[Unit_Staged], EU.[Unit_Cleared])
			ELSE datediff(s, EU.[Unit_Arrived] , EU.[Unit_Cleared])
			END as Unit_Arrived_to_Cleared,
		case
			WHEN EU.[UNIT_Staged_As_Arrived]=1 THEN datediff(s, EI.[Phone_Pickup_Time], EU.[Unit_Cleared]) 
			ELSE datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Arrived]) 
			END as "Phone_Pickup_to_Unit_Arrived",
		datediff(s, EU.[Unit_Assigned] , EU.[Unit_Cleared]) as "Unit_Assign_to_Cleared",		
		datediff(s, EU.[Time_Depart_Scene] , EU.[Time_At_Destination]) as Depart_to_At_Destination,
		datediff(s, EU.[Time_Depart_Scene] , EU.[Time_Cleared_Destination]) as Depart_to_Cleared_Destination,
		
		-- concurrent usage
		EU."Unit_Usage_At_Time_of_Alarm",
		EU."Time_0_Active",
		EU."Time_1_Active",
		EU."Time_2_Active",
		EU."Time_3_Active",
		EU."Time_4_Active",
		EU."Time_5_Active",	
		EU."Time_6_Active",	
		EU."Time_7_Active",	
		EU."Time_8_Active",	
		EU."Time_9_Active",	
		
		-- timing information
		CASE
			WHEN datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Arrived]) BETWEEN  1 AND 240 THEN  '1-240'
			WHEN datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Arrived]) BETWEEN  241 AND 390 THEN  '241-390'
			WHEN datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Arrived]) BETWEEN  391 AND 450 THEN  '391-450'
			WHEN datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Arrived]) BETWEEN  451 AND 600 THEN  '451-600'
			WHEN datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Arrived]) BETWEEN  601 AND 750 THEN  '601-750'
			WHEN datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Arrived]) BETWEEN  751 AND 1200 THEN  'Over 750'
			WHEN datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Arrived]) > 1200 THEN  'Over 20 Min'
			ELSE NULL
			END as Ph_PU_to_Unit_Arrive_Time_Intervals,
		CASE 
			WHEN datediff(s, EI.[Phone_Pickup_Time] , EU.[Unit_Arrived]) >  1200 then 1
			else 0
			end as "Calls_Over_20_Min",
		CASE
			WHEN EU.[Status] = '1' then 1
			WHEN EU.[Status] = 'C' then 1
			ELSE 0
			END as "Unit_Response_Single_vs_Multi_Response_Count"

	FROM [master].[dbo].[EMSIncidents] as EI

	inner join [master].[dbo].[EMSUnits] as EU
		on EI.[Incident_Number] = EU.[Incident_Number]

	)

	Union All
	(SELECT 
		FI."Incident_Number",
		'fire' as Data_Source,
		FU."Status",
		FI."Problem",
		FI."Incident_Type",
		FI."Response_Plan",
		FI."Priority_Description",
		'' as "EMD_Code",
		FI."Incident_Call_Disposition",
		FI."Incident_Call_Count",

		-- Incident Location
		FI."X_Long",
		FI."Y_Lat",
		FI."City",
		FI."Jurisdiction",
		FI."AFD_Response_Box",
		FI."Population_Classification",
		FI."People_Per_Mile",
		FI."IsESD17",
		FI."isETJ",
		FI."isCOP",	
		FI.[Closest_Station],
		FI.[Distance_to_S01_in_miles],
		FI.[Distance_to_S02_in_miles],
		FI.[Distance_to_S03_in_miles],
		FI.[Distance_to_S04_in_miles],
		FI.[Distance_to_S05_in_miles],
		FI.[Distance_to_S06_in_miles],
		FI.[Distance_to_S07_in_miles],
		FI.[Distance_to_S08_in_miles],
		FI.[Distance_to_S09_in_miles],
		FU.[Is_Closest_Station],
		FI.[is_walkup],

		-- Incident Times
		MONTH(FI.Phone_Pickup_Time) as "Month",
		YEAR(FI.Phone_Pickup_Time) as "Year",
		FI."Phone_Pickup_Time",
		FI."call_delayed",
		FI."ESD02_Shift",

		-- Incident Time Deltas
		case
			WHEN datediff(s, FI.[Phone_Pickup_Time] , FI.[Call_Entered_in_Queue])> 0 THEN datediff(s, FI.[Phone_Pickup_Time] , FI.[Call_Entered_in_Queue])
			ELSE NULL
			END as Phone_Pickup_to_In_Queue,
		case
			WHEN datediff(s, FI.[Call_Entered_in_Queue] , FI.[First_Unit_Assigned])> 0 THEN datediff(s, FI.[Call_Entered_in_Queue] , FI.[First_Unit_Assigned])
			ELSE NULL
			END as In_Queue_to_1st_Assigned,
		datediff(s, FI.[Phone_Pickup_Time] , FI.[First_Unit_Assigned]) as Phone_Pickup_to_1st_Assigned,
		datediff(s, FI.First_Unit_Assigned , FI.First_Unit_Enroute) as "1st_Assigned_to_1st_Enroute",
		FI.[INC_Staged_As_Arrived],
		case
			WHEN FI.[INC_Staged_As_Arrived]=1 THEN datediff(s, FI.[First_Unit_Enroute] , FI.[First_Unit_Staged])
			ELSE datediff(s, FI.[First_Unit_Enroute] , FI.[First_Unit_Arrived])
			END as "1st_Enroute_to_1st_Arrived",
		case
			WHEN FI.[INC_Staged_As_Arrived]=1 THEN datediff(s, FI.[First_Unit_Assigned] , FI.[First_Unit_Staged])
			ELSE datediff(s, FI.[First_Unit_Assigned] , FI.[First_Unit_Arrived])
			END as "1st_Assigned_to_1st_Arrived",
		case
			WHEN FI.[INC_Staged_As_Arrived]=1 THEN datediff(s, FI.[First_Unit_Staged], FI.[Call_Closed])
			ELSE datediff(s, FI.[First_Unit_Arrived] , FI.[Call_Closed])
			END as "1st_Arrived_to_Call_Closed",
		case
			WHEN FI.[INC_Staged_As_Arrived]=1 THEN datediff(s, FI.[Phone_Pickup_Time], FI.[Call_Closed]) 
			ELSE datediff(s, FI.[Phone_Pickup_Time] , FI.[First_Unit_Arrived]) 
			END as "Phone_Pickup_to_1st_Arrived",
		--datediff(s, FI.[First_Unit_Enroute] , FI.[First_Unit_Arrived]) as "1st_Enroute_to_1st_Arrived",
		--datediff(s, FI.[First_Unit_Assigned] , FI.[First_Unit_Arrived]) as "1st_Assigned_to_1st_Arrived",
		--datediff(s, FI.[First_Unit_Arrived] , FI.[Call_Closed]) as "1st_Arrived_to_Call_Closed",
		--datediff(s, FI.[Phone_Pickup_Time] , FI.[First_Unit_Arrived]) as "Phone_Pickup_to_1st_Arrived",
		datediff(s, FI.[Phone_Pickup_Time] , FI.[Call_Closed]) as "Phone_Pickup_to_Call_Closed",

		FI."Incident_ERF_Time",

		-- Unit Information
		FU."Unit",
		FU."Unit_Type",
		FU."Bucket_Type",
		FU."Station",
		FU."Department",	
		FU."Frontline_Status",
		FU."Response_Status",
		FU."Unit_Disposition",
		null as "Transport_Count",
		'' as "Transport_Mode",
		'' as "Transport_Protocol",
		FU."First_Arrived_Esri",

		-- Unit Time Deltas
		case
			WHEN datediff(s, FI.[Call_Entered_in_Queue] , FU.[Unit_Assigned])> 0 THEN datediff(s, FI.[Call_Entered_in_Queue] , FU.[Unit_Assigned])
			ELSE NULL
			END as In_Queue_to_Unit_Assigned,
		datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Assigned]) as Phone_Pickup_to_Unit_Assigned,
		datediff(s, FU.[Unit_Assigned] , FU.[Unit_Enroute]) as "Unit_Assigned_to_Enroute",
		FU.[UNIT_Staged_As_Arrived],
		case
			WHEN FU.[UNIT_Staged_As_Arrived]=1 THEN datediff(s, FU.[Unit_Enroute] , FU.[Unit_Staged])
			ELSE datediff(s, FU.[Unit_Enroute] , FU.[Unit_Arrived])
			END as "Unit_Enroute_to_Arrived",
		case
			WHEN FU.[UNIT_Staged_As_Arrived]=1 THEN datediff(s, FU.[Unit_Assigned] , FU.[Unit_Staged])
			ELSE datediff(s, FU.[Unit_Assigned] , FU.[Unit_Arrived])
			END as "Unit_Assigned_to_Arrived",
		case
			WHEN FU.[UNIT_Staged_As_Arrived]=1 THEN datediff(s, FU.[Unit_Staged], FU.[Unit_Cleared])
			ELSE datediff(s, FU.[Unit_Arrived] , FU.[Unit_Cleared])
			END as Unit_Arrived_to_Cleared,
		case
			WHEN FU.[UNIT_Staged_As_Arrived]=1 THEN datediff(s, FI.[Phone_Pickup_Time], FU.[Unit_Cleared]) 
			ELSE datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Arrived]) 
			END as "Phone_Pickup_to_Unit_Arrived",
		datediff(s, FU.[Unit_Assigned] , FU.[Unit_Cleared]) as "Unit_Assign_to_Cleared",		
		null as "Depart_to_At_Destination",
		null as "Depart_to_Cleared_Destination",
		
		-- concurrent usage
		FU."Unit_Usage_At_Time_of_Alarm",
		FU."Time_0_Active",
		FU."Time_1_Active",
		FU."Time_2_Active",
		FU."Time_3_Active",
		FU."Time_4_Active",
		FU."Time_5_Active",	
		FU."Time_6_Active",	
		FU."Time_7_Active",	
		FU."Time_8_Active",	
		FU."Time_9_Active",	
		
		-- timing information
		CASE
			WHEN datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Arrived]) BETWEEN  1 AND 240 THEN  '1-240'
			WHEN datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Arrived]) BETWEEN  241 AND 390 THEN  '241-390'
			WHEN datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Arrived]) BETWEEN  391 AND 450 THEN  '391-450'
			WHEN datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Arrived]) BETWEEN  451 AND 600 THEN  '451-600'
			WHEN datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Arrived]) BETWEEN  601 AND 750 THEN  '601-750'
			WHEN datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Arrived]) BETWEEN  751 AND 1200 THEN  'Over 750'
			WHEN datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Arrived]) > 1200 THEN  'Over 20 Min'
			ELSE NULL
			END as Ph_PU_to_Unit_Arrive_Time_Intervals,
		CASE 
			WHEN datediff(s, FI.[Phone_Pickup_Time] , FU.[Unit_Arrived]) >  1200 then 1
			else 0
			end as "Calls_Over_20_Min",
		CASE
			WHEN FU.[Status] = '1' then 1
			WHEN FU.[Status] = 'C' then 1
			ELSE 0
			END as "Unit_Response_Single_vs_Multi_Response_Count"

	FROM [master].[dbo].[FireIncidents] as FI

	inner join [master].[dbo].[FireUnits] as FU
		on FI.[Incident_Number] = FU.[Incident_Number]
	)
) t

ORDER BY
	t.[Phone_Pickup_Time],
	case
		when t.[Response_Status] = 'ONSC' then 1
		when t.[Response_Status] = 'STAGED' then 2
		when t.[Response_Status] = 'ENROUTE ONLY' then 3
		when t.[Response_Status] = 'NEVER ENROUTE' then 4
		when t.[Response_Status] = 'NEVER ASSIGNED' then 5		
		else 6
	end,
	case
		when t.[First_Arrived_Esri] = '1' then 1
		when t.[First_Arrived_Esri] = '-' then 2
		when t.[First_Arrived_Esri] = 'x' then 3	
		else 4
	end
"""

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

        csvfile = pd.read_csv(
            "C:\\Users\\bfairbanks\\Desktop\\New folder (4)\\EMS+FireRunData.csv"
        )

        # show(csvfile)
        # return 0
        csv_item = self.gis.content.get("ee94cb00272a46b7b6c98bbdb6857e9e")

        # item_props = {
        #     "type": csv_item.type,
        #     "description": csv_item.description,
        #     "tags": csv_item.tags,
        #     "typeKeywords": csv_item.typeKeywords,
        #     "title": csv_item.title,
        # }

        # csv_item.update(
        #     item_props,
        #     data=r"C:\\Users\\bfairbanks\\Desktop\\Schedule Overwrite\\EMS+FireRunData.csv",
        # )

        publish_params = {
            "type": "csv",
            "name": csv_item.title,
            "locationType": "coordinates",
            "latitudeFieldName": "Y_Lat",
            "longitudeFieldName": "X_Long",
            "coordinateFieldType": "LatitudeAndLongitude",
        }

        csv_item.publish(publish_params, overwrite=True)


# ===============================================================================================================
#  this is the data mostly put together myself
#  It will create a Dataframe, pulled from our database
#  then overwrite and push this data directly to a particular feature layer
# ===============================================================================================================


def appendFeatureLayer():
    # csv_file = r"C:\\Users\\bfairbanks\\Desktop\\esriDF.csv"
    # df = GeoAccessor.from_table(csv_file)

    from Database import SQLDatabase

    db = SQLDatabase()
    df = db.retreiveDF(
        esri_Export_Query,
        [
            "Phone_Pickup_Time",
        ],
    )

    # BIT auto converted to BOOLEAN, but exists in ESRI as INTEGER
    for col in [
        "IsESD17",
        "isETJ",
        "isCOP",
        "is_walkup",
        "call_delayed",
        "INC_Staged_As_Arrived",
        "UNIT_Staged_As_Arrived",
    ]:
        df[col] = df[col].astype(int)
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
