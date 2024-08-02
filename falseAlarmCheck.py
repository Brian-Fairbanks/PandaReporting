from dotenv import load_dotenv
from os import getenv, remove
import datetime

import logging

# Setup and consts
# ==============================================================================================================
LOG_FILENAME = f"../Logs/FalseAlarmCheck_{(datetime.datetime.now()).strftime('%y-%m-%d_%H-%M')}.txt"
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)

esri_Export_Query = "SELECT * FROM [dbo].[v_esri_export-Query-Filtered] where [Phone_Pickup_Time] >= '10/01/2022 00:00:00.00' and [Data_Source] = 'fire'"
alarmQuery = "SELECT * FROM [dbo].[v_esri_export-Query-Filtered] where [Phone_Pickup_Time] >= '01/01/2021 00:00:00.00' and [Data_Source] = 'fire' and [problem] like '%larm%' and [Incident_Call_Disposition] in ('CAR - Canceled On Arrival', 'CBA - Canceled Before Arrival')"

# Database Access
# ==============================================================================================================
def getFormattedTable(query):
    """Helper function to get a specific table from SQL - Returns Dataframe"""
    from Database import SQLDatabase

    try:
        db = SQLDatabase()
        df = db.retrieve_df(
            query,
            [
                "Phone_Pickup_Time",
            ],
        )
        return df
    except:
        print(
            "  - Process Failed!  - Error in Database Extraction - Please check the logs."
        )
        logging.exception("Exception found in Database Extraction")
        exit(1)


def getFullTable():
    return getFormattedTable(esri_Export_Query)


def getFalseAlarms():
    return getFormattedTable(alarmQuery)


def analyzeFalseAlarms(df):
    alarms = getFalseAlarms()
    print(alarms)
    return None


# ==================================================================================================
# ==================================================================================================
def main():
    df = getFullTable()
    analyzeFalseAlarms(df)


if __name__ == "__main__":
    main()
