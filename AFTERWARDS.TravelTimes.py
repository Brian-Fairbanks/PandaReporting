import logging
from pandasgui import show
import datetime as datetime

import random

random.seed(10)
import math


# ==================================================================================
# Collect Data
# ==================================================================================
db_query = """
SELECT
	[Incident_Number]
    ,[Phone_Pickup_Time]
	,[Unit]
	,[Department]
	,[Station] as originalStation
	,null as station
	,[X_Long]
    ,[Y_Lat]
	,[Closest_Station] as originalClosest
	,[Distance_to_S01_in_miles] as originalDistance01
    ,[Distance_to_S02_in_miles] as originalDistance02
    ,[Distance_to_S03_in_miles] as originalDistance03
    ,[Distance_to_S04_in_miles] as originalDistance04
    ,[Distance_to_S05_in_miles] as originalDistance05
    ,[Distance_to_S06_in_miles] as originalDistance06
    ,[Distance_to_S07_in_miles] as originalDistance07
    ,[Distance_to_S08_in_miles] as originalDistance08
    ,[Distance_to_S09_in_miles] as originalDistance09
    ,[1st_Enroute_to_1st_Arrived] as travelTime
FROM [dbo].[v_esri_export-Query-Filtered]
WHERE [Phone_Pickup_Time] >= '2022-09-01 00:00:00.00'
AND [Phone_Pickup_Time] < '2022-12-01 00:00:00.00'
AND [Data_Source] = 'fire'
"""


def getFormattedTable():
    from Database import SQLDatabase

    try:
        db = SQLDatabase()
        df = db.retrieve_df(
            db_query,
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


# ==================================================================================
# Recalculate Station
# ==================================================================================
def EstimateNewStation(time, stationDict):
    # here is the magic of the program!

    # outright remove possibility for stations:
    del stationDict["S03"]
    del stationDict["S04"]
    if not (datetime.time(8, 0, 0) <= time.time() < datetime.time(17, 0, 0)):
        del stationDict["S01"]

    # and remove nulls from array
    cleanDict = {k: v for k, v in stationDict.items() if v and not math.isnan(v)}

    # if nothing left, return None
    if cleanDict == {}:
        return None
    #  sort the array, so we can grab either closest or second closest
    closestArray = list(cleanDict.values())
    closestArray.sort()

    # use second closest 25% of the time
    rand = math.floor(random.randint(1, 5) / 4)

    value = None
    if closestArray and rand < len(closestArray):
        print(f"#{rand} closest element of {cleanDict}", end=" is ")
        print(f"{closestArray[rand]}")
        value = [k for k, v in cleanDict.items() if v == closestArray[rand]][0]
    elif closestArray:
        rand = 0
        print(f"#{rand} closest element of {cleanDict}", end=" is ")
        print(f"{closestArray[rand]}")
        value = [k for k, v in cleanDict.items() if v == closestArray[rand]][0]
    return value


def recalculateStations(df):
    df["station"] = df.apply(
        lambda x: EstimateNewStation(
            x.Phone_Pickup_Time,
            {
                "S01": x.originalDistance01,
                "S02": x.originalDistance02,
                "S03": x.originalDistance03,
                "S04": x.originalDistance04,
                "S05": x.originalDistance05,
                "S06": x.originalDistance06,
                "S07": x.originalDistance07,
                "S09": x.originalDistance09,
            },
        ),
        axis=1,
    )


def addTimeEstimate(df):
    def calculateTime(station, travelTime, stationDict):
        cleanDict = {k: v for k, v in stationDict.items() if v and not math.isnan(v)}
        if station not in cleanDict.keys():
            return None
        return travelTime / cleanDict[station]

    df["ind_Seconds_Per_Mile"] = df.apply(
        lambda x: calculateTime(
            x.originalStation,
            x.travelTime,
            {
                "S01": x.originalDistance01,
                "S02": x.originalDistance02,
                "S03": x.originalDistance03,
                "S04": x.originalDistance04,
                "S05": x.originalDistance05,
                "S06": x.originalDistance06,
                "S07": x.originalDistance07,
                "S09": x.originalDistance09,
            },
        ),
        axis=1,
    )

    def estimateTime(rtt, station, stationDict):
        cleanDict = {k: v for k, v in stationDict.items() if v and not math.isnan(v)}
        if station not in cleanDict.keys() or not rtt:
            return None
        return rtt * cleanDict[station]

    df["est_ind_Travel_Time"] = df.apply(
        lambda x: estimateTime(
            x.ind_Seconds_Per_Mile,
            x.station,
            {
                "S01": x.originalDistance01,
                "S02": x.originalDistance02,
                "S03": x.originalDistance03,
                "S04": x.originalDistance04,
                "S05": x.originalDistance05,
                "S06": x.originalDistance06,
                "S07": x.originalDistance07,
                "S09": x.originalDistance09,
            },
        ),
        axis=1,
    )

    def quickCalc(act, est):
        if not act or not est:
            return None
        return ((est / act) - 1) * 100

    df["Percent_Increase_ind"] = df.apply(
        lambda x: quickCalc(x.travelTime, x.est_ind_Travel_Time),
        axis=1,
    )

    avg = df["ind_Seconds_Per_Mile"].mean()

    df["avg_Seconds_Per_Mile"] = avg
    df["est_avg_Travel_Time"] = df.apply(
        lambda x: estimateTime(
            avg,
            x.station,
            {
                "S01": x.originalDistance01,
                "S02": x.originalDistance02,
                "S03": x.originalDistance03,
                "S04": x.originalDistance04,
                "S05": x.originalDistance05,
                "S06": x.originalDistance06,
                "S07": x.originalDistance07,
                "S09": x.originalDistance09,
            },
        ),
        axis=1,
    )
    df["Percent_Increase_Avg"] = df.apply(
        lambda x: quickCalc(x.travelTime, x.est_avg_Travel_Time),
        axis=1,
    )


def main():
    df = getFormattedTable()
    recalculateStations(df)
    addTimeEstimate(df)
    show(df)
    # dfInTimeFrame = allData[(allData['date'] >= '2022-07-01 00:00:00.00') & (allData['date'] < '2022-10-01 00:00:00.00')]


if __name__ == "__main__":
    main()
