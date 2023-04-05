import pandas as pd
from datetime import datetime as dt
from preprocess import preprocess


def get(requestingRaw=False):
    # file = "Fire 07 2021 ESD02_RAWDATA_UPDATE_Fairbanks.xlsx"
    # file = "fire 10-21.xlsx"
    # file = "ESD02 January 22 Raw Data.xlsx"
    # file = "ESD02 February 22 Raw Data.xlsx"
    # file = "March 22 ESD02 Raw Data.xlsx"
    # file = "March EMS - ESD02 Dataset.xlsx"
    # file = "FIRE ESD02_2006_2011.xlsx"
    # file = "04 April ESD02 Raw Data.xlsx"
    # file = "04 April ESD02 EMS data.xlsx"
    # file = "EMS Report Data 2021.xlsx"
    # file = "November 2021 - April 2022 Fire Data.xlsx"
    # file = "Fire Jan 2020 - May 2022 Raw Data.xlsx"
    # file = "May 22 ESD02 Raw Data.xlsx"
    # file = "05 April ESD02 EMS Data.xlsx"
    startTime = pd.to_datetime("1/1/05 00:00:00")
    endTime = pd.to_datetime("1/1/32 00:00:00")
    # file = "fire 06 2021 Raw QV Data.xlsx"
    # file = "XLSs\\05 May ESD02 EMS Data.xlsx"
    # file = "XLSs\\06 June ESD02 EMS Data.xlsx"
    # file = "ESD02 Raw Data - Weekly-8.22.22.xlsx"
    file = "2012_2022 ESD02 Raw Data - AFD Fire.xlsx"
    fireDF = pd.read_excel(file)

    if requestingRaw:
        return fireDF
    return preprocess(fireDF, startTime, endTime)
    # return preprocess(fireDF)
