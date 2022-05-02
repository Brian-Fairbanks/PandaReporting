import pandas as pd
from datetime import datetime as dt
from preprocess import preprocess


def get():
    # fire = "Fire 07 2021 ESD02_RAWDATA_UPDATE_Fairbanks.xlsx"
    # fire = "fire 10-21.xlsx"
    # fire = "ESD02 January 22 Raw Data.xlsx"
    # fire = "ESD02 February 22 Raw Data.xlsx"
    # fire = "March 22 ESD02 Raw Data.xlsx"
    # fire = "March EMS - ESD02 Dataset.xlsx"
    # fire = "FIRE ESD02_2006_2011.xlsx"
    # fire = "04 April ESD02 Raw Data.xlsx"
    fire = "04 April ESD02 EMS data.xlsx"
    startTime = pd.to_datetime("1/1/05 00:00:00")
    endTime = pd.to_datetime("1/1/32 00:00:00")
    # fire = "fire 06 2021 Raw QV Data.xlsx"

    fireDF = pd.read_excel(fire)

    return preprocess(fireDF, startTime, endTime)
    # return preprocess(fireDF)
