# Built in
import datetime

# Dependancies
from pandasgui import show
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Sibling Modules
from crf import getCRF
import utils
import ConcurrentUse as cu
import roads as rd
import getData as data
import timeBreakdowns as tb

# Dont warn me about potentially assigning a copy
pd.options.mode.chained_assignment = None

# import global additional/updatable json data
locations = data.getLocations()
reserveUnits = data.getReserves()
stationDict = data.getStations()


# Create a Pandas Excel writer using XlsxWriter as the engine.
# Also set the default datetime and date formats.

# writer = pd.ExcelWriter(
#     "Output.xlsx",
#     engine="xlsxwriter",
#     datetime_format="mmm d yyyy hh:mm:ss",
#     date_format="mmmm dd yyyy",
# )

# ##############################################################################################################################################
#     Station Assignment Functions
# ##############################################################################################################################################


def getPop(EMSPopDenData):
    # =================================================================
    #     Set pop density values Values
    # =================================================================
    import popden

    EMSPopDenData = popden.addPopDen(EMSPopDenData)
    show(EMSPopDenData)


if __name__ == "__main__":
    # load test files
    from datetime import datetime as dt
    from preprocess import preprocess

    fire = "EMS data for Acadian Analysis.xlsx"
    startTime = pd.to_datetime("12/1/19 00:00:00")
    endTime = pd.to_datetime("1/1/23 00:00:00")
    EMSPopDenData = pd.read_excel(fire)
    # fireDF = preprocess(fireDF, startTime, endTime)
    # run test file
    getPop(EMSPopDenData)
