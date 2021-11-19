import pandas as pd
import numpy as np
from preprocess import preprocess


def get():
    # fire = "Fire 07 2021 ESD02_RAWDATA_UPDATE_Fairbanks.xlsx"
    fire = "fire 10-21.xlsx"
    # fire = "fire 06 2021 Raw QV Data.xlsx"
    # gracefulCrash("A file was not found for Fire Data")
    # if ems == "":
    #     gracefulCrash("A file was not found for EMS Data")

    fireDF = pd.read_excel(fire)
    ### save an OG copy (Auto duplicate when you start working)
    # try:
    #     # check if file_original exists, and only write to it if it does not.
    #     fireDF.to_excel(fire.split(".")[0] + "_Original.xlsx")
    # except Exception as ex:
    #     print(ex)
    #     input("\nPress enter to exit")
    #     exit(1)

    # =================================================================
    #     Formatting and Renaming of DataFrames
    # =================================================================

    # fireDF.rename(columns={"Master Incident Number": "Incident Number"})

    # confirm time values are recognized as time values
    # fireDF["Earliest Time Phone Pickup AFD or EMS"] = pd.to_datetime(
    #     fireDF["Earliest Time Phone Pickup AFD or EMS"],
    #     # format="%m/%d/%Y %H:%M:%S",
    #     infer_datetime_format=True,
    #     errors="ignore",
    # )

    #    "Last Real Unit Clear Incident"
    #    "Earliest Time Phone Pickup AFD or EMS"

    return preprocess(fireDF)
