import numpy as np
import pandas as pd
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd
import utils


# Setup Logging for the remainder of the data
import logging

runtime = dt.now().strftime("%Y.%m.%d %H.%M")

# set up logging folder
writePath = "../Logs"

# logging setup - write to output file as well as printing visably
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger()
logger.addHandler(logging.FileHandler(f"{writePath}/RunLog-{runtime}.log", "a"))
print = logger.info


def dumpRawData(df, type):
    print("Dumping Raw Data to Database")

    from Database import SQLDatabase

    db = SQLDatabase()

    db.insertRaw(df, type)


def main():
    # load test files
    import loadTestFile

    df = loadTestFile.get(True)
    # run test file
    from pandasgui import show

    df = df.replace("-", np.nan)

    incorrectColumns = [
        "Earliest Time Phone Pickup to In Queue",
        "In Queue to 1st Real Unit Assigned",
        "Earliest Time Phone Pickup to 1st Real Unit Assigned",
        "Incident Turnout - 1st Real Unit Assigned to 1st Real Unit Enroute",
        "Incident Travel Time - 1st Real Unit Enroute to 1st Real Unit Arrived ",
        "Incident First Unit Response - 1st Real Unit Assigned to 1st Real Unit Arrived",
        "Time Spent OnScene - 1st Real Unit Arrived to Last Real Unit Call Cleared",
        "Earliest Time Phone Pickup to 1st Real Unit Arrived",
        "IncidentDuration - Earliest Time Phone Pickup to Last Real Unit Call Cleared",
        "In Queue to Unit Dispatch",
        "Unit Dispatch to Respond Time",
        "Unit Respond to Arrival",
        "Unit Dispatch to Onscene",
        "Unit OnScene to Clear Call",
        "Earliest Phone Pickup Time to Unit Arrival",
        "Unit Assign To Clear Call Time",
    ]

    def fixTime(time):
        # print(f"{time} : {type(time)}")
        if pd.isna(time):
            return None
        if type(time) is int or type(time) is float:
            return time

        seconds = (time.hour * 60 + time.minute) * 60 + time.second

        return seconds

    for column in incorrectColumns:
        df[column] = df.apply(lambda row: fixTime(row[column]), axis=1)

    show(df)
    dumpRawData(df, "fire")


if __name__ == "__main__":
    main()
