import numpy as np
import pandas as pd
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd
import utils


def preprocess(fireDF, start=None, end=None):
    # =================================================================
    # Get Date Range
    # =================================================================
    if end is None and start is None:
        # get start of this month for end
        end = dt.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # and then go back one month for the start
        start = end - rd(months=1)

    # =================================================================
    # correct naming conventions
    # =================================================================
    renames = {
        "Master_Incident_Number": "Master Incident Number",
        "Last Unit Clear Incident": "Last Real Unit Clear Incident",
        "X_Long": "X-Long",
    }
    fireDF = fireDF.rename(columns=renames, errors="ignore")

    # =================================================================
    # replace all instances of "-" with null values
    # =================================================================
    fireDF = fireDF.replace("-", np.nan)

    # =================================================================
    # order fire data by time : - 'Master Incident Number' > 'Unit Time Arrived At Scene' > 'Unit Time Staged' > 'Unit Time Enroute' > 'Unit Time Assigned'
    # =================================================================
    def getFrontline(name):
        if name == "Frontline":
            return False
        return True

    fireDF["ignoreInStatus"] = fireDF.apply(
        lambda row: getFrontline(row["Frontline_Status"]), axis=1
    )

    fireDF["Not Arrived"] = fireDF.apply(
        lambda row: pd.isnull(row["Unit Time Arrived At Scene"]), axis=1
    )

    fireDF = utils.putColAt(fireDF, ["ignoreInStatus", "Not Arrived"], 1)

    fireDF = fireDF.sort_values(
        by=[
            "Master Incident Number",
            "Not Arrived",
            "ignoreInStatus",
            "Unit Time Arrived At Scene",
            "Unit Time Staged",
            "Unit Time Enroute",
            "Unit Time Assigned",
        ]
    )

    # =================================================================
    # convert strings to datetime
    # =================================================================
    time_columns_to_convert = [
        "Earliest Time Phone Pickup AFD or EMS",
        "Incident Time Call Entered in Queue",
        "Time First Real Unit Assigned",
        "Time First Real Unit Enroute",
        "Incident Time First Staged",
        "Time First Real Unit Arrived",
        "Incident Time Call Closed",
        "Last Real Unit Clear Incident",
    ]

    for index, colName in enumerate(time_columns_to_convert):
        pd.to_datetime(fireDF[colName], errors="raise", unit="s")

    # =================================================================
    # now that we have actual times... Filter to date range
    # =================================================================
    fireDF = fireDF[
        (fireDF["Earliest Time Phone Pickup AFD or EMS"] >= start)
        & (fireDF["Earliest Time Phone Pickup AFD or EMS"] < end)
        | (fireDF["Incident Time Call Entered in Queue"] >= start)
        & (fireDF["Incident Time Call Entered in Queue"] < end)
    ]

    # =================================================================
    # convert strings/dates to timeDelta
    # =================================================================
    timeDeltasToConvert = [
        "Earliest Time Phone Pickup to In Queue",
        "In Queue to 1st Real Unit Assigned",
        "Earliest Time Phone Pickup to 1st Real Unit Assigned",
        "Incident Turnout - 1st Real Unit Assigned to 1st Real Unit Enroute",
        "Incident Travel Time - 1st Real Unit Enroute to 1st Real Unit Arrived ",
        "Incident First Unit Response - 1st Real Unit Assigned to 1st Real Unit Arrived",
        "Earliest Time Phone Pickup to 1st Real Unit Arrived",
        "Time Spent OnScene - 1st Real Unit Arrived to Last Real Unit Call Cleared",
        "IncidentDuration - Earliest Time Phone Pickup to Last Real Unit Call Cleared",
        "In Queue to Unit Dispatch",
        "Unit Dispatch to Respond Time",
        "Unit Respond to Arrival",
        "Unit Dispatch to Onscene",
        "Unit OnScene to Clear Call",
        "Earliest Phone Pickup Time to Unit Arrival",
        "Unit Assign To Clear Call Time",
    ]

    fireDF = fireDF.drop(timeDeltasToConvert, axis=1)

    # =================================================================
    # Reset index for order and size
    # =================================================================
    fireDF = fireDF.reset_index(drop=True)

    # =================================================================
    #     Drop useless data
    # =================================================================
    fireDF = fireDF.drop(
        [
            "ESD02_Record",
            "Master Incident Without First Two Digits",
            "ignoreInStatus",
            "Not Arrived",
        ],
        axis=1,
    )

    return fireDF


if __name__ == "__main__":
    # load test files
    import loadTestFile
    from pandasgui import show

    df = loadTestFile.get()
    show(df)
