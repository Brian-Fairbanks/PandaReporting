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

    # duplicate field if EMS data exists for EMS data
    try:
        fireDF["Last Real Unit Clear Incident"] = fireDF["Closed_Time"]
    except:
        # closed_time must not exist, which means last real unit clear incident already does anyway.
        pass

    # =================================================================
    # correct naming conventions
    # =================================================================
    print(" -- renaming")
    renames = {
        "Master_Incident_Number": "Master Incident Number",
        "Last Unit Clear Incident": "Last Real Unit Clear Incident",
        "X_Long": "X-Long",
        # fix for EMS to work properly
        "Incident": "Master Incident Number",
        "not in EMS": "Master Incident Without First Two Digits",
        "Agency": "Calltaker Agency",
        "Address": "Address of Incident",
        "Response_Area": "Response Area",
        "Incident_Type": "Incident Type",
        "Response_Plan": "Response Plan",
        "Priority_Number": "PriorityDescription",
        "AFD Only": "Alarm Level",
        "Mapsco": "Map_Info",
        "Longitude_X": "X-Long",
        "Latitude_Y": "Y_Lat",
        "Shift": "ESD02_Shift",
        "Ph_PU_Time": "Earliest Time Phone Pickup AFD or EMS",
        "In_Queue": "Incident Time Call Entered in Queue",
        "Closed_Time": "Incident Time Call Closed",
        "1st_Unit_Assigned": "Time First Real Unit Assigned",
        "1st_Unit_Enroute": "Time First Real Unit Enroute",
        "1st_Unit_Staged": "Incident Time First Staged",
        "1st_Unit_Arrived": "Time First Real Unit Arrived",
        "Ph_PU_to_In_Queue": "Earliest Time Phone Pickup to In Queue",
        "In_Queue_to_1stAssign": "In Queue to 1st Real Unit Assigned",
        "Ph_PU_to_1stAssign": "Earliest Time Phone Pickup to 1st Real Unit Assigned",
        "1stAssign_to_1stEnroute": "Incident Turnout - 1st Real Unit Assigned to 1st Real Unit Enroute",
        "1stEnroute_to_1stArrive": "Incident Travel Time - 1st Real Unit Enroute to 1st Real Unit Arrived",
        "1stAssign_to_1stArrive": "Incident First Unit Response - 1st Real Unit Assigned to 1st Real Unit Arrived",
        "1stArrive_to_LastUnit_Cleared": "Time Spent OnScene - 1st Real Unit Arrived to Last Real Unit Call Cleared",
        "Ph_PU_to_1stArrive": "Earliest Time Phone Pickup to 1st Real Unit Arrived",
        "IncidentDuration_Ph_PU_to_Clear": "IncidentDuration - Earliest Time Phone Pickup to Last Real Unit Call Cleared",
        "Final_Disposition": "Incident Call Disposition",
        "Unit": "Radio_Name",
        "Unit_Agency": "Department",
        "Unit_Type": "Frontline_Status",
        "Address_at_Assign": "Location_At_Assign_Time",
        "Primary_Flag": "FirstArrived",
        "Assigned": "Unit Time Assigned",
        "Enroute": "Unit Time Enroute",
        "Staged": "Unit Time Staged",
        "Arrived": "Unit Time Arrived At Scene",
        "Complete": "Unit Time Call Cleared",
        "In Queue_to_UnitAssign": "In Queue to Unit Dispatch",
        "UnitAssign_to_UnitEnroute": "Unit Dispatch to Respond Time",
        "UnitEnroute_to_UnitArrive": "Unit Respond to Arrival",
        "UnitAssign_to_UnitArrive": "Unit Dispatch to Onscene",
        "UnitArrive_to_Clear": "Unit OnScene to Clear Call",
        "Ph_PU_to_UnitArrive_in_seconds": "Earliest Phone Pickup to Unit Arrival",
        "UnitDuration_Assign_to_Clear": "Unit Assign To Clear Call Time",
        "Itemized_Unit_Disposition": "Unit Call Disposition",
    }
    fireDF = fireDF.rename(columns=renames, errors="ignore")

    # =================================================================
    # replace all instances of "-" with null values
    # =================================================================
    fireDF = fireDF.replace("-", np.nan)

    # =================================================================
    # order fire data by time : - 'Master Incident Number' > 'Unit Time Arrived At Scene' > 'Unit Time Staged' > 'Unit Time Enroute' > 'Unit Time Assigned'
    # =================================================================
    print(" -- Ordering")

    def getFrontline(name):
        if name == "Frontline":
            return False
        return True

    try:
        fireDF["ignoreInStatus"] = fireDF.apply(
            lambda row: getFrontline(row["Frontline_Status"]), axis=1
        )
    except Exception as ex:
        print(
            f"\n------------ERROR --------------\n{ex}\n---------end ---------------\n"
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
    print(" -- Getting Datetimes")
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
    print(" -- Filtering to Selected Date Range")
    fireDF = fireDF[
        (fireDF["Earliest Time Phone Pickup AFD or EMS"] >= start)
        & (fireDF["Earliest Time Phone Pickup AFD or EMS"] < end)
        | (fireDF["Incident Time Call Entered in Queue"] >= start)
        & (fireDF["Incident Time Call Entered in Queue"] < end)
    ]

    # =================================================================
    # convert strings/dates to timeDelta
    # =================================================================
    print(" -- Removing any Existing Time Deltas")
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

    fireDF = fireDF.drop(timeDeltasToConvert, axis=1, errors="ignore")

    # =================================================================
    # Reset index for order and size
    # =================================================================
    print(" -- Resetting Index")
    fireDF = fireDF.reset_index(drop=True)

    # =================================================================
    #     Drop useless data
    # =================================================================
    print(" -- Dropping Data")
    fireDF = fireDF.drop(
        [
            "ESD02_Record",
            "Master Incident Without First Two Digits",
            "ignoreInStatus",
            "Not Arrived",
        ],
        axis=1,
        errors="ignore",
    )

    print(" -- Complete!")
    return fireDF


if __name__ == "__main__":
    # load test files
    import loadTestFile
    from pandasgui import show

    df = loadTestFile.get()
    show(df)
