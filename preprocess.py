import numpy as np
import pandas as pd
from datetime import datetime as dt
from dateutil.relativedelta import relativedelta as rd
import utils

# from pandasgui import show


def preprocess(df, start=None, end=None):
    if "Ph_PU_Time" in df.columns or "Ph PU Time" in df.columns:
        fileType = "ems"
    else:
        fileType = "fire"

    print("Preparing for Analysis")
    # =================================================================
    # Get Date Range
    # =================================================================
    # if end is None and start is None:
    #     # get start of this month for end
    #     end = dt.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    #     # and then go back one month for the start
    #     start = end - rd(months=1)

    # =================================================================
    # Ensure that old EMS files will work with even PRE-RENAME-FUNCTIONS (this seems like bad practice...)
    # =================================================================
    prepreprocess = {
        # Why was this file set up so differently!?!?
        "Agency: ": "Agency",
        "Jurisdiction: ": "Jurisdiction",
        "Problem: ": "Problem",
        "Ph PU Time": "Ph_PU_Time",
        "1st Unit Assigned": "1st_Unit_Assigned",
        "1st Unit Enroute": "1st_Unit_Enroute",
        "1st Unit Staged": "1st_Unit_Staged",
        "1st Unit Arrived": "1st_Unit_Arrived",
        "Closed Time": "Closed_Time",
        "In Queue": "In_Queue",
        "2nd Unit Assigned": "2nd_Unit_Assigned",
        "2nd Unit Enroute": "2nd_Unit_Enroute",
        "2nd Unit Staged": "2nd_Unit_Staged",
        "2nd Unit Arrived": "2nd_Unit_Arrived",
        "Closed Time": "Closed_Time",
        "In Queue": "In_Queue",
        "Incident Duration Ph PU to Clear": "Incident_Duration_Ph_PU_to_Clear",
        "In Queue to Unit Assign ": "In_Queue_to_Unit_Assign_",
        "Ph PU to Unit Assign": "Ph_PU_to_Unit_Assign",
        "Unit Assign to Unit Enroute": "Unit_Assign_to_Unit_Enroute",
        "Unit Enroute to Unit Arrive": "Unit_Enroute_to_Unit_Arrive",
        "Unit Assign to Unit Arrive": "Unit_Assign_to_Unit_Arrive",
        "Unit Duration Assign to Clear": "Unit_Duration_Assign_to_Clear",
        "Ph PU to UnitArrive": "Ph_PU_to_UnitArrive",
    }
    df = df.rename(columns=prepreprocess, errors="ignore")

    # ##########################################################################################
    # TODO: correct this: add better calculations
    # ##########################################################################################
    # ##########################################################################################

    if "Unit_Type" not in df.columns and "Frontline_Status" not in df.columns:
        # there has to be a way to calculate this when not given
        df["Unit_Type"] = "Frontline"

    if "Unit_Agency" not in df.columns and "Agency" in df.columns:
        # there has to be a way to calculate this when not given
        df["Unit_Agency"] = df["Agency"]

    # =================================================================
    # Assign Destination/File Source
    # =================================================================
    # This should be (maybe not the best) a way to determine EMS or Fire source data
    if fileType == "fire":
        # as good a time as any to ensure response_area columns ACTUALLY mean the same thing
        df = df.rename(columns={"Response Area": "AFD Response Box"}, errors="ignore")

    # Can this be handeled better?
    df["Data Source"] = fileType

    # =================================================================
    # Closer match column COUNT between EMS and Fire
    # =================================================================
    # merge two to one Ph_PU_Time & Ph_PU_Data
    # if the data was null, then we need to mark this.
    # Likely also the best time to check fire for the same thing: the call came from another department, and was 'delayed' before we were made aware.
    if fileType == "ems":
        df["call_delayed"] = df.apply(
            lambda row: bool(pd.isnull(row["Ph_PU_Time"])),
            axis=1,
        )
        df["Ph_PU_Time"] = df.apply(
            lambda row: row["Ph_PU_Date"]
            if pd.isnull(row["Ph_PU_Time"])
            else row["Ph_PU_Time"],
            axis=1,
        )
    else:
        df["call_delayed"] = df.apply(
            lambda row: bool(row["Calltaker Agency"] in ["APD", "TCSO", "PPD"]),
            axis=1,
        )

    print(" -- Matching Column Contents")
    # duplicate cell:  closed_time -> ["last real unit clear incident", "incident time call closed"]
    try:
        df["Last Real Unit Clear Incident"] = df["Closed_Time"]
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
        # Match EMS data to working Fire column names
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
        # "Primary_Flag": "FirstArrived",
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
        "Ph_PU_to_UnitArrive_in_seconds": "Earliest Phone Pickup Time to Unit Arrival",
        "UnitDuration_Assign_to_Clear": "Unit Assign To Clear Call Time",
        "Itemized_Unit_Disposition": "Unit Call Disposition",
    }
    df = df.rename(columns=renames, errors="ignore")

    # =================================================================
    # replace all instances of "-" with null values
    # =================================================================
    df = df.replace("-", np.nan)

    # =================================================================
    #     Add unit type column to simplify analysis
    # =================================================================
    df = utils.addUnitType(df)
    df = utils.addBucketType(df)

    # =================================================================
    #     Remove COMM units / Information Only
    # =================================================================
    df = df[
        ~df["Unit Type"].isin(["MEDC", "FTAC", "TEST", "ALARMT", "COM", "CNTRL", "CC"])
    ]

    # =================================================================
    # order fire data by time : - 'Master Incident Number' > 'Unit Time Arrived At Scene' > 'Unit Time Staged' > 'Unit Time Enroute' > 'Unit Time Assigned'
    # =================================================================
    print(" -- Ordering Rows")

    def getFrontline(frontline_status, unit):
        if "Safe" in unit:
            return (
                True  # Do not ignore units with 'Safe' regardless of frontline status
            )
        return frontline_status == "Frontline"

    try:
        df["isFrontlineOrSafe"] = df.apply(
            lambda row: getFrontline(row["Frontline_Status"], row["Unit"]), axis=1
        )
    except Exception as ex:
        df["isFrontlineOrSafe"] = False
        print(
            f"\n\t\t------------ERROR --------------\n\t\t{ex}\n\t\t---------end ---------------\n"
        )

    df["Not Arrived"] = df.apply(
        lambda row: pd.isnull(row["Unit Time Arrived At Scene"]), axis=1
    )
    df = utils.putColAt(df, ["ignoreInStatus", "Not Arrived"], 1)

    df = df.sort_values(
        by=[
            "Master Incident Number",
            "Not Arrived",
            "isFrontlineOrSafe",  # This now comes after the primary sorting columns
            "Unit Time Arrived At Scene",
            "Unit Time Staged",
            "Unit Time Enroute",
            "Unit Time Assigned",
        ],
        ascending=[
            True,
            True,
            False,
            True,
            True,
            True,
            True,
        ],  # Adjust ascending/descending order as needed
    )

    # =================================================================
    # convert strings to datetime
    # =================================================================
    print(" -- Converting Datetimes")
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
        pd.to_datetime(df[colName], errors="raise", unit="s")

    # =================================================================
    # now that we have actual times... Filter to date range (if dates are supplied)
    # =================================================================
    if end is not None and start is not None:
        print(" -- Filtering to Selected Date Range")
        df = df[
            (df["Earliest Time Phone Pickup AFD or EMS"] >= start)
            & (df["Earliest Time Phone Pickup AFD or EMS"] < end)
            | (df["Incident Time Call Entered in Queue"] >= start)
            & (df["Incident Time Call Entered in Queue"] < end)
        ]

    # =================================================================
    # Drop timeDelta Columns, as we can calculate them ourselves
    # =================================================================
    print(" -- Removing any Existing Time Deltas")
    timeDeltasToConvert = [
        # Known Fire Columns
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
        # Known EMS Columns
        "Ph_PU_to_In_Queue_in_seconds",
        "In_Queue_to_1stAssign_in_seconds",
        "Ph_PU_to_1stAssign_in_seconds",
        "1stAssign_to_1stEnroute_in_seconds",
        "1stEnroute_to_1stArrive_in_seconds",
        "1stAssign_to_1stArrive_in_seconds",
        "1stArrive_to_LastUnit_Cleared_in_seconds",
        "Ph_PU_to_1stArrive_in_seconds",
        "IncidentDuration_Ph_PU_to_Clear_in_seconds_in_seconds",
        "In Queue_to_UnitAssign_in_seconds",
        "UnitAssign_to_UnitEnroute_in_seconds",
        "UnitEnroute_to_UnitArrive_in_seconds",
        "UnitAssign_to_UnitArrive_in_seconds",
        "UnitArrive_to_Clear_in_seconds",
        "Ph_PU_to_UnitArrive_in_seconds",
        "UnitDuration_Assign_to_Clear_in_seconds",
        "Depart_to_At_Destination_in_seconds",
        "Depart_to_Cleared_Destination_in_seconds",
    ]

    df = df.drop(timeDeltasToConvert, axis=1, errors="ignore")

    # =================================================================
    # Reset index for order and size
    # =================================================================
    print(" -- Resetting Index")
    df = df.reset_index(drop=True)

    # =================================================================
    #     Drop useless data
    # =================================================================
    print(" -- Dropping Extra/Useless Data")
    df = df.drop(
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
    return df


if __name__ == "__main__":
    # load test files
    import loadTestFile

    df = loadTestFile.get()
    # show(df)
