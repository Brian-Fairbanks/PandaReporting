# Built in
import datetime

# Dependancies
# from pandasgui import show
import pandas as pd
import numpy as np

# Sibling Modules
from crf import getCRF
import utils
import ConcurrentUse as cu
import roads as rd
import getData as data
import timeBreakdowns as tb
import naming as n

# Dont warn me about potentially assigning a copy
pd.options.mode.chained_assignment = None

# import global additional/updatable json data
locations = data.getLocations()
reserveUnits = data.getReserves()
stationDict = data.getStations()
ourNames = ["AUSTIN-TRAVIS COUNTY EMS", "ESD02 - Pflugerville", "ESD02"]

# ##############################################################################################################################################
#     Station Assignment Functions
# ##############################################################################################################################################


def addIsClosestStation(df):
    def getIsClosestStation(station, closest):
        if pd.isnull(closest):
            return None
        return station == closest

    df["Is Closest Station"] = df.apply(
        lambda row: getIsClosestStation(row["Station"], row["Closest Station"]), axis=1
    )
    return df


def getLoc(address):
    current_location = str(address).lower()
    stationNum = None
    if "fs020" in current_location:
        stationNum = "S" + current_location[-2:]
    # else check it against known street names (specified at the top of the file)
    else:
        for street in locations.keys():
            if street.lower() in current_location:
                stationNum = locations[street]
                break
    return stationNum


def getLocAtAssign(station, address):
    return station == getLoc(address)


def addLocAtAssignToDF(df):
    if "Location_At_Assign_Time" in df.columns:
        df["Assigned at Station"] = df.apply(
            lambda row: getLocAtAssign(row["Station"], row["Location_At_Assign_Time"]),
            axis=1,
        )
    else:
        df["Assigned at Station"] = "Unknown"
    return df


def getStations(fireDF):
    # Add a new Stations Column
    # -------------------------------------------------------------------------------------------
    # look up radio name and department
    # S01, S02, S03, S04, S05, ADMIN, AFD, OTHER

    # pulled from fire data xlsx
    # =IF(AND(AZ1="ESD02 - Pflugerville",BA1="Frontline"),RIGHT(Fire_Table[@[Radio Name]],3),"ADMIN ESD02")

    az = "Department"
    ba = "Frontline_Status"

    conditions = [
        # fmt: off
        (fireDF["Frontline_Status"] == "Not a unit"),  # 0
        (fireDF[az] == "AFD") & (fireDF[ba] == "Other"),  # 1
        (fireDF[az] == "AFD"),
        (fireDF[az] == "ESD12 - Manor") & (fireDF[ba] == "Other"),
        (fireDF[az] == "ESD12 - Manor"),
        (fireDF[az] == "WC - Round Rock") & (fireDF[ba] == "Other"),
        (fireDF[az] == "WC - Round Rock"),
        (fireDF[az] == "A/TCEMS") & (fireDF[ba] == "Administrative Staff"),
        (fireDF[az] == "A/TCEMS"),
        (fireDF[az].isin(ourNames)) & (fireDF[ba].isin(["Other", "Command"])),
        (fireDF[az].isin(ourNames)) & (fireDF["Radio_Name"] == "QNT261"),  # 6
        (fireDF[az].isin(ourNames)) & (fireDF["Radio_Name"].str.contains("BAT20")),
        (fireDF[az].isin(ourNames)) & (fireDF["Radio_Name"].str.contains("MED271")),
        (fireDF[az].isin(ourNames)) & (fireDF["Radio_Name"].str.contains("MED281")),
        (fireDF[az].isin(ourNames)) & (fireDF["Radio_Name"].str.contains("MED270")),
        (fireDF[az].isin(ourNames)) & (fireDF["Radio_Name"].str.contains("MED280")),
            # account for instances where vehicle is filling in for another.  
            # We will need to deterimine which station it is filling in for based on the location at time of assignment
        (fireDF[az].isin(ourNames)) & (fireDF["Radio_Name"].isin(reserveUnits)),
        (fireDF[az].isin(ourNames)),
        # fmt: on
    ]
    choices = [
        "Not a unit",  # 0
        "AFD Other",
        "AFD",
        "ESD12 Manor Other",
        "ESD12 Manor",
        "RRFD Other",
        "RRFD",
        "A/TCEMS Other",
        "A/TCEMS",
        "ADMIN",
        "S05",  # 6
        "S01",
        "S03",
        "S02",
        "S04",
        "S03",
        # mark instances of reserved units, so we can run an extra filter on these in a moment
        "Reserve Unit",
        "S0" + fireDF["Radio_Name"].str[-2],
    ]

    fireDF["Station"] = np.select(conditions, choices, default=fireDF[az])

    # correct reserved units, as I have spent far too long trying to do this all as one part
    #    ----------------------------
    rescor = fireDF.index[fireDF["Station"] == "Reserve Unit"].tolist()
    # again, this is going to be very slow compared to other vectorized checks/changes
    for i in rescor:
        # get location as a string
        curLoc = str(fireDF.loc[i, "Location_At_Assign_Time"]).lower()
        # set default value
        stationNum = "UNKNOWN"

        # check if it has already been specified
        if "fs020" in curLoc:
            stationNum = "S" + curLoc[-2:]
        # else check it against known street names (specified at the top of the file)
        else:
            for street in locations.keys():
                if street.lower() in curLoc:
                    stationNum = locations[street]
                    break
        # and then set the finalized location
        fireDF.loc[i, "Station"] = stationNum

    # move Status col to front
    fireDF = utils.putColAt(fireDF, ["Station", "Status"], 0)

    return fireDF


def addFirstArrived(df):
    # df["FirstArrived"] = df[
    #     df["Unit Time Arrived At Scene"] == df["Time First Real Unit Arrived"]
    # ]
    df["FirstArrived"] = False
    # get array of unique Incidents
    unique_incidents = df["Master Incident Number"].unique()
    # for each incident, get array of calls
    for incident in unique_incidents:
        incident_data = df[df["Master Incident Number"] == incident]
        # show(incident_data)
        try:
            # get earliest arrival (either index of earliest, or null if not exits)
            first = incident_data[
                incident_data["Unit Time Arrived At Scene"]
                == incident_data["Unit Time Arrived At Scene"].min()
            ].index[0]
            # and set earliest arrival of all for incident as 'FirstArrived' if it is less than firstUnitArrived (accounts for out of jurisdiction other units)
            if (
                df.loc[first, "Unit Time Arrived At Scene"]
                <= df.loc[first, "Time First Real Unit Arrived"]
            ):
                df.loc[first, "FirstArrived"] = True
        except:
            pass
    return df


def formatPriority(val):
    """takes a string, removes any alpha characters, and prepend priority with P.
    Returns this new value"""
    if pd.isnull(val):
        return None
    ret = "P" + "".join(c for c in str(val) if c.isdigit())
    # print(f"{val}  :  {ret}")
    return ret


def reprocessPriority(df):
    # preserve the original data
    df["Priority_Description_Orig"] = df["PriorityDescription"]
    df = utils.putColAfter(df, ["Priority_Description_Orig"], "PriorityDescription")

    # format priority_description column
    df["PriorityDescription"] = df.apply(
        lambda x: formatPriority(x["PriorityDescription"]),
        axis=1,
    )

    return df


# ##############################################################################################################################################
#     Main Code
# ##############################################################################################################################################


def analyzeFire(fireDF):
    """
    Extrapolates information from a specifically formatted XLS file, returning a larger set presumed, formatted, and corracted data

    Parameters
    --------------------------------
    df : Dataframe
        An excel file of Fire Call information as received

    Returns
    --------------------------------
    Dataframe:
        A larger dataframe with additional information gleamed from the passed file.
    """

    dataSource = None
    # =================================================================
    #    Confirm creation of FirstArrived column
    # =================================================================
    # file should have this column already, ems should need it.  Log this step.
    if "FirstArrived" in fireDF:
        dataSource = "fire"
    else:
        dataSource = "ems"

    # =================================================================
    #    Match Incident Number Format - drop anything not a number
    # =================================================================
    # fireDF["Master Incident Number"] = fireDF.apply(
    #     lambda x: "".join(c for c in str(x["Master Incident Number"]) if c.isdigit()),
    #     axis=1,
    # )
    # =================================================================
    #    Add Response Status Information
    # =================================================================
    # fireDF["Response_Status"] = None

    def get_response_status(uonsc, ustaged, uenroute, uassigned):
        # "ONSC" - "Unit arrived on scene"
        if pd.notnull(uonsc):
            return "ONSC"
        # "STAGED" - "Unit staged, but never arrived"
        if pd.notnull(ustaged):
            return "STAGED"
        # "ENROUTE ONLY" - "Unit enroute only, but never arrived"
        if pd.notnull(uenroute):
            return "ENROUTE ONLY"
        # "NEVER ENROUTE" - "Never enroute"
        if pd.notnull(uassigned):
            return "NEVER ENROUTE"
        # "NEVER ASSIGNED" - no units were ever assigned
        return "NEVER ASSIGNED"

    fireDF["Response_Status"] = fireDF.apply(
        lambda row: get_response_status(
            row["Unit Time Arrived At Scene"],
            row["Unit Time Staged"],
            row["Unit Time Enroute"],
            row["Unit Time Assigned"],
        ),
        axis=1,
    )

    # =================================================================
    #    Fire/EMS Specific Handles
    # =================================================================
    if dataSource == "fire":
        fireDF["FirstArrived_Orig"] = fireDF["FirstArrived"]
        fireDF = addFirstArrived(fireDF)
    else:
        fireDF = reprocessPriority(fireDF)
        fireDF = addFirstArrived(fireDF)

    # =================================================================
    #     Match esri formatting for first arrived
    # =================================================================
    #  if unit arrived first, yes
    #  if arrived at all, but not first, -
    #  else X
    # def replaceAssigned(firstArrived, timeAtScene):
    #     if pd.isnull(timeAtScene):
    #         return "X"
    #     if pd.isnull(firstArrived) or firstArrived == "" or firstArrived == " ":
    #         return "-"
    #     print(firstArrived)
    #     return firstArrived
    def replaceAssigned(firstArrived, timeAtScene, timeFirstArrived):
        if pd.isnull(timeAtScene):
            return "X"
        # if pd.isnull(firstArrived) or firstArrived == "" or firstArrived == " ":
        if timeAtScene == timeFirstArrived:
            return "Yes"
        return "-"

    fireDF["FirstArrivedEsri"] = fireDF.apply(
        lambda x: replaceAssigned(
            x["FirstArrived"],
            x["Unit Time Arrived At Scene"],
            x["Time First Real Unit Arrived"],
        ),
        axis=1,
    )
    fireDF = utils.putColAfter(fireDF, ["FirstArrivedEsri"], "FirstArrived")

    # =================================================================
    #     Fire Data Error Checking
    # =================================================================
    from validateData import checkFile

    fireDF = checkFile(fireDF)

    # # =================================================================
    # #     Add unit type column to simplify analysis
    # # =================================================================
    # fireDF = utils.addUnitType(fireDF)
    # fireDF = utils.addBucketType(fireDF)

    # =================================================================
    #     Calculate Concurrent Use for Each Unit
    # =================================================================
    fireDF = cu.addConcurrentUse(fireDF, "Unit Time Assigned", "Unit Time Call Cleared")
    # =================================================================
    #     Set District 17 Values
    # =================================================================

    from shapely.geometry import Point
    import geopandas as gpd

    # Set up boundaries for ESD17
    ##############################################################
    print("loading esd shape:")
    esd17 = gpd.read_file("Shape\\esd17.shp")
    # specify that source data is 'NAD 1983 StatePlane Texas Central FIPS 4203 (US Feet)' - https://epsg.io/2277
    esd17.set_crs(epsg=2277, inplace=True)
    # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    esd17 = esd17.to_crs(4326)

    # Assign values for esd17
    ##############################################################
    print("assigning ESD17 status:")

    def isESD(jur, lon, lat, inc):
        try:
            if jur not in ["ESD02", "PFLUGERVILLE - ESD TSCO"]:
                # print(lat, lon, "is not in esd17")
                return False
            plot = Point(lon, lat)
            if (esd17.contains(plot)).any():
                # print(lat, lon, "is in esd17")
                return True
            # print(lat, lon, "is not in esd17")
            return False
        except Exception as err:
            print(err)
            faults.append(inc)
            pass

    faults = []
    fireDF["IsESD17"] = np.vectorize(isESD)(
        fireDF["Jurisdiction"],
        fireDF["X-Long"],
        fireDF["Y_Lat"],
        fireDF["Master Incident Number"],
    )
    if len(faults) > 0:
        print(faults, end="\n")
        exit(0)

    # Clear data
    esd17 = None

    # =================================================================
    #     Set District ETJ Values
    # =================================================================

    # Set up boundaries for ETJ
    ##############################################################
    print("loading esd shape:")
    etj = gpd.read_file("Shape\\ETJ.shp")
    # specify that source data is 'NAD 1983 StatePlane Texas Central FIPS 4203 (US Feet)' - https://epsg.io/2277
    etj.set_crs(epsg=2277, inplace=True)
    # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    etj = etj.to_crs(4326)

    # Assign values for etj
    ##############################################################
    print("assigning ETJ status:")

    def isETJ(lon, lat):
        plot = Point(lon, lat)
        if (etj.contains(plot)).any():
            return True
        return False

    fireDF["isETJ"] = np.vectorize(isETJ)(fireDF["X-Long"], fireDF["Y_Lat"])

    # Clear data
    etj = None

    # =================================================================
    #     Set District COP Values
    # =================================================================
    # Set up boundaries for cop
    ##############################################################
    print("loading COP shape:")
    cop = gpd.read_file("Shape\\City_Limits.shp")
    # specify that source data is 'NAD 1983 StatePlane Texas Central FIPS 4203 (US Feet)' - https://epsg.io/2277
    cop.set_crs(epsg=2277, inplace=True)
    # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    cop = cop.to_crs(4326)

    # Assign values for cop
    ##############################################################
    print("assigning cop status:")

    def isCOP(lon, lat):
        plot = Point(lon, lat)
        if (cop.contains(plot)).any():
            return True
        return False

    fireDF["isCOP"] = np.vectorize(isCOP)(fireDF["X-Long"], fireDF["Y_Lat"])

    # Clear data
    cop = None

    # =================================================================
    #     Add Fire Response Areas to EMS Data
    # =================================================================
    print("loading response areas:")
    responseArea = gpd.read_file("Shape\\AFD_Response_Areas.shp")
    # specify that source data is 'NAD 1983 StatePlane Texas Central FIPS 4203 (US Feet)' - https://epsg.io/2277
    responseArea.set_crs(epsg=2277, inplace=True)
    # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    responseArea = responseArea.to_crs(4326)

    def getResponseArea(lon, lat):
        plot = Point(lon, lat)
        try:
            mapInd = (responseArea.index[responseArea.contains(plot)])[0]
            return responseArea.loc[mapInd, "RESPONSE_A"]
        except:
            return None

    if "AFD Response Box" not in fireDF:
        fireDF["AFD Response Box"] = fireDF.apply(
            lambda x: getResponseArea(x["X-Long"], x["Y_Lat"]),
            axis=1,
        )
    # Clear data
    responseArea = None

    # =================================================================
    #     Set pop density values Values
    # =================================================================
    import popden

    fireDF = popden.addPopDen(fireDF)
    # (getMapscoGrid)(fireDF["X-Long"], fireDF["Y_Lat"])

    print("Adding Status:")
    # =================================================================
    #     Set Status for each call
    # =================================================================
    #   'Status'   - 1, 0, x, c

    # 1 - is the earliest arrived of a set of identical 'Master Incident Number'
    # 0 - all other rows in a set of identical 'Master Incident Number' - multi unit Response
    # C - Incident Canceled Prior to unit arrival (no 'Unit Time Arrived At Scene')
    # X - all other rows in a set of identical 'Master Incident Number' with no 'Unit Time Arrived At Scene'

    # 1 - is the earliest arrived of a set of identical 'Master Incident Number'
    # 1 if "firstArrivedEsri" == "Yes"
    # 0 - all other rows in a set of identical 'Master Incident Number' - multi unit Response
    # else "0"
    # C - Incident Canceled Prior to unit arrival (no 'Unit Time Arrived At Scene')
    # pd.isnull("Unit time arrived at scene")
    # ||  "firstArrivedEsri" == "Yes"
    # X - all other rows in a set of identical 'Master Incident Number' with no 'Unit Time Arrived At Scene'

    # Set Canceled, 1(first arrived), or 0(multi incident)
    conditions = [
        (fireDF["Master Incident Number"] != fireDF.shift(1)["Master Incident Number"])
        & (fireDF["Unit Time Arrived At Scene"].isnull()),
        (fireDF["Master Incident Number"] == fireDF.shift(1)["Master Incident Number"]),
    ]
    choices = ["C", "0"]
    fireDF["Status"] = np.select(conditions, choices, default="1")

    # Overwrite 0 with X on canceled calls
    # get array of indexes with 0
    res0 = fireDF.index[fireDF["Status"] == "0"].tolist()

    # this is going to be really slow...
    # this will also break if referencing an incident removed by
    for i in res0:
        fireDF.loc[i, "Status"] = (
            "X" if ((fireDF.loc[i - 1, "Status"] in (["X", "C"]))) else "0"
        )

    # # =================================================================
    # #     Remove COMM units / Information Only
    # # =================================================================
    # fireDF = fireDF[
    #     ~fireDF["Unit Type"].isin(
    #         ["MEDC", "FTAC", "TEST", "ALARMT", "COM", "CNTRL", "CC"]
    #     )
    # ]

    # =================================================================
    #     Recalculate Shift Data
    # =================================================================
    def getShift(assigned, phpu):
        # =CHOOSE(1+MOD(DAYS(BB,DATE(2021,3,30)) + (IF( IF(ISBLANK(BB),HOUR(W),HOUR(BB))<7, 0, 1)), 3), "A Shift","B Shift","C Shift")
        shift = ["A Shift", "B Shift", "C Shift"]

        if pd.isnull(phpu):
            time = assigned
        else:
            time = phpu
        delta = (1 + (time - pd.to_datetime("3/30/21 7:00:00")).days) % 3

        return shift[delta]

    col_bb = "Unit Time Assigned"
    col_w = "Earliest Time Phone Pickup AFD or EMS"

    fireDF["ESD02_Shift"] = fireDF.apply(
        lambda row: getShift(row[col_bb], row[col_w]), axis=1
    )

    # =================================================================
    #     Add a new Stations (Origin) Column
    # =================================================================
    fireDF = getStations(fireDF)

    # =================================================================
    #     Add a new Column for if Unit was at its Station Address when Assigned
    # =================================================================
    fireDF = addLocAtAssignToDF(fireDF)

    # =================================================================
    #     Calculate Station Distances
    # =================================================================
    print(" -- adding road checks")
    fireDF = rd.addRoadDistances(fireDF)

    # =================================================================
    #     add Is Sent From Closest Station
    # =================================================================
    print(" -- adding Closest Station")
    fireDF = addIsClosestStation(fireDF)

    # =================================================================
    # Time delta/interval Colulmn Creation
    # =================================================================
    print(" -- adding Time Deltas")
    incidentCols = {
        "Earliest Time Phone Pickup to In Queue": [
            "Earliest Time Phone Pickup AFD or EMS",
            "Incident Time Call Entered in Queue",
        ],
        "In Queue to 1st Real Unit Assigned": [
            "Incident Time Call Entered in Queue",
            "Time First Real Unit Assigned",
        ],
        "Earliest Time Phone Pickup to 1st Real Unit Assigned": [
            "Earliest Time Phone Pickup AFD or EMS",
            "Time First Real Unit Assigned",
        ],
        "Incident Turnout - 1st Real Unit Assigned to 1st Real Unit Enroute": [
            "Time First Real Unit Assigned",
            "Time First Real Unit Enroute",
        ],
        "Incident Travel Time - 1st Real Unit Enroute to 1st Real Unit Arrived ": [
            "Time First Real Unit Enroute",
            "Time First Real Unit Arrived",
        ],
        "Incident First Unit Response - 1st Real Unit Assigned to 1st Real Unit Arrived": [
            "Time First Real Unit Assigned",
            "Time First Real Unit Arrived",
        ],
        "Earliest Time Phone Pickup to 1st Real Unit Arrived": [
            "Earliest Time Phone Pickup AFD or EMS",
            "Time First Real Unit Arrived",
        ],
        "Time Spent OnScene - 1st Real Unit Arrived to Last Real Unit Call Cleared": [
            "Time First Real Unit Arrived",
            "Last Real Unit Clear Incident",
        ],
        "Incident Duration - Earliest Time Phone Pickup to Last Real Unit Call Cleared": [
            "Earliest Time Phone Pickup AFD or EMS",
            "Last Real Unit Clear Incident",
        ],
    }

    # Create TimeDelta Columns
    for col in incidentCols:
        fireDF = utils.addTimeDiff(
            fireDF, col, incidentCols[col][0], incidentCols[col][1]
        )

    # Move TimeDelta Columns to correct spot in file
    fireDF = utils.putColAfter(
        fireDF,
        list(incidentCols.keys()),
        "Last Real Unit Clear Incident",
    )

    unitCols = {
        "Phone_Pickup_to_Unit_Assigned": [
            "Earliest Time Phone Pickup AFD or EMS",
            "Unit Time Assigned",
        ],
        "In Queue to Unit Dispatch": [
            "Incident Time Call Entered in Queue",
            "Unit Time Assigned",
        ],
        "Unit Dispatch to Respond Time": ["Unit Time Assigned", "Unit Time Enroute"],
        "Unit Respond to Arrival": ["Unit Time Enroute", "Unit Time Arrived At Scene"],
        "Unit Dispatch to Onscene": [
            "Unit Time Assigned",
            "Unit Time Arrived At Scene",
        ],
        "Unit OnScene to Clear Call": [
            "Unit Time Arrived At Scene",
            "Unit Time Call Cleared",
        ],
        "Earliest Phone Pickup Time to Unit Arrival": [
            "Earliest Time Phone Pickup AFD or EMS",
            "Unit Time Arrived At Scene",
        ],
        "Unit Assign To Clear Call Time": [
            "Unit Time Assigned",
            "Unit Time Call Cleared",
        ],
    }

    # Add some EMS Specific columns
    if dataSource == "ems":
        unitCols["Depart_to_At_Destination"] = [
            "Time_Depart_Scene",
            "Time_At_Destination",
        ]
        unitCols["Depart_to_Cleared_Destination"] = [
            "Time_Depart_Scene",
            "Time_Cleared_Destination",
        ]

    # Create TimeDelta Columns
    for col in unitCols:
        fireDF = utils.addTimeDiff(fireDF, col, unitCols[col][0], unitCols[col][1])

    # Move TimeDelta Columns to correct spot in file
    fireDF = utils.putColAfter(
        fireDF,
        list(unitCols.keys()),
        "Unit Time Call Cleared",
    )

    # =================================================================
    # Correction of time: staging calls
    # =================================================================

    # Create columns assuming no... this to be changed only on those that are needed.
    fireDF["INC_Staged_As_Arrived"] = 0
    fireDF["UNIT_Staged_As_Arrived"] = 0

    def getSingleTimeDiff(df, row, t1, t2, reverse):
        if not reverse:
            res = df.loc[row, t2] - df.loc[row, t1]
        else:
            res = df.loc[row, t1] - df.loc[row, t2]
        # Convert to seconds
        return res / np.timedelta64(1, "s")

    #   ----------------
    #           Incident Recalculations
    #   ----------------

    # columns to be recalculated using U(staging time) instead of V(arrived time) (if needed)
    recalcIncidentCols = {
        #    Column name:
        #       [  column to get diff from 'arrived time'  ,   reverse order  ]
        "Incident First Unit Response - 1st Real Unit Assigned to 1st Real Unit Arrived": [
            "Time First Real Unit Assigned",
            False,
        ],
        "Incident Travel Time - 1st Real Unit Enroute to 1st Real Unit Arrived ": [
            "Time First Real Unit Enroute",
            False,
        ],
        "Earliest Time Phone Pickup to 1st Real Unit Arrived": [
            "Earliest Time Phone Pickup AFD or EMS",
            False,
        ],
        "Time Spent OnScene - 1st Real Unit Arrived to Last Real Unit Call Cleared": [
            "Last Real Unit Clear Incident",
            True,
        ],
    }
    # get a list of units that needs to be recalculated  staged = arrived_on_scene if ((first_real_unit was staged prior to arrival) & (real unit was not staged prior to Enroute))
    t = "Time First Real Unit Enroute"
    u = "Incident Time First Staged"
    v = "Time First Real Unit Arrived"

    # --- get a list of columns that need to be recalculated
    recalc = fireDF[
        (
            (~fireDF[v].isnull())
            & (fireDF[v] != "-")
            & (~fireDF[u].isnull())
            & (fireDF[u] != "-")
            & (~fireDF[t].isnull())
            & (fireDF[t] != "-")
        )
    ]
    if not recalc.empty:
        recalc2 = recalc[
            (
                (pd.to_datetime(recalc[u]) < pd.to_datetime(recalc[v]))
                & (pd.to_datetime(recalc[u]) > pd.to_datetime(recalc[t]))
            )
        ]
        recalcArray = recalc2.index.tolist()

        for i in recalcArray:
            fireDF.loc[i, "INC_Staged_As_Arrived"] = 1
            for col in recalcIncidentCols:
                fireDF.loc[i, col] = getSingleTimeDiff(
                    fireDF, i, recalcIncidentCols[col][0], u, recalcIncidentCols[col][1]
                )

    # for col in recalcIncidentCols:
    #     fireDF = utils.putColAfter(fireDF, [col + "recalc"], col)

    #   ----------------
    #           Unit Recalculations
    #   ----------------

    # now using U(staging time) instead of V(arrived time), recaluclate the following columns
    #    Column name:
    #       [  column to get diff from 'arrived time'  ,   reverse order  ]
    recalcUnitCols = {
        "Unit Respond to Arrival": ["Unit Time Enroute", False],
        "Unit Dispatch to Onscene": ["Unit Time Assigned", False],
        "Unit OnScene to Clear Call": ["Unit Time Call Cleared", True],
        "Earliest Phone Pickup Time to Unit Arrival": [
            "Earliest Time Phone Pickup AFD or EMS",
            False,
        ],
    }

    t = "Unit Time Enroute"
    u = "Unit Time Staged"
    v = "Unit Time Arrived At Scene"

    # --- get a list of columns that need to be recalculated
    recalc = fireDF[
        (
            (~fireDF[v].isnull())
            & (fireDF[v] != "-")
            & (~fireDF[u].isnull())
            & (fireDF[u] != "-")
            & (~fireDF[t].isnull())
            & (fireDF[t] != "-")
        )
    ]
    if not recalc.empty:
        recalc2 = recalc[((recalc[u] < recalc[v]) & (recalc[u] > recalc[t]))]
        recalcArray = recalc2.index.tolist()

        for i in recalcArray:
            fireDF.loc[i, "UNIT_Staged_As_Arrived"] = 1
            for col in recalcUnitCols:
                fireDF.loc[i, col] = getSingleTimeDiff(
                    fireDF, i, recalcUnitCols[col][0], u, recalcUnitCols[col][1]
                )

    # =================================================================
    #   Extra Time Formatted Columns
    # =================================================================

    # format times to [HH]:mm:ss
    fireDF = tb.addFormattedTimes(fireDF)

    # add Month / Year / Qtr Year
    fireDF = tb.addMothData(fireDF)

    # add call time classifications ('Calls > 20 Min - PU to Arrive' , 'Ph_PU2_UnitArrive Time_Intervals in seconds')
    fireDF = tb.addPhPuSteps(fireDF)

    # add call count / single / multi cols
    fireDF = tb.addCallCount(fireDF)
    fireDF = tb.addSingleVSMulti(fireDF)

    # move Qtr Year to end to match marys data
    fireDF = utils.putColAt(fireDF, ["Qtr Year"], 200)

    # =================================================================
    #     get Complete Response Force for each Structure Fire
    # =================================================================
    # check transport not reflecting onscene status

    if dataSource == "ems":
        transportCheck = fireDF[
            pd.isnull(fireDF["Unit Time Arrived At Scene"]) & fireDF["Transport_Count"]
            > 0
        ].index.tolist()

        for i in transportCheck:
            # Status
            if (fireDF.loc[i, "Incident Call Count"] == 1) and fireDF.loc[
                i, "Jurisdiction"
            ] in ourNames:
                fireDF.loc[i, "Status"] = "1"
            else:
                fireDF.loc[i, "Status"] = "0"

            # Response Status
            fireDF.loc[i, "Response_Status"] = "ONSC"

            # First Arrived Esri
            if (fireDF.loc[i, "Incident Call Count"] == 1) and fireDF.loc[
                i, "Jurisdiction"
            ] in ourNames:
                fireDF.loc[i, "FirstArrivedEsri"] = "1"
            else:
                fireDF.loc[i, "FirstArrivedEsri"] = "-"
            # fireDF.loc[i, "UNIT_Staged_As_Arrived"] = 1
            # for col in recalcUnitCols:
            #     fireDF.loc[i, col] = getSingleTimeDiff(
            #         fireDF, i, recalcUnitCols[col][0], u, recalcUnitCols[col][1]
            #     )
    # =================================================================
    #     get Complete Response Force for each Structure Fire
    # =================================================================
    crfdf = getCRF(fireDF)

    # fireDF.join(crfdf.set_index("incident"), on="Master Incident Number")
    try:
        fireDF = pd.merge(fireDF, crfdf, how="left", on=["Master Incident Number"])
    except:
        fireDF["Incident_ERF_Time"] = None
        fireDF["Force_At_ERF_Time_of_Close"] = None
        print("No ERF Found")

    # =================================================================
    # finalize naming
    # =================================================================
    fireDF = n.rename(fireDF)

    # =================================================================
    #     Column Organization
    # =================================================================

    fireDF = utils.putColAfter(
        fireDF,
        ["Incident_Call_Count"],
        "Force_At_ERF_Time_of_Close",
    )
    fireDF = utils.putColAfter(fireDF, ["Response_Status"], "Status")

    # ----------------
    # Exporting and completion
    # ----------------
    # print to files

    # using builtin function vs using ExcelWriter class
    # fireDF.to_excel("Output{0}.xlsx".format((datetime.datetime.now()).strftime("%H-%M-%S")))

    # writer = pd.ExcelWriter(
    #     "Output\\Output_{0}.xlsx".format(
    #         (datetime.datetime.now()).strftime("%y-%m-%d_%H-%M")
    #     ),
    #     engine="xlsxwriter",
    #     datetime_format="mm/dd/yyyy hh:mm:ss",
    #     date_format="mm/dd/yyyy",
    # )

    # fireDF.to_excel(writer)
    # writer.save()
    # plt.savefig('saved_figure.png')

    # ----------------
    # Write to Database
    # ----------------
    # show(fireDF)
    from Database import SQLDatabase

    db = SQLDatabase()
    db.insertDF(fireDF)

    # ----------------
    # Write to Esri Directly
    # ----------------
    # from esriOverwrite import EsriDatabase

    # esriDF = EsriDatabase.formatDFForEsri(fireDF)
    # edb = EsriDatabase()
    # edb.connect()
    # edb.appendDF(fireDF)

    # show(esriDF)

    ######################################
    # show in gui just after writing
    print("Complete")
    # show(fireDF)


################################
# ==================================================================
#
# Testing Code: will only run when this file is called directly.
#
# ==================================================================
################################
if __name__ == "__main__":
    # load test files
    import loadTestFile

    df = loadTestFile.get()
    # run test file
    analyzeFire(df)
