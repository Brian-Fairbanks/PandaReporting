# Built in
import datetime
from os import path
from os import makedirs

# Dependancies
# from pandasgui import show
import pandas as pd
import numpy as np

# Geodata Dependancies
from shapely.geometry import Point
import geopandas as gpd

# Sibling Modules
import crf
import utils
import ConcurrentUse as cu

# import roads as rd
import getData as data
import timeBreakdowns as tb
import naming as n

# Dont warn me about potentially assigning a copy
pd.options.mode.chained_assignment = None

# import global additional/updatable json data
locations = data.getLocations()
reserveUnits = data.getReserves()
stationDict = data.getStations()

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
    curLoc = str(address).lower()
    stationNum = None
    if "fs020" in curLoc:
        stationNum = "S" + curLoc[-2:]
    # else check it against known street names (specified at the top of the file)
    else:
        for street in locations.keys():
            if street.lower() in curLoc:
                stationNum = locations[street]
                break
    return stationNum


def getLocAtAssign(station, address):
    return station == getLoc(address)


def addLocAtAssignToDF(df):

    df["Assigned at Station"] = df.apply(
        lambda row: getLocAtAssign(row["Station"], row["Location_At_Assign_Time"]),
        axis=1,
    )
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
        (fireDF[az] == "ESD02 - Pflugerville") & (fireDF[ba].isin(["Other", "Command"])),
        (fireDF[az] == "ESD02 - Pflugerville") & (fireDF["Radio_Name"] == "QNT261"),  # 6
        (fireDF[az] == "ESD02 - Pflugerville") & (fireDF["Radio_Name"].str.contains("BAT20")),
            # account for instances where vehicle is filling in for another.  We will need to deterimine which station it is filling in for based on the location at time of assignment
        (fireDF[az] == "ESD02 - Pflugerville") & (fireDF["Radio_Name"].isin(reserveUnits)),
        (fireDF[az] == "ESD02 - Pflugerville"),
        # fmt: on
    ]
    choices = [
        "Not a unit",  # 0
        "AFD Other",
        "AFD",
        "ESD12 - Manor Other",
        "ESD12 - Manor",
        "ADMIN",
        "S05",  # 6
        "S01",
        # mark instances of reserved units, so we can run an extra filter on these in a moment
        "Reserve Unit",
        "S0" + fireDF["Radio_Name"].str[-2],
    ]

    fireDF["Station"] = np.select(conditions, choices, default=fireDF["Department"])

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
    from GUI.validateData import checkFile

    fireDF = checkFile(fireDF)

    # =================================================================
    #     Add unit type column to simplify analysis
    # =================================================================
    fireDF = utils.addUnitType(fireDF)
    fireDF = utils.addBucketType(fireDF)

    # =================================================================
    #     Calculate Concurrent Use for Each Unit
    # =================================================================
    fireDF = cu.addConcurrentUse(fireDF, "Unit Time Assigned", "Unit Time Call Cleared")

    # =================================================================
    #     Set District 17 Values
    # =================================================================

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

    def isESD(jur, lon, lat):
        if jur != "ESD02":
            # print(lat, lon, "is not in esd17")
            return False
        plot = Point(lon, lat)
        if (esd17.contains(plot)).any():
            # print(lat, lon, "is in esd17")
            return True
        # print(lat, lon, "is not in esd17")
        return False

    fireDF["IsESD17"] = np.vectorize(isESD)(
        fireDF["Jurisdiction"], fireDF["X-Long"], fireDF["Y_Lat"]
    )

    # Clear data
    esd17 = None

    # # =================================================================
    # #     Set District ETJ Values
    # # =================================================================

    # from shapely.geometry import Point
    # import geopandas as gpd

    # # Set up boundaries for ESD17
    # ##############################################################
    # print("loading esd shape:")
    # etj = gpd.read_file("Shape\\notETJ.shp")
    # # specify that source data is 'NAD 1983 StatePlane Texas Central FIPS 4203 (US Feet)' - https://epsg.io/2277
    # etj.set_crs(epsg=2277, inplace=True)
    # # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    # etj = etj.to_crs(4326)

    # # Assign values for etj
    # ##############################################################
    # print("assigning ETJ status:")

    # def isETJ(jur, lon, lat):
    #     if jur != "ESD02":
    #         # print(lat, lon, "is not in etj")
    #         return False
    #     plot = Point(lon, lat)
    #     if (etj.contains(plot)).any():
    #         # print(lat, lon, "is in etj")
    #         return False
    #     # print(lat, lon, "is not in etj")
    #     return True

    # fireDF["isETJ"] = np.vectorize(isETJ)(
    #     fireDF["Jurisdiction"], fireDF["X-Long"], fireDF["Y_Lat"]
    # )

    # # Clear data
    # etj = None

    # # =================================================================
    # #     Set District ETJ Values
    # # =================================================================
    # fireDF["isCOP"] = fireDF.apply(
    #     lambda row: (row["isETJ"] + row["IsESD17"]) == 0, axis=1
    # )

    # =================================================================
    #     Set pop density values Values
    # =================================================================
    # import popden

    # fireDF = popden.addPopDen(fireDF)

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
    for i in res0:
        fireDF.loc[i, "Status"] = (
            "X" if ((fireDF.loc[i - 1, "Status"] in (["X", "C"]))) else "0"
        )

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
    # fireDF = rd.addRoadDistances(fireDF)

    # =================================================================
    #     add Is Sent From Closest Station
    # =================================================================
    # fireDF = addIsClosestStation(fireDF)

    # =================================================================
    # Time Data Colulmn Creation
    # =================================================================

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
        "Earliest Phone Pickup Time to Unit Arrival": [
            "Earliest Time Phone Pickup AFD or EMS",
            "Unit Time Arrived At Scene",
        ],
        "Unit Assign To Clear Call Time": [
            "Unit Time Assigned",
            "Unit Time Call Cleared",
        ],
        "Unit OnScene to Clear Call": [
            "Unit Time Arrived At Scene",
            "Unit Time Call Cleared",
        ],
    }

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
    recalc2 = recalc[((recalc[u] < recalc[v]) & (recalc[u] > recalc[t]))]
    recalcArray = recalc2.index.tolist()

    # now using U(staging time) instead of V(arrived time), recaluclate the following columns
    recalcIncidentCols = {
        #    Column name:
        #       [  column to get diff from 'arrived time'  ,   reverse order  ]
        "Incident First Unit Response - 1st Real Unit Assigned to 1st Real Unit Arrived": [
            "Time First Real Unit Assigned",
            False,
        ],
        "Earliest Time Phone Pickup to 1st Real Unit Arrived": [
            "Earliest Time Phone Pickup AFD or EMS",
            False,
        ],
        "Incident Travel Time - 1st Real Unit Enroute to 1st Real Unit Arrived ": [
            "Time First Real Unit Enroute",
            False,
        ],
        "Time Spent OnScene - 1st Real Unit Arrived to Last Real Unit Call Cleared": [
            "Last Real Unit Clear Incident",
            True,
        ],
    }

    for i in recalcArray:
        for col in recalcIncidentCols:
            fireDF.loc[i, col] = getSingleTimeDiff(
                fireDF, i, recalcIncidentCols[col][0], u, recalcIncidentCols[col][1]
            )

    # for col in recalcIncidentCols:
    #     fireDF = utils.putColAfter(fireDF, [col + "recalc"], col)

    #   ----------------
    #           Unit Recalculations
    #   ----------------

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
    recalc2 = recalc[((recalc[u] < recalc[v]) & (recalc[u] > recalc[t]))]
    recalcArray = recalc2.index.tolist()

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

    for i in recalcArray:
        for col in recalcUnitCols:
            fireDF.loc[i, col] = getSingleTimeDiff(
                fireDF, i, recalcUnitCols[col][0], u, recalcUnitCols[col][1]
            )

    # =================================================================
    #   Extra Formatting
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
    crfdf = crf.getCRF(fireDF)
    # show(crfdf)

    # fireDF.join(crfdf.set_index("incident"), on="Master Incident Number")
    try:
        fireDF = pd.merge(fireDF, crfdf, how="left", on=["Master Incident Number"])
    except:
        print("No CRF Found")
    # =================================================================
    #     Column Organization
    # =================================================================

    # fireDF = utils.putColAfter(
    #     fireDF,
    #     ["Unit OnScene to Clear Call"],
    #     "Earliest Phone Pickup Time to Unit Arrival",
    # )
    # fireDF = utils.putColAfter(
    #     fireDF,
    #     ["Unit OnScene to Clear Call Formatted"],
    #     "Earliest Phone Pickup Time to Unit Arrival Formatted",
    # )

    # ----------------
    # finalize naming
    # ----------------
    fireDF = n.rename(fireDF)

    # ----------------
    # Exporting and completion
    # ----------------
    # print to files

    # using builtin function vs using ExcelWriter class
    # fireDF.to_excel("Output{0}.xlsx".format((datetime.datetime.now()).strftime("%H-%M-%S")))

    # Create a Pandas Excel writer using XlsxWriter as the engine.
    # Also set the default datetime and date formats.

    from os import path
    from os import makedirs

    fp = "./testfolder"
    if not (path.exists(fp)):
        makedirs(fp)

    time = datetime.datetime.now().strftime("%y-%m-%d_%H-%M")
    outputFileName = f"{fp}\\Output_{time}.xlsx"

    with pd.ExcelWriter(
        outputFileName,
        engine="xlsxwriter",
        datetime_format="mm/dd/yyyy hh:mm:ss",
        date_format="mm/dd/yyyy",
    ) as writer:
        fireDF.to_excel(writer)
        writer.save()

    input(f"File has been exported to {fp} .  Press enter to exit.")

    # ----------------
    # Write to Database
    # ----------------

    # show(fireDF)
    # from Database import SQLDatabase

    # db = SQLDatabase()
    # db.insertDF(fireDF)

    ######################################
    # show in gui just after writing
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
