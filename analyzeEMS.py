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
import naming as n

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


def getStations(emsDF):
    # Add a new Stations Column
    # -------------------------------------------------------------------------------------------
    # look up radio name and department
    # S01, S02, S03, S04, S05, ADMIN, AFD, OTHER

    # pulled from ems data xlsx
    # =IF(AND(AZ1="ESD02 - Pflugerville",BA1="Frontline"),RIGHT(ems_Table[@[Radio Name]],3),"ADMIN ESD02")

    az = "Department"
    ba = "Frontline_Status"

    conditions = [
        # fmt: off
        (emsDF["Frontline_Status"] == "Not a unit"),  # 0
        (emsDF[az] == "AFD") & (emsDF[ba] == "Other"),  # 1
        (emsDF[az] == "AFD"),
        (emsDF[az] == "ESD12 - Manor") & (emsDF[ba] == "Other"),
        (emsDF[az] == "ESD12 - Manor"),
        (emsDF[az] == "ESD02 - Pflugerville") & (emsDF[ba].isin(["Other", "Command"])),
        (emsDF[az] == "ESD02 - Pflugerville") & (emsDF["Radio_Name"] == "QNT261"),  # 6
        (emsDF[az] == "ESD02 - Pflugerville") & (emsDF["Radio_Name"].str.contains("BAT20")),
            # account for instances where vehicle is filling in for another.  We will need to deterimine which station it is filling in for based on the location at time of assignment
        (emsDF[az] == "ESD02 - Pflugerville") & (emsDF["Radio_Name"].isin(reserveUnits)),
        (emsDF[az] == "ESD02 - Pflugerville"),
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
        "S0" + emsDF["Radio_Name"].str[-2],
    ]

    emsDF["Station"] = np.select(conditions, choices, default=emsDF["Department"])

    # correct reserved units, as I have spent far too long trying to do this all as one part
    #    ----------------------------
    rescor = emsDF.index[emsDF["Station"] == "Reserve Unit"].tolist()
    # again, this is going to be very slow compared to other vectorized checks/changes
    for i in rescor:
        # get location as a string
        curLoc = str(emsDF.loc[i, "Location_At_Assign_Time"]).lower()
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
        emsDF.loc[i, "Station"] = stationNum

    # move Status col to front
    emsDF = utils.putColAt(emsDF, ["Station", "Status"], 0)

    return emsDF


# ##############################################################################################################################################
#     Main Code
# ##############################################################################################################################################


def analyzeems(emsDF):
    """
    Extrapolates information from a specifically formatted XLS file, returning a larger set presumed, formatted, and corracted data

    Parameters
    --------------------------------
    df : Dataframe
        An excel file of ems Call information as received

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

    emsDF["FirstArrivedEsri"] = emsDF.apply(
        lambda x: replaceAssigned(
            x["FirstArrived"],
            x["Unit Time Arrived At Scene"],
            x["Time First Real Unit Arrived"],
        ),
        axis=1,
    )
    emsDF = utils.putColAfter(emsDF, ["FirstArrivedEsri"], "FirstArrived")

    # =================================================================
    #     ems Data Error Checking
    # =================================================================
    from GUI.validateData import checkFile

    emsDF = checkFile(emsDF)

    # =================================================================
    #     Add unit type column to simplify analysis
    # =================================================================
    emsDF = utils.addUnitType(emsDF)
    emsDF = utils.addBucketType(emsDF)

    # =================================================================
    #     Calculate Concurrent Use for Each Unit
    # =================================================================
    emsDF = cu.addConcurrentUse(emsDF, "Unit Time Assigned", "Unit Time Call Cleared")

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

    emsDF["IsESD17"] = np.vectorize(isESD)(
        emsDF["Jurisdiction"], emsDF["X-Long"], emsDF["Y_Lat"]
    )

    # Clear data
    esd17 = None

    # =================================================================
    #     Set District ETJ Values
    # =================================================================

    from shapely.geometry import Point
    import geopandas as gpd

    # Set up boundaries for ESD17
    ##############################################################
    print("loading esd shape:")
    etj = gpd.read_file("Shape\\notETJ.shp")
    # specify that source data is 'NAD 1983 StatePlane Texas Central FIPS 4203 (US Feet)' - https://epsg.io/2277
    etj.set_crs(epsg=2277, inplace=True)
    # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    etj = etj.to_crs(4326)

    # Assign values for etj
    ##############################################################
    print("assigning ETJ status:")

    def isETJ(jur, lon, lat):
        if jur != "ESD02":
            # print(lat, lon, "is not in etj")
            return False
        plot = Point(lon, lat)
        if (etj.contains(plot)).any():
            # print(lat, lon, "is in etj")
            return False
        # print(lat, lon, "is not in etj")
        return True

    emsDF["isETJ"] = np.vectorize(isETJ)(
        emsDF["Jurisdiction"], emsDF["X-Long"], emsDF["Y_Lat"]
    )

    # Clear data
    etj = None

    # =================================================================
    #     Set District ETJ Values
    # =================================================================
    emsDF["isCOP"] = emsDF.apply(
        lambda row: (row["isETJ"] + row["IsESD17"]) == 0, axis=1
    )

    # =================================================================
    #     Set pop density values Values
    # =================================================================
    import popden

    emsDF = popden.addPopDen(emsDF)
    # (getMapscoGrid)(emsDF["X-Long"], emsDF["Y_Lat"])

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
        (emsDF["Master Incident Number"] != emsDF.shift(1)["Master Incident Number"])
        & (emsDF["Unit Time Arrived At Scene"].isnull()),
        (emsDF["Master Incident Number"] == emsDF.shift(1)["Master Incident Number"]),
    ]
    choices = ["C", "0"]
    emsDF["Status"] = np.select(conditions, choices, default="1")

    # Overwrite 0 with X on canceled calls
    # get array of indexes with 0
    res0 = emsDF.index[emsDF["Status"] == "0"].tolist()
    # this is going to be really slow...
    for i in res0:
        emsDF.loc[i, "Status"] = (
            "X" if ((emsDF.loc[i - 1, "Status"] in (["X", "C"]))) else "0"
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

    emsDF["ESD02_Shift"] = emsDF.apply(
        lambda row: getShift(row[col_bb], row[col_w]), axis=1
    )

    # =================================================================
    #     Add a new Stations (Origin) Column
    # =================================================================
    emsDF = getStations(emsDF)

    # =================================================================
    #     Add a new Column for if Unit was at its Station Address when Assigned
    # =================================================================
    emsDF = addLocAtAssignToDF(emsDF)

    # =================================================================
    #     Calculate Station Distances
    # =================================================================
    emsDF = rd.addRoadDistances(emsDF)

    # =================================================================
    #     add Is Sent From Closest Station
    # =================================================================
    # TODO: add Is Sent From Closest Station
    emsDF = addIsClosestStation(emsDF)

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
        emsDF = utils.addTimeDiff(
            emsDF, col, incidentCols[col][0], incidentCols[col][1]
        )

    # Move TimeDelta Columns to correct spot in file
    emsDF = utils.putColAfter(
        emsDF,
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

    # Create TimeDelta Columns
    for col in unitCols:
        emsDF = utils.addTimeDiff(emsDF, col, unitCols[col][0], unitCols[col][1])

    # Move TimeDelta Columns to correct spot in file
    emsDF = utils.putColAfter(
        emsDF,
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
    recalc = emsDF[
        (
            (~emsDF[v].isnull())
            & (emsDF[v] != "-")
            & (~emsDF[u].isnull())
            & (emsDF[u] != "-")
            & (~emsDF[t].isnull())
            & (emsDF[t] != "-")
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
            emsDF.loc[i, col] = getSingleTimeDiff(
                emsDF, i, recalcIncidentCols[col][0], u, recalcIncidentCols[col][1]
            )

    # for col in recalcIncidentCols:
    #     emsDF = utils.putColAfter(emsDF, [col + "recalc"], col)

    #   ----------------
    #           Unit Recalculations
    #   ----------------

    t = "Unit Time Enroute"
    u = "Unit Time Staged"
    v = "Unit Time Arrived At Scene"

    # --- get a list of columns that need to be recalculated
    recalc = emsDF[
        (
            (~emsDF[v].isnull())
            & (emsDF[v] != "-")
            & (~emsDF[u].isnull())
            & (emsDF[u] != "-")
            & (~emsDF[t].isnull())
            & (emsDF[t] != "-")
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
            emsDF.loc[i, col] = getSingleTimeDiff(
                emsDF, i, recalcUnitCols[col][0], u, recalcUnitCols[col][1]
            )

    # =================================================================
    #   Extra Formatting
    # =================================================================

    # format times to [HH]:mm:ss
    emsDF = tb.addFormattedTimes(emsDF)

    # add Month / Year / Qtr Year
    emsDF = tb.addMothData(emsDF)

    # add call time classifications ('Calls > 20 Min - PU to Arrive' , 'Ph_PU2_UnitArrive Time_Intervals in seconds')
    emsDF = tb.addPhPuSteps(emsDF)

    # add call count / single / multi cols
    emsDF = tb.addCallCount(emsDF)
    emsDF = tb.addSingleVSMulti(emsDF)

    # move Qtr Year to end to match marys data
    emsDF = utils.putColAt(emsDF, ["Qtr Year"], 200)

    # =================================================================
    #     get Complete Response Force for each Structure ems
    # =================================================================
    crfdf = getCRF(emsDF)
    # show(crfdf)

    # emsDF.join(crfdf.set_index("incident"), on="Master Incident Number")
    emsDF = pd.merge(emsDF, crfdf, how="left", on=["Master Incident Number"])

    # =================================================================
    #     Column Organization
    # =================================================================

    emsDF = utils.putColAfter(
        emsDF,
        ["Unit OnScene to Clear Call"],
        "Earliest Phone Pickup Time to Unit Arrival",
    )
    emsDF = utils.putColAfter(
        emsDF,
        ["Unit OnScene to Clear Call Formatted"],
        "Earliest Phone Pickup Time to Unit Arrival Formatted",
    )

    # ----------------
    # finalize naming
    # ----------------
    emsDF = n.rename(emsDF)

    # ----------------
    # Exporting and completion
    # ----------------
    # print to files

    # using builtin function vs using ExcelWriter class
    # emsDF.to_excel("Output{0}.xlsx".format((datetime.datetime.now()).strftime("%H-%M-%S")))

    # convert specific rows to format {h}:mm:ss format

    # Incident 1st Enroute to 1stArrived Time
    # Incident Duration - Ph PU to Clear
    # Unit  Ph PU to UnitArrived
    # emsDF[""] = emsDF[""].apply(utils.dtFormat)

    writer = pd.ExcelWriter(
        "Output\\Output_{0}.xlsx".format(
            (datetime.datetime.now()).strftime("%y-%m-%d_%H-%M")
        ),
        engine="xlsxwriter",
        datetime_format="mm/dd/yyyy hh:mm:ss",
        date_format="mm/dd/yyyy",
    )

    emsDF.to_excel(writer)
    writer.save()
    # plt.savefig('saved_figure.png')

    # ----------------
    # Write to Database
    # ----------------

    # show(emsDF)
    from Database import SQLDatabase

    db = SQLDatabase()
    # db.insertDF(emsDF)

    ######################################
    # show in gui just after writing
    show(emsDF)


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
    analyzeems(df)
