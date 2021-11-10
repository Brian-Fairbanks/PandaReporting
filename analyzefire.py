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
    df["Is Closest Station"] = df.apply(
        lambda row: row["Station"] == row["Closest Station"], axis=1
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
        (fireDF[az] == "AFD") & (fireDF[ba] == "Other"),  # 1
        (fireDF[az] == "AFD"),
        (fireDF[az] == "ESD12 - Manor") & (fireDF[ba] == "Other"),
        (fireDF[az] == "ESD12 - Manor"),
        (fireDF[az] == "ESD02 - Pflugerville")
        & (fireDF[ba].isin(["Other", "Command"])),
        (fireDF[az] == "ESD02 - Pflugerville")
        & (fireDF["Radio_Name"] == "QNT261"),  # 6
        # fmt: off
        (fireDF[az] == "ESD02 - Pflugerville") & (fireDF["Radio_Name"].str.contains("BAT20")),
        # fmt: on
        # account for instances where vehicle is filling in for another.  We will need to deterimine which station it is filling in for based on the location at time of assignment
        (fireDF[az] == "ESD02 - Pflugerville")
        & (fireDF["Radio_Name"].isin(reserveUnits)),
        (fireDF[az] == "ESD02 - Pflugerville"),
    ]
    choices = [
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

    fireDF["Station"] = np.select(conditions, choices, default=fireDF["Radio_Name"])

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
            stationNum = "FS" + curLoc[-2:]
        # else check it against known street names (specified at the top of the file)
        else:
            for street in locations.keys():
                if street.lower() in curLoc:
                    stationNum = "F" + locations[street]
                    break
        # and then set the finalized location
        fireDF.loc[i, "Station"] = stationNum

    # move Status col to front
    fireDF = utils.putColAt(fireDF, ["Station", "Status"], 0)
    fireDF = utils.putColAt(fireDF, ["Master Incident Without First Two Digits"], 100)

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
    #     Basic Format
    # =================================================================
    # replace all instances of "-" with null values
    fireDF = fireDF.replace("-", np.nan)

    # =================================================================
    #     Fire Data Error Checking
    # =================================================================
    from GUI.validateData import checkFile

    fireDF = checkFile(fireDF)

    # =================================================================
    #     order fire data by time
    # =================================================================
    # - 'Master Incident Number' > 'Unit Time Arrived At Scene' > 'Unit Time Staged' > 'Unit Time Enroute' > 'Unit Time Assigned'
    fireDF = fireDF.sort_values(
        by=[
            "Master Incident Number",
            "Unit Time Arrived At Scene",
            "Unit Time Staged",
            "Unit Time Enroute",
            "Unit Time Assigned",
        ]
    )
    fireDF = fireDF.reset_index(drop=True)

    # =================================================================
    #     get Complete Response Force for each Structure Fire
    # =================================================================
    # crfDF = getCRF(fireDF)
    # utils.pprint(crfDF)

    # =================================================================
    #     Add unit type column to simplify analysis
    # =================================================================
    fireDF = utils.addUnitType(fireDF)

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

    esd17 = gpd.read_file("Shape\\esd17.shp")
    # specify that source data is 'NAD 1983 StatePlane Texas Central FIPS 4203 (US Feet)' - https://epsg.io/2277
    esd17.set_crs(epsg=2277, inplace=True)
    # and convert to 'World Geodetic System 1984' (used in GPS) - https://epsg.io/4326
    esd17 = esd17.to_crs(4326)

    # Set assign values for esd17
    ##############################################################

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

    # After the checks, add 2 extra columns -
    #   'Sequence' -

    #   'Status'   - 1, 0, x, c
    # 1 - is the earliest arrived of a set of identical 'Master Incident Number'
    # 0 - all other rows in a set of identical 'Master Incident Number' - multi unit Response
    # C - Incident Canceled Prior to unit arrival (no 'Unit Time Arrived At Scene')
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
    #     Add a new Stations Column
    # =================================================================
    fireDF = getStations(fireDF)

    # =================================================================
    #     Add a new Column for if Unit was at its Station Address when Assigned
    # =================================================================
    fireDF = addLocAtAssignToDF(fireDF)

    # =================================================================
    #     Calculate Station Distances
    # =================================================================
    fireDF = rd.addRoadDistances(fireDF)

    # =================================================================
    #     add Is Sent From Closest Station
    # =================================================================
    # TODO: add Is Sent From Closest Station
    fireDF = addIsClosestStation(fireDF)

    # =================================================================
    # Time Data Extra Colulmn Creation
    # =================================================================

    nc1 = "Incident 1st Enroute to 1stArrived Time"
    nc2 = "Incident Duration - Ph PU to Clear"
    nc3 = "Unit Ph PU to UnitArrived"

    fireDF = utils.addTimeDiff(
        fireDF, nc1, "Time First Real Unit Arrived", "Time First Real Unit Enroute"
    )
    fireDF = utils.addTimeDiff(
        fireDF,
        nc2,
        "Last Real Unit Clear Incident",
        "Earliest Time Phone Pickup AFD or EMS",
    )
    fireDF = utils.addTimeDiff(
        fireDF,
        nc3,
        "Time First Real Unit Arrived",
        "Earliest Time Phone Pickup AFD or EMS",
    )

    #   right after "Incident Turnout - 1st Real Unit Assigned to 1st Real Unit Enroute", AD
    fireDF = utils.putColAt(fireDF, [nc1], 29)  # 29
    #   right after "Incident First Unitresponse - 1st Real Unit assigned to 1st Real Unit Arrived", AD
    fireDF = utils.putColAt(fireDF, [nc2], 31)
    #   right after "Unit Assign to Clear Call", AD
    fireDF = utils.putColAt(fireDF, [nc3], 33)

    # ----------------
    # Correction of time: staging calls
    # ----------------

    # get a list of units that needs to be recalculated  staged = arrived_on_scene if ((first_real_unit was staged prior to arrival) & (real unit was not staged prior to Enroute))
    t = "Time First Real Unit Enroute"
    u = "Incident Time First Staged"
    v = "Time First Real Unit Arrived"

    recalc = fireDF[
        (
            (~fireDF[v].isnull())
            & (fireDF[v] != "-")
            & (~fireDF[u].isnull())
            & (fireDF[u] != "-")
            & (~fireDF[t].isnull())
            & (fireDF[t] != "-")
        )
        # & ((fireDF[u] < fireDF[v]) & (fireDF[u] > fireDF[t]))
    ]
    recalc2 = recalc[((recalc[u] < recalc[v]) & (recalc[u] > recalc[t]))]

    recalcArray = recalc2.index.tolist()

    def getSingleTimeDiff(df, row, t1, t2):
        res = df.loc[row, t2] - df.loc[row, t1]
        res = utils.dtformat(res)
        return res

    # now using U instead of V, recaluclate the following columns
    rt1 = (
        "Incident First Unit Response - 1st Real Unit Assigned to 1st Real Unit Arrived"
    )
    rt2 = "Earliest Time Phone Pickup to 1st Real Unit Arrived"
    # nc1 - "Incident 1st Enroute to 1stArrived Time"
    rt3 = "Time Spent OnScene - 1st Real Unit Arrived to Last Real Unit Call Cleared"

    for i in recalcArray:
        fireDF.loc[i, rt1] = getSingleTimeDiff(
            fireDF, i, "Time First Real Unit Assigned", u
        )
        fireDF.loc[i, rt2] = getSingleTimeDiff(
            fireDF, i, "Earliest Time Phone Pickup AFD or EMS", u
        )
        fireDF.loc[i, nc1] = getSingleTimeDiff(
            fireDF, i, "Time First Real Unit Enroute", u
        )
        fireDF.loc[i, rt3] = getSingleTimeDiff(
            fireDF, i, u, "Last Real Unit Clear Incident"
        )

    # ----------------
    # Exporting and completion
    # ----------------
    # print to files

    # using builtin function vs using ExcelWriter class
    # fireDF.to_excel("Output{0}.xlsx".format((datetime.datetime.now()).strftime("%H-%M-%S")))

    # convert specific rows to format {h}:mm:ss format

    # Incident 1st Enroute to 1stArrived Time
    # Incident Duration - Ph PU to Clear
    # Unit  Ph PU to UnitArrived
    # fireDF[""] = fireDF[""].apply(utils.dtFormat)

    ######################################
    # show in gui just before writing
    gui = show(fireDF)

    writer = pd.ExcelWriter(
        "Output\\Output_{0}.xlsx".format(
            (datetime.datetime.now()).strftime("%y-%m-%d_%H-%M")
        ),
        engine="xlsxwriter",
        datetime_format="mm/dd/yyyy hh:mm:ss",
        date_format="mm/dd/yyyy",
    )

    fireDF.to_excel(writer)
    writer.save()
    # plt.savefig('saved_figure.png')


################################
# ==================================================================
#
#
# Testing Code: will only run when this file is called directly.
# ==================================================================
################################
if __name__ == "__main__":
    # load test files
    import loadTestFile

    df = loadTestFile.get()
    # run test file
    analyzeFire(df)