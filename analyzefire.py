# Built in
import datetime

# Dependancies
import pandas as pd
import numpy as np

from shapely.geometry import Point
import geopandas as gpd
import geopy.distance

# Sibling Modules
from crf import getCRF
import utils
import ConcurrentUse as cu
import roads as rd
import getData as data
import timeBreakdowns as tb
import naming as n
import os
from os import path
import geocode

# Setup Logging for the remainder of the data
import ServerFiles as sf

runtime = datetime.datetime.now().strftime("%Y.%m.%d %H.%M")
logger = sf.setup_logging(f"RunLog-{runtime}.log", debug=False)
print = logger.info

# Setup base_dir as location of run-file 
base_dir = sf.get_base_dir()

# Dont warn me about potentially assigning a copy
pd.options.mode.chained_assignment = None

# import global additional/updatable json data
stationDict = data.getStations()
ourNames = ["AUSTIN-TRAVIS COUNTY EMS", "ESD02 - Pflugerville", "ESD02"]
locations = data.getLocations()
reserveUnits = data.getReserves()
specialUnits = data.getSpecialUnits()

# ----------------
# Exporting and completion
# ----------------
def export_to_xlsx(name, fileDF):
    # Format the current timestamp as part of the file name
    timestamp = datetime.datetime.now().strftime("%y-%m-%d_%H-%M")

    # Define the file path
    file_path = os.path.join("..", "Logs", f"{name}_{timestamp}.xlsx")

    print(f"Writing to XLSX: {file_path}")

    # Create the Excel writer using the xlsxwriter engine
    writer = pd.ExcelWriter(file_path, engine="xlsxwriter")

    # Write the DataFrame to the Excel file
    fileDF.to_excel(writer, index=False, sheet_name="Sheet1")

    # Get the xlsxwriter workbook and worksheet objects
    workbook = writer.book
    worksheet = writer.sheets["Sheet1"]

    # Automatically adjust the column widths to fit the content
    for i, col in enumerate(fileDF.columns):
        column_len = max(fileDF[col].astype(str).str.len().max(), len(col))
        worksheet.set_column(i, i, column_len)

    # Save the Excel file
    writer.close()

    print("  Complete")

    return file_path  # Return the file path for reference


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


def getLoc(unit_location, data_source, locations, x_long=None, y_lat=None, stationDict=None):
    """
    Determine the station based on location description or GPS coordinates.

    Parameters:
    - unit_location: The location of the unit at assign.
    - data_source: 'fire' or 'ems'
    - x_long: Longitude coordinate of GPS at unit assign (optional).
    - y_lat: Latitude coordinate of GPS at unit assign (optional).
    - locations: a dict of street names, and their associated stations
    - stationDict: Dictionary of station details including GPS coordinates (optional).

    Returns:
    - The determined station or None if no match is found.
    """
    # print(f"{unit_location}, {data_source}, ({x_long}, {y_lat})")
    if x_long is not None and y_lat is not None and stationDict is not None:
        # print(f"debug: checking GPS of EMS Units: {y_lat} , {x_long}")
        for station, data in stationDict.items():
            if is_near_station((y_lat, x_long), data["gps"]):
                # print(f"Debug: Location match found for station {station}")
                return station

    current_location = str(unit_location).lower()
    stationNum = None
    if "fs020" in current_location or "esd2 - station 2" in current_location:
        stationNum = "S" + current_location[-2:]
    elif data_source == 'fire':
        for street in locations.keys():
            if street.lower() in current_location:
                stationNum = locations[street]
                break
    # print(f"Debug: Location match not found for station")
    return stationNum


def getLocAtAssign(station, unit_location, data_source, locations, x_long=None, y_lat=None, stationDict=None):
    """
    Check if the unit was assigned at its station.

    Parameters:
    - station: The station to check.
    - unit_location: The location of the unit at assign.
    - data_source: 'fire' or 'ems'
    - locations: a dict of street names, and their associated stations
    - x_long: Longitude coordinate of GPS at unit assign (optional).
    - y_lat: Latitude coordinate of GPS at unit assign (optional).
    - stationDict: Dictionary of station details including GPS coordinates (optional).

    Returns:
    - True if the unit was assigned at its station, False otherwise.
    """
    assigned_station = getLoc(unit_location, data_source, locations, x_long, y_lat, stationDict)
    return station == assigned_station

def addLocAtAssignToDF(df, data_source, locations, stationDict):
    """
    Add a column indicating if the unit was assigned at its station.

    Parameters:
    - df: DataFrame containing unit data.
    - locations: a dict of street names, and their associated stations
    - stationDict: Dictionary of station details including GPS coordinates.

    Returns:
    - DataFrame with an additional column 'Assigned at Station'.
    """
    print("Begin Analyzing Location Assigned:\n================================================")
    if "Location_At_Assign_Time" in df.columns:
        df["Assigned at Station"] = df.apply(
            lambda row: getLocAtAssign(
                row["Station"], 
                row["Location_At_Assign_Time"],
                data_source,
                locations, 
                row.get("Longitude_at_Assign"), 
                row.get("Latitude_at_Assign"), 
                stationDict
            ),
            axis=1,
        )
    else:
        df["Assigned at Station"] = "Unknown"
    return df


def assign_reserve_to_station(row, dataSource, stationDict, specialUnits, locations):
    """
    Assigns a reserve unit to a station based on GPS coordinates, location, special units, and response box.

    Parameters:
    - row: The DataFrame row containing incident details.
    - stationDict: Dictionary of station details including GPS coordinates.
    - specialUnits: Dictionary of special units and their corresponding stations.
    - locations: a dict of street names, and their associated stations

    Returns:
    - The assigned station or 'UNKNOWN' if no match is found.
    """
    radioName = row["Radio_Name"]
    afd_response_box = row.get("AFD Response Box")
    x_long = row.get("Longitude_at_Assign")
    y_lat = row.get("Latitude_at_Assign")
    unit_location = row["Location_At_Assign_Time"]

    # Use getLoc to determine the station based on GPS or address
    stationNum = getLoc(unit_location, dataSource, locations, x_long, y_lat, stationDict)
    if stationNum:
        return stationNum

    # Check if unit is listed in special units
    if radioName in specialUnits:
        return specialUnits[radioName]

    # Check the AFD response box
    if afd_response_box:
        # Parse the AFD response box to extract station information
        parts = afd_response_box.split("-")
        if len(parts) == 2:
            _, first_station = parts
            return f"S{first_station[:2].zfill(2)}"

    return "UNKNOWN"

def stationName(row, dataSource, ourNames, stationDict, locations, reserveUnits, specialUnits):
    
    department = row["Department"]
    radioName = row["Radio_Name"]

    # Exclude non-unit frontline statuses
    notUnits = [
        "Not a unit",
        "Rescue Talk Group 1",
        "Rescue Talk Group 2",
        "Rescue Talk Group 3",
        "Rescue Talk Group 4",
        "MCOT",
        ""
    ]
    if row["Frontline_Status"] in notUnits:
        return "Not a unit"

    otherUnits = {
        "ESD12 - Manor": "ESD12 Manor",
        "WC - Round Rock": "RRFD",
    }
    # Handle private ambulance providers
    if row["Frontline_Status"] == "Private Ambulance Provider" or "ALG" in str(radioName):
        return "Private"

    # Handle our own units
    if department in ourNames:
        # Handle non-frontline units
        if row["Frontline_Status"] in ["Other", "Command"]:
            return "Admin"

        # Check reserve units
        if radioName in reserveUnits:
            return assign_reserve_to_station(row, dataSource, stationDict, specialUnits, locations)

        # Check special units
        if radioName in specialUnits:
            return specialUnits[radioName]

        # Default for our own frontline units
        return f"S0{radioName[-2]}"

    # Handle other units
    outsiders = otherUnits.get(department, department)
    nonFrontline = [
        "Command",
        "Other",
        "Support",
        "Clinical Practice",
        "Special Events Medic",
        "Emergency Support Unit",
        "Paramedic Practitioner Resp",
        "Aid Unit",
        "Administrative Support",
        "Administrative Staff",
    ]
    if row["Frontline_Status"] in nonFrontline:
        outsiders += " Other"

    return outsiders


def is_near_station(unit_coords, station_coords, threshold=0.1):
    """
    Check if the unit's coordinates are within a certain distance of the station coordinates.
    """
    if pd.isnull(unit_coords[0]) or pd.isnull(unit_coords[1]):
        return False
    distance = geopy.distance.distance(unit_coords, station_coords).miles
    return distance < threshold

def getStations(fireDF, dataSource, ourNames, stationDict, locations, reserveUnits, specialUnits):
    def apply_station_name(row):
        return stationName(row, dataSource, ourNames, stationDict, locations, reserveUnits, specialUnits)
    
    fireDF["Station"] = fireDF.apply(apply_station_name, axis=1)
    fireDF = utils.putColAt(fireDF, ["Station", "Status"], 0)
    return fireDF

# ===============================================================================================================
# End of Station Selection Scripts


def addFirstArrived(df):
    df["FirstArrived"] = False
    # get array of unique Incidents
    unique_incidents = df["Master Incident Number"].unique()
    # for each incident, get array of calls
    for incident in unique_incidents:
        incident_data = df[df["Master Incident Number"] == incident]
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

def get_data_source(df):
    if "FirstArrived" in df:
        return "fire"
    return "ems"

# ##############################################################################################################################################
#     Main Code
# ##############################################################################################################################################


def analyzeFire(fileDF):
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

    # Correct no GPS coord issues
    # =================================================================
    geocode.fixCoords(fileDF)

    dataSource = get_data_source(fileDF)

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

    fileDF["Response_Status"] = fileDF.apply(
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
        fileDF["FirstArrived_Orig"] = fileDF["FirstArrived"]
        # fileDF = addFirstArrived(fileDF)
        fileDF["FirstArrived"] = (
            fileDF["FirstArrived"].fillna("").map({"Yes": True, " ": False, "": False})
        )

    else:
        fileDF = reprocessPriority(fileDF)
        fileDF = addFirstArrived(fileDF)

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
        # if pd.isnull(firstArrived) or firstArrived == "" or firstArrived == " ":
        # if timeAtScene == timeFirstArrived:
        #     return "Yes"
        if firstArrived == "Yes" or firstArrived == True:
            return "Yes"
        if pd.isnull(timeAtScene):
            return "X"
        return "-"

    fileDF["FirstArrivedEsri"] = fileDF.apply(
        lambda x: replaceAssigned(
            x["FirstArrived"],
            x["Unit Time Arrived At Scene"],
            x["Time First Real Unit Arrived"],
        ),
        axis=1,
    )
    fileDF = utils.putColAfter(fileDF, ["FirstArrivedEsri"], "FirstArrived")

    #     Fire Data Error Checking
    # =================================================================
    from validateData import checkFile
    fileDF = checkFile(fileDF)

    #     Add unit type column to simplify analysis
    # =================================================================
        # Moved to preprocess steps

    #     Calculate Concurrent Use for Each Unit
    # =================================================================
    fileDF = cu.addConcurrentUse(fileDF, "Unit Time Assigned", "Unit Time Call Cleared")

    #     Set District 17 Values
    # =================================================================

    # Set up boundaries for ESD17
    ##############################################################
    print("loading esd shape:")
    # esd17 = gpd.read_file("Shape\\esd17.shp")
    shape_file_path = path.join(base_dir, "Shape", "esd17.shp")
    esd17 = gpd.read_file(shape_file_path)
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
    fileDF["IsESD17"] = np.vectorize(isESD)(
        fileDF["Jurisdiction"],
        fileDF["X-Long"],
        fileDF["Y_Lat"],
        fileDF["Master Incident Number"],
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
    # etj = gpd.read_file("Shape\\ETJ.shp")
    shape_file_path = path.join(base_dir, "Shape", "ETJ.shp")
    etj = gpd.read_file(shape_file_path)
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

    fileDF["isETJ"] = np.vectorize(isETJ)(fileDF["X-Long"], fileDF["Y_Lat"])

    # Clear data
    etj = None

    # =================================================================
    #     Set District COP Values
    # =================================================================
    # Set up boundaries for cop
    ##############################################################
    print("loading COP shape:")
    # cop = gpd.read_file("Shape\\City_Limits.shp")
    shape_file_path = path.join(base_dir, "Shape", "City_Limits.shp")
    cop = gpd.read_file(shape_file_path)
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

    fileDF["isCOP"] = np.vectorize(isCOP)(fileDF["X-Long"], fileDF["Y_Lat"])

    # Clear data
    cop = None

    # =================================================================
    #     Add Fire Response Areas to EMS Data
    # =================================================================
    print("loading response areas:")
    # AFD Response Areas shape file
    shape_file_path = path.join(base_dir, "Shape", "AFD_Response_Areas.shp")
    responseArea = gpd.read_file(shape_file_path)
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

    if "AFD Response Box" not in fileDF:
        fileDF["AFD Response Box"] = fileDF.apply(
            lambda x: getResponseArea(x["X-Long"], x["Y_Lat"]),
            axis=1,
        )
    # Clear data
    responseArea = None

    # =================================================================
    #     Set pop density values Values
    # =================================================================
    import popden

    fileDF = popden.addPopDen(fileDF)
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
        (fileDF["Master Incident Number"] != fileDF.shift(1)["Master Incident Number"])
        & (fileDF["Unit Time Arrived At Scene"].isnull()),
        (fileDF["Master Incident Number"] == fileDF.shift(1)["Master Incident Number"]),
    ]
    choices = ["C", "0"]
    fileDF["Status"] = np.select(conditions, choices, default="1")

    # Overwrite 0 with X on canceled calls
    # get array of indexes with 0
    res0 = fileDF.index[fileDF["Status"] == "0"].tolist()

    # this is going to be really slow...
    # this will also break if referencing an incident removed by
    for i in res0:
        fileDF.loc[i, "Status"] = (
            "X" if ((fileDF.loc[i - 1, "Status"] in (["X", "C"]))) else "0"
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

    fileDF["ESD02_Shift"] = fileDF.apply(
        lambda row: getShift(row[col_bb], row[col_w]), axis=1
    )

    # =================================================================
    #     Add a new Stations (Origin) Column
    # =================================================================
    fileDF = getStations(fileDF, dataSource, ourNames, stationDict, locations, reserveUnits, specialUnits)

    # =================================================================
    #     Add a new Column for if Unit was at its Station Address when Assigned
    # =================================================================
    fileDF = addLocAtAssignToDF(fileDF, dataSource, locations, stationDict)

    # =================================================================
    #     Calculate Station Distances
    # =================================================================
    print(" -- adding road checks")
    fileDF = rd.addRoadDistances(fileDF)

    # =================================================================
    #     add Is Sent From Closest Station
    # =================================================================
    print(" -- adding Closest Station")
    fileDF = addIsClosestStation(fileDF)

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
        fileDF = utils.addTimeDiff(
            fileDF, col, incidentCols[col][0], incidentCols[col][1]
        )

    # Move TimeDelta Columns to correct spot in file
    fileDF = utils.putColAfter(
        fileDF,
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
        fileDF = utils.addTimeDiff(fileDF, col, unitCols[col][0], unitCols[col][1])

    # Move TimeDelta Columns to correct spot in file
    fileDF = utils.putColAfter(
        fileDF,
        list(unitCols.keys()),
        "Unit Time Call Cleared",
    )

    # =================================================================
    # Correction of time: staging calls
    # =================================================================

    # Create columns assuming no... this to be changed only on those that are needed.
    fileDF["INC_Staged_As_Arrived"] = 0
    fileDF["UNIT_Staged_As_Arrived"] = 0

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
    recalc = fileDF[
        (
            (~fileDF[v].isnull())
            & (fileDF[v] != "-")
            & (~fileDF[u].isnull())
            & (fileDF[u] != "-")
            & (~fileDF[t].isnull())
            & (fileDF[t] != "-")
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
            fileDF.loc[i, "INC_Staged_As_Arrived"] = 1
            for col in recalcIncidentCols:
                fileDF.loc[i, col] = getSingleTimeDiff(
                    fileDF, i, recalcIncidentCols[col][0], u, recalcIncidentCols[col][1]
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
    recalc = fileDF[
        (
            (~fileDF[v].isnull())
            & (fileDF[v] != "-")
            & (~fileDF[u].isnull())
            & (fileDF[u] != "-")
            & (~fileDF[t].isnull())
            & (fileDF[t] != "-")
        )
    ]
    if not recalc.empty:
        recalc2 = recalc[((recalc[u] < recalc[v]) & (recalc[u] > recalc[t]))]
        recalcArray = recalc2.index.tolist()

        for i in recalcArray:
            fileDF.loc[i, "UNIT_Staged_As_Arrived"] = 1
            for col in recalcUnitCols:
                fileDF.loc[i, col] = getSingleTimeDiff(
                    fileDF, i, recalcUnitCols[col][0], u, recalcUnitCols[col][1]
                )

    # =================================================================
    #   Extra Time Formatted Columns
    # =================================================================

    # format times to [HH]:mm:ss
    fileDF = tb.addFormattedTimes(fileDF)

    # add Month / Year / Qtr Year
    fileDF = tb.addMothData(fileDF)

    # add call time classifications ('Calls > 20 Min - PU to Arrive' , 'Ph_PU2_UnitArrive Time_Intervals in seconds')
    fileDF = tb.addPhPuSteps(fileDF)

    # add call count / single / multi cols
    fileDF = tb.addCallCount(fileDF)
    fileDF = tb.addSingleVSMulti(fileDF)

    # move Qtr Year to end to match marys data
    fileDF = utils.putColAt(fileDF, ["Qtr Year"], 200)

    # =================================================================
    #     get Complete Response Force for each Structure Fire
    # =================================================================
    # check transport not reflecting onscene status

    if dataSource == "ems":
        # Create a new list of all incidents where unit time arrived and scene or transport count are null
        transportCheck = fileDF[
            (pd.isnull(fileDF["Unit Time Arrived At Scene"]))
            & (fileDF["Transport_Count"] > 0)
        ].index.tolist()

        # Iterate through said list...
        for i in transportCheck:
            # Set Status...
            if (fileDF.loc[i, "Incident Call Count"] == 1) and fileDF.loc[
                i, "Department"
            ] in ourNames:
                fileDF.loc[i, "Status"] = "1"
            else:
                fileDF.loc[i, "Status"] = "0"

            # Response Status
            fileDF.loc[i, "Response_Status"] = "ONSC"

            # First Arrived Esri
            if (fileDF.loc[i, "Incident Call Count"] == 1) and fileDF.loc[
                i, "Department"
            ] in ourNames:
                fileDF.loc[i, "FirstArrivedEsri"] = "1"
            else:
                fileDF.loc[i, "FirstArrivedEsri"] = "-"
            # fireDF.loc[i, "UNIT_Staged_As_Arrived"] = 1
            # for col in recalcUnitCols:
            #     fireDF.loc[i, col] = getSingleTimeDiff(
            #         fireDF, i, recalcUnitCols[col][0], u, recalcUnitCols[col][1]
            #     )
    # =================================================================
    #     get Complete Response Force for each Structure Fire
    # =================================================================
    crfdf = getCRF(fileDF)

    # fireDF.join(crfdf.set_index("incident"), on="Master Incident Number")
    try:
        fileDF = pd.merge(fileDF, crfdf, how="left", on=["Master Incident Number"])
    except:
        fileDF["Incident_ERF_Time"] = None
        fileDF["Force_At_ERF_Time_of_Close"] = None
        print("No ERF Found")

    # =================================================================
    # finalize naming
    # =================================================================
    fileDF = n.rename(fileDF)

    # =================================================================
    #     Column Organization
    # =================================================================

    fileDF = utils.putColAfter(
        fileDF,
        ["Incident_Call_Count"],
        "Force_At_ERF_Time_of_Close",
    )
    fileDF = utils.putColAfter(fileDF, ["Response_Status"], "Status")

    # export_to_xlsx("output", fileDF)

    return fileDF


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

    analyzeDF = analyzeFire(df)

    # ----------------
    # Write to Database
    # ----------------
    from Database import SQLDatabase

    db = SQLDatabase()
    db.new_insert_DF(analyzeDF, 'ems')
