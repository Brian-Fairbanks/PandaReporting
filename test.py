import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from utils import gracefulCrash
from cfr import getCRF
import utils
import datetime

# Create a Pandas Excel writer using XlsxWriter as the engine.
# Also set the default datetime and date formats.

# writer = pd.ExcelWriter(
#     "Output.xlsx",
#     engine="xlsxwriter",
#     datetime_format="mmm d yyyy hh:mm:ss",
#     date_format="mmmm dd yyyy",
# )


reserveUnits = ["ENG280", "ENG290", "BT235"]

locations = {
    "BRATTON": "S02",
    "WREN": "S01",
    "KELLY": "S03",
    "PICADILLY": "S04",
    "NIMBUS": "S05",
    # "DEFAULT": "ALERT to user in gui to determine location"
}

print(locations.keys)

# ##############################################################################################################################################
#     Main Code
# ##############################################################################################################################################

# set up scope for fire and ems files
fire, ems = "", ""

try:
    for i in range(1, 3):
        if "fire" in sys.argv[i].lower():
            fire = sys.argv[i]
        if "ems" in sys.argv[i].lower():
            ems = sys.argv[i]
except IndexError:
    # just means that we dont need to check all 3 files
    pass
except Exception as ex:
    gracefulCrash(ex, sys.exc_info())

#   Handle file input errors
# ------------------------------------------------------------
if fire == "":
    fire = "2021 Jan-March Fire Data.xlsx"
    # fire = "fire 06 2021 Raw QV Data.xlsx"
    # gracefulCrash("A file was not found for Fire Data")
# if ems == "":
#     gracefulCrash("A file was not found for EMS Data")


fireDF = pd.read_excel(fire)
# save an OG copy
try:
    # check if file_original exists, and only write to it if it does not.
    fireDF.to_excel(fire.split(".")[0] + "_Original.xlsx")
except Exception as ex:
    print(ex)
    input("\nPress enter to exit")
    exit(1)

# =================================================================
#     get Complete Response Force for each Structure Fire
# =================================================================
# crfDF = getCRF(fireDF)
# utils.pprint(crfDF)


# =================================================================
#     Formatting and Renaming of DataFrames
# =================================================================

# fireDF.rename(columns={"Master Incident Number": "Incident Number"})

# confirm time values are recognized as time values
fireDF["Earliest Time Phone Pickup AFD or EMS"] = pd.to_datetime(
    fireDF["Earliest Time Phone Pickup AFD or EMS"],
    # format="%m/%d/%Y %H:%M:%S",
    infer_datetime_format=True,
    errors="ignore",
)

#    "Last Real Unit Clear Incident"
#    "Earliest Time Phone Pickup AFD or EMS"

# # confirm time values are recognized as time values
# fireDF["Last Real Unit Clear Incident"] = pd.to_datetime(
#     fireDF["Last Real Unit Clear Incident"],
#     # format="%m/%d/%Y %H:%M:%S",
#     infer_datetime_format=True,
#     errors="ignore",
# )


# order fire data by time : - 'Master Incident Number' > 'Unit Time Arrived At Scene' > 'Unit Time Staged' > 'Unit Time Enroute' > 'Unit Time Assigned'
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


# ##############################################################################################################################################
#     Fire Data Error Checking
# ##############################################################################################################################################

# =================================================================
#     Check # 0 -  Checking for misssing "Earliest Time Phone Pickup AFD or EMS"
# =================================================================

# If any case where 'Earliest Time Phone Pickup AFD or EMS' is blank, Either pull updated information from visinet, or copy time from 'Incident Time Call Entered in Queue'
c0 = fireDF[(fireDF["Earliest Time Phone Pickup AFD or EMS"].isnull())]
if c0.size > 0:
    # limit the rows that will show in the error output
    limit = [
        "Master Incident Number",
        "Earliest Time Phone Pickup AFD or EMS",
        "Incident Time Call Entered in Queue",
    ]
    c0.to_excel("debug.xlsx")
    print(
        "Warning: Please check on the following incidents:\n 'Earliest Time Phone Pickup AFD or EMS' is blank \n 'Earliest Time Phone Pickup AFD or EMS' field must have a value to continue\n Either pull updated information from visinet, or copy time from 'Incident Time Call Entered in Queue' field \n\n"
    )
    utils.pprint(c0[limit])
    input("\nPress enter to exit")
    exit(1)
c0 = ""


# =================================================================
#     Check # 1 -  Checking for misssing first arrived status
# =================================================================
# its a problem if there is no FirstArrived
# check each incident in visinet - find Phone Pickup Time
c1 = fireDF[(fireDF["FirstArrived"].isnull())]
c1 = c1[(c1["Unit Time Arrived At Scene"].notnull())]
# and it is not an
#     alarm test - ALARMT
#     burn notification - CNTRL02
#     test - TEST'

c1 = c1[(~c1["Radio_Name"].isin(["ALARMT", "CNTRL02", "TEST"]))]
# would like to display as: Unit Time Assigned,	Unit Time Enroute	Unit Time Staged,	Unit Time Arrived At Scene,	Unit Time Call Cleared

#  ---------------
# Solution here is to set first arrived of incident to Yes, and all others to -
# ----------------
if c1.size > 0:
    limit = [
        "Master Incident Number",
        "Unit Time Assigned",
        "Unit Time Enroute",
        "Unit Time Staged",
        "Unit Time Arrived At Scene",
        "Unit Time Call Cleared",
    ]
    c1.to_excel("debug.xlsx")
    print(
        "Warning: Please check on the following incidents:\n We arrived on scene, but first arrived is blank \n 'Earliest Time Phone Pickup AFD or EMS' field must have a value to continue \n\n"
    )
    utils.pprint(c1[limit])
    input("\nPress enter to exit")
    exit(1)
c1 = ""

# =================================================================
#     Check #2 -  Missing First Arrived Time
# =================================================================
c2 = fireDF[(fireDF["FirstArrived"] == "Yes")]
c2 = c2[(c2["Unit Time Arrived At Scene"].isnull())]

# ----------------
# To automate solution here
# copy data from "Time First Real Unit Arrived"
# ----------------
if c2.size > 0:
    print(
        "Warning: Please check on the following incidents:\nThese incidents are missing 'Unit Time Arrived At Scene' field \n 'Unit Time Arrived At Scene' field must have a value to continue \n\n",
        c2,
    )
    input("\nPress enter to exit")
    exit(1)
c2 = ""

# =================================================================
#     Check #3 -  PhonePickupTime is  unknown*
# =================================================================
c3 = fireDF[
    (fireDF["Earliest Time Phone Pickup AFD or EMS"] == "Unknown")
    | (fireDF["Earliest Time Phone Pickup AFD or EMS"].isnull())
]
###  more than likely TCSO or APD, but still has to be filled
# c3 = c3[(~c3["Calltaker Agency"].isin(["TCSO", "APD"]))]

if c3.size > 0:
    limit = [
        "Master Incident Number",
        "Earliest Time Phone Pickup AFD or EMS",
        "Unit Time Assigned",
        "Unit Time Enroute",
        "Unit Time Staged",
        "Unit Time Arrived At Scene",
        "Unit Time Call Cleared",
    ]
    print(
        "Warning: Please check on the following incidents:\n'Earliest Time Phone Pickup AFD or EMS' is blank or 'unknown':\n"
    )
    utils.pprint(c3[limit])
    input("\nPress enter to exit")
    exit(1)

c3 = ""

# To automate solution here
# c3 = fireDF[(fireDF["Earliest Time Phone Pickup AFD or EMS"] == "Unknown")]
# overwrite Unkown with time from - "Incident Time Call Entered in Queue"
## < -----------------------------------------------------------------------------------  look into how to overwrite data with pandas and run this!

# ##############################################################################################################################################
#     After checks are preformed...
# ##############################################################################################################################################


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

# Add a new Stations Column
# look up radio name and department
# S01, S02, S03, S04, S05, ADMIN, AFD, OTHER

# pulled from fire data xlsx
# =IF(AND(AZ1="ESD02 - Pflugerville",BA1="Frontline"),RIGHT(Fire_Table[@[Radio Name]],3),"ADMIN ESD02")
az = "Department"
ba = "Frontline_Status"


conditions = [
    (fireDF[az] == "AFD") & (fireDF[ba] == "Other"),
    (fireDF[az] == "AFD"),
    (fireDF[az] == "ESD12 - Manor") & (fireDF[ba] == "Other"),
    (fireDF[az] == "ESD12 - Manor"),
    (fireDF[az] == "ESD02 - Pflugerville") & (fireDF[ba] == "Other"),
    (fireDF[az] == "ESD02 - Pflugerville") & (fireDF["Radio_Name"] == "QNT261"),
    # account for instances where vehicle is filling in for another.  We will need to deterimine which station it is filling in for based on the location at time of assignment
    (fireDF[az] == "ESD02 - Pflugerville") & (fireDF["Radio_Name"].isin(reserveUnits)),
    (fireDF[az] == "ESD02 - Pflugerville"),
]
choices = [
    "AFD Other",
    "AFD",
    "ESD12 - Manor Other",
    "ESD12 - Manor",
    "ADMIN ESD02",
    "S05",
    # determination of the above cases
    "RESERVE",
    "S0" + fireDF["Radio_Name"].str[-2],
]

fireDF["Station"] = np.select(conditions, choices, default=fireDF["Radio_Name"])

# move Status col to front
fireDF = utils.putColAt(fireDF, ["Station", "Status"], 0)
fireDF = utils.putColAt(fireDF, ["Master Incident Without First Two Digits"], 100)


# ----------------
# Time Data Extra Colulmn Creation
# ----------------

nc1 = "Incident 1st Enroute to 1stArrived Time"
nc2 = "Incident Duration - Ph PU  to Clear"
nc3 = "Unit  Ph PU to UnitArrived"

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
    "Time First Real Unit Arrived" "Earliest Time Phone Pickup AFD or EMS",
)

# fireDF[nc3] = (
#     fireDF["Time First Real Unit Arrived"]
#     - fireDF["Earliest Time Phone Pickup AFD or EMS"]
# )


#   right after "Incident Turnout - 1st Real Unit Assigned to 1st Real Unit Enroute", AD
fireDF = utils.putColAt(fireDF, [nc1], 29)  # 29
#   right after "Incident First Unitresponse - 1st Real Unit assigned to 1st Real Unit Arrived", AD
fireDF = utils.putColAt(fireDF, [nc2], 31)
#   right after "Unit Assign to Clear Call", AD
fireDF = utils.putColAt(fireDF, [nc3], 33)

# # Testing for time data creation
# timeData = fireDF[
#     [
#         "Master Incident Number",
#         "Time First Real Unit Arrived",
#         "Time First Real Unit Enroute",
#         nc1,
#         "Last Real Unit Clear Incident",
#         "Earliest Time Phone Pickup AFD or EMS",
#         nc2,
#     ]
# ]
# timeData.to_excel(
#     "timeData{0}.xlsx".format((datetime.datetime.now()).strftime("%H-%M-%S"))
# )

# ----------------
# Exporting and completion
# ----------------
# print to files

# using builtin function vs using ExcelWriter class
fireDF.to_excel("test.xlsx")
# fireDF.to_excel(writer)
# writer.save()
# plt.savefig('saved_figure.png')


# wait for close command
# input("\nPress enter to exit")
exit(0)
