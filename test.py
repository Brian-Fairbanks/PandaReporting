import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from utils import gracefulCrash
from cfr import getCRF
import utils


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
    fire = "fire 06 2021 Raw QV Data.xlsx"
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
c3 = c3[(~c3["Calltaker Agency"].isin(["TCSO", "APD"]))]
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
    (fireDF[az] == "ESD02 - Pflugerville") & fireDF["Radio_Name"] == "BT235",
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
    "",
    "S0" + fireDF["Radio_Name"].str[-2],
]

fireDF["Station"] = np.select(conditions, choices, default=fireDF["Radio_Name"])


# move Status col to front
fireDF = utils.putColAt(fireDF, ["Station", "Status"], 0)
fireDF = utils.putColAt(fireDF, ["Master Incident Without First Two Digits"], 100)

# print to files
fireDF.to_excel("test.xlsx")
# plt.savefig('saved_figure.png')


# wait for close command
# input("\nPress enter to exit")
exit(0)
