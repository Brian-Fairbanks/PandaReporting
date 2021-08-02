import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys


def gracefulCrash(err):
    print("ERROR:", err)
    input("\nPress enter to exit")
    exit(1)


# Sort through list, check for structure fires, and grab a list of unique IDs
def getStructureFires():
    try:
        sfDF = fireDF[(fireDF["Problem"].str.contains("Structure Fire"))]
        incnums = sfDF["Incident Number"].values.tolist()
        # remove duplicates
        return list(set(incnums))
    except Exception as ex:
        gracefulCrash(ex)


def getCRF(incident):
    try:
        incDF = fireDF[(fireDF["Incident Number"] == incident)]
        incDF.sort_values(by=["Ph PU to UnitArrive in seconds"])
        print(incDF, "\n\n")
    except Exception as ex:
        gracefulCrash(ex)


# # set up scope for fire and ems files
fire, ems = "", ""

try:
    for i in range(1, 3):
        print
        if "fire" in sys.argv[i].lower():
            fire = sys.argv[i]
        if "ems" in sys.argv[i].lower():
            ems = sys.argv[i]
except IndexError:
    pass
except Exception as ex:
    gracefulCrash(ex)


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
# print(fireDF)

# =================================================================
#     get Complete Response Force for each Structure Fire
# =================================================================
# structureFiresArray = getStructureFires()
# print(structureFiresArray)
# for f in structureFiresArray:
#     getCRF(f)


# =================================================================
#     Formatting and Renaming of DataFrames
# =================================================================

# fireDF.rename(columns={"Master Incident Number": "Incident Number"})
# print(fireDF)


# order fire data by time : - 'Master Incident Number' > 'Unit Time Arrived At Scene' > 'Unit Time Staged' > 'Unit Time Enroute' > 'Unit Time Assigned'
fireDF.sort_values(
    by=[
        "Master Incident Number",
        "Unit Time Arrived At Scene",
        "Unit Time Staged",
        "Unit Time Enroute",
        "Unit Time Assigned",
    ]
)
print(fireDF)


# ##############################################################################################################################################
#     Fire Data Error Checking
# ##############################################################################################################################################

# =================================================================
#     Check # 0 -  Checking for misssing "Earliest Time Phone Pickup AFD or EMS"
# =================================================================

# If any case where 'Earliest Time Phone Pickup AFD or EMS' is blank, Either pull updated information from visinet, or copy time from 'Incident Time Call Entered in Queue'
c0 = fireDF[(fireDF["Earliest Time Phone Pickup AFD or EMS"].isnull())]
if c0.size > 0:
    c0.to_excel("debug.xlsx")
    print(
        "Warning: Please check on the following incidents:\n 'Earliest Time Phone Pickup AFD or EMS' is blank \n 'Earliest Time Phone Pickup AFD or EMS' field must have a value to continue\n Either pull updated information from visinet, or copy time from 'Incident Time Call Entered in Queue' field \n\n",
        c0,
    )
    input("\nPress enter to exit")
    exit(1)
c0 = ""


# =================================================================
#     Check # 1 -  Checking for misssing first arrived status
# =================================================================
# Check for first arrived Fire Data
# its a problem if there is no FirstArrived

# check each incident in visinet - find Phone Pickup Time
c1 = fireDF[(fireDF["FirstArrived"].isnull())]
# and there is
c1 = c1[(c1["Unit Time Arrived At Scene"].notnull())]
# and it is not an
# alarm test - ALARMT
# burn notification - CNTRL02
# test - TEST'

c1 = c1[(~c1["Radio_Name"].isin(["ALARMT", "CNTRL02", "TEST"]))]
# Unit Time Assigned	Unit Time Enroute	Unit Time Staged	Unit Time Arrived At Scene	Unit Time Call Cleared


#  ---------------
# Solution here is to set first arrived of incident to Yes, and all others to -
# ----------------
if c1.size > 0:
    c1.to_excel("debug.xlsx")
    print(
        "Warning: Please check on the following incidents:\n We arrived on scene, but first arrived is blank \n 'Earliest Time Phone Pickup AFD or EMS' field must have a value to continue \n\n",
        c1,
    )
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
c3 = fireDF[(fireDF["Earliest Time Phone Pickup AFD or EMS"] == "Unknown")]
###  more than likely TCSO or APD.  Confirm, print error if not
c3 = c3[(~c3["Calltaker Agency"].isin(["TCSO", "APD"]))]
if c3.size > 0:
    print(
        "Warning: Please check on the following incidents:\n'Earliest Time Phone Pickup AFD or EMS' is 'unknown', but 'Calltaker Agency' is not TCSO or APD:\n",
        c3,
    )
    input("\nPress enter to exit")
    exit(1)

c3 = ""

# To automate solution here
# c3 = fireDF[(fireDF["Earliest Time Phone Pickup AFD or EMS"] == "Unknown")]
# overwrite Unkown with time from - "Incident Time Call Entered in Queue"
## < -----------------------------------------------------------------------------------  look into how to overwrite data with pandas and run this!


# =================================================================
#     Check #4 -  PhonePickupTime is null
# =================================================================
c4 = fireDF[(fireDF["Earliest Time Phone Pickup AFD or EMS"].isnull())]

# To automate solution here
# get first 'Earliest Time Phone Pickup AFD or EMS' of same 'Master Incident Number'
# copy this in

if c4.size > 0:
    print(
        "Warning: Please check on the following incidents:/n'Earliest Time Phone Pickup AFD or EMS' is Null:\n",
        c4,
    )
    input("\nPress enter to exit")
    exit(1)
c4 = ""

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

# Add a new Stations Column
# look up radio name and department
# S01, S02, S03, S04, S05, ADMIN, AFD, OTHER


# print to files
# firstArrivedArray.to_excel("test.xlsx")
# plt.savefig('saved_figure.png')


# wait for close command
input("\nPress enter to exit")
exit(0)
