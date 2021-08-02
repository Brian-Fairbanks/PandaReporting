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
    fire = "06 2021 Raw QV Data.xlsx"
    # gracefulCrash("A file was not found for Fire Data")
# if ems == "":
#     gracefulCrash("A file was not found for EMS Data")


fireDF = pd.read_excel(fire)
# print(fireDF)

# =================================================================
#     Formatting and Renaming of DataFrames
# =================================================================

# fireDF.rename(columns={"Master Incident Number": "Incident Number"})
# print(fireDF)


# =================================================================
#     get Complete Response Force for each Structure Fire
# =================================================================
# structureFiresArray = getStructureFires()
# print(structureFiresArray)
# for f in structureFiresArray:
#     getCRF(f)


# =================================================================
#     Check # 1 -  Checking for misssing first arrived status
# =================================================================
# Check for first arrived Fire Data
# its a problem if there is no FirstArrived
c1 = fireDF[(fireDF["FirstArrived"].isnull())]
# and there is
c1 = c1[(c1["Unit Time Arrived At Scene"].notnull())]
# and it is not an
# alarm test - ALARMT
# burn notification - CNTRL02
# test - TEST'

c1 = c1[(~c1["Radio_Name"].isin(["ALARMT", "CNTRL02", "TEST"]))]

if c1.size > 0:
    c1.to_excel("debug.xlsx")
    print(
        "Warning: Please check on the following incidents, where We were not the first arrived, and not a test: \n",
        c1,
    )
    exit(1)
c1 = ""

# =================================================================
#     Check #2 -  Missing First Arrived Time
# =================================================================
c2 = fireDF[(fireDF["FirstArrived"] == "Yes")]
c2 = c2[(c2["Unit Time Arrived At Scene"].isnull())]
if c2.size > 0:
    print("Warning: Please check on the following incidents:\n", c2)
    exit(1)
c2 = ""

# =================================================================
#     Check #3 -  PhonePickupTime is  unknown*
# =================================================================
c3 = fireDF[(fireDF["Earliest Time Phone Pickup AFD or EMS"] == "Unknown")]
###  more than likely TCSO or APD.  Confirm, print error if not
c3 = c3["Calltaker Agency"].isin(["TCSO", "APD"])
print("There should be an error right here")
print(c3)
input("\nPress enter to exit")
if c3.size > 0:
    print(
        "Warning: Please check on the following incidents, where PhonePickupTime is 'unknown', but not from TSCO or APD:\n",
        c3,
    )
    exit(1)

# overwrite Unkown with time from - "Incident Time Call Entered in Queue"
c3 = ""


# =================================================================
#     Check #4 -  PhonePickupTime is null
# =================================================================
c4 = fireDF[(fireDF["Earliest Time Phone Pickup AFD or EMS"].isnull())]
if c4.size > 0:
    print(
        "Warning: Please check on the following incidents, where Phone Pickup Time is Null:\n",
        c4,
    )
    exit(1)
c4 = ""

# print to files
# firstArrivedArray.to_excel("test.xlsx")
# plt.savefig('saved_figure.png')


# wait for close command
input("\nPress enter to exit")
exit(0)
