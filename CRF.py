import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from tabulate import tabulate
import sys


def fmtStation(name):
    num = name
    return "S0" + num


# reorder columns
def set_column_sequence(dataframe, seq, front=True):
    cols = seq[:]  # copy so we don't mutate seq
    for x in dataframe.columns:
        if x not in cols:
            if front:  # we want "seq" to be in the front
                # so append current column to the end of the list
                cols.append(x)
            else:
                # we want "seq" to be last, so insert this
                # column in the front of the new column list
                # "cols" we are building:
                cols.insert(0, x)
    return dataframe[cols]


def putColAt(dataframe, seq, loc):
    # account for loc being too large
    if loc >= (len(dataframe.columns) - len(seq)):
        loc = len(dataframe.columns) - len(seq)
    if loc < 0:
        loc = 0
    cols = []
    curLoc = 0
    # account of it being 0
    if loc == 0:
        cols = seq[:]
    for x in dataframe.columns:
        if x not in cols + seq:
            cols.append(x)
            curLoc += 1
            # print(x, " : ", curLoc, "!=", loc)
            if curLoc == loc:
                cols += seq
    return dataframe[cols]


def pprint(dframe):
    print(tabulate(dframe, headers="keys", tablefmt="psql", showindex=False))


def gracefulCrash(err):
    print("ERROR:", err)
    input("\nPress enter to exit")
    exit(1)


# Sort through list, check for structure fires, and grab a list of unique IDs
def getStructureFires():
    try:
        sfDF = fireDF[(fireDF["Problem"].str.contains("Structure Fire"))]
        incnums = sfDF["Master Incident Number"].values.tolist()
        # remove duplicates
        return list(set(incnums))
    except Exception as ex:
        gracefulCrash(ex)


def getCRF(incident):
    try:
        incDF = fireDF[(fireDF["Master Incident Number"] == incident)]
        incDF = incDF.sort_values(by=["Unit Time Arrived At Scene"])

        # ret = incDF[["Unit Time Arrived At Scene", "Radio_Name"]]
        # print(ret)

        # instanciate a force count
        force = 0
        time = "CRF never reached"

        res0 = incDF.index[incDF["Unit Time Arrived At Scene"].notnull()].tolist()
        # print(res0)

        # Get Start Time
        try:
            sTime = incDF.loc[res0[0], "Unit Time Arrived At Scene"]
        except:
            sTime = "No Units Arrived"

        # this is going to be really slow...
        for i in res0:
            vehicle = incDF.loc[i, "Radio_Name"]
            if any(
                map(
                    vehicle.__contains__,
                    [
                        "QNT",
                        "ENG",
                    ],
                )
            ):
                force += 4
            elif any(
                map(
                    vehicle.__contains__,
                    [
                        "BAT",
                        "BT",
                    ],
                )
            ):
                force += 3
            else:
                force += 2

            # print(force, "/16")
            if force > 15:
                return incDF.loc[
                    i,
                    "Incident First Unitresponse - 1st Real Unit assigned to 1st Real Unit Arrived",
                ]

                break

        return time

    except Exception as ex:
        gracefulCrash(ex)


# ##############################################################################################################################################
#     Main Code
# ##############################################################################################################################################

# # set up scope for fire and ems files
fire, ems = "", ""

try:
    for i in range(1, 3):
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

# =================================================================
#     get Complete Response Force for each Structure Fire
# =================================================================
structureFiresArray = getStructureFires()
print(structureFiresArray)
for f in structureFiresArray:
    print(f, ": ", getCRF(f))


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


# print to files
fireDF.to_excel("test.xlsx")
# plt.savefig('saved_figure.png')


# wait for close command
input("\nPress enter to exit")
exit(0)
