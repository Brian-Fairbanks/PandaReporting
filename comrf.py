import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys
from utils import gracefulCrash


# =================================================================
#     get Complete Response Force for each Structure Fire
# =================================================================

# Input a single DF, and get a table containing all the relevant information for the Complete Response Force
def getCRF(df):
    structureFiresArray = getStructureFires(df)
    crf = []
    for f in structureFiresArray:
        crf.append(getIncidentCRF(f, df))

    return pd.DataFrame(crf)


# Sort through list, check for structure fires, and grab a list of unique IDs
def getStructureFires(df):
    try:
        sfDF = fireDF[(df["Problem"].str.contains("Structure Fire"))]
        incnums = sfDF["Master Incident Number"].values.tolist()
        # remove duplicates
        return list(set(incnums))
    except Exception as ex:
        gracefulCrash(ex, sys.exc_info())


# Take a single incident, return the analyzed CRF data for said incident
def getIncidentCRF(incident, df):
    try:
        incDF = df[(df["Master Incident Number"] == incident)]
        incDF = incDF.sort_values(by=["Unit Time Arrived At Scene"])

        # ret = incDF[["Unit Time Arrived At Scene", "Radio_Name"]]
        # print(ret)

        # instanciate a force count
        objDict = {"incident": incident, "time": "CRF never reached", "force": 0}

        res0 = incDF.index[incDF["Unit Time Arrived At Scene"].notnull()].tolist()
        # print(res0)

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
                objDict["force"] += 4
            elif any(
                map(
                    vehicle.__contains__,
                    [
                        "BAT",
                        "BT",
                    ],
                )
            ):
                objDict["force"] += 3
            else:
                objDict["force"] += 2

            # print(force, "/16")
            if objDict["force"] > 15:
                objDict["time"] = incDF.loc[
                    i,
                    "Incident First Unitresponse - 1st Real Unit assigned to 1st Real Unit Arrived",
                ]
                break

        return objDict

    except Exception as ex:
        gracefulCrash(ex, sys.exc_info())


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
    gracefulCrash(ex, sys.exc_info())


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

crfdf = getCRF(fireDF)
print(crfdf)

# wait for close command
input("\nPress enter to exit")
exit(0)
