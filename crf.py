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
        sfDF = df[(df["Problem"].str.contains("Structure Fire"))]
        incnums = sfDF["Master Incident Number"].values.tolist()
        # remove duplicates
        return list(set(incnums))
    except Exception as ex:
        gracefulCrash(ex, sys.exc_info())


# Take a single incident, return the analyzed CRF data for said incident
def getIncidentCRF(incident, df):
    try:
        incDF = df[(df["Master Incident Number"] == incident)]
        # print(incDF["Unit Time Arrived At Scene"])

        incDF = incDF.sort_values(by=["Unit Time Arrived At Scene"])

        # ret = incDF[["Unit Time Arrived At Scene", "Radio_Name"]]
        # print(ret)

        # instanciate a force count
        objDict = {
            "Master Incident Number": incident,
            "Incident CRF Time": "CRF never reached",
            "Force At CRF Time or Close": 0,
        }

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
                objDict["Force At CRF Time or Close"] += 4
            elif any(
                map(
                    vehicle.__contains__,
                    [
                        "BT",
                    ],
                )
            ):
                objDict["Force At CRF Time or Close"] += 3
            else:
                objDict["Force At CRF Time or Close"] += 2

            # print(
            #     objDict["Force At CRF Time or Close"],
            #     "/16 at ",
            #     incDF.loc[
            #         i,
            #         "Unit Dispatch to Onscene",
            #     ],
            # )
            # TODO:  over 16.  over 17 if if a quint is assigned at all.
            if objDict["Force At CRF Time or Close"] > 15:
                objDict["CRF Time"] = incDF.loc[
                    i,
                    "Unit Dispatch to Onscene",
                ]
                break

        return objDict

    except Exception as ex:
        gracefulCrash(ex, sys.exc_info())


# ##############################################################################################################################################
#     Main Code
# ##############################################################################################################################################
def main():
    fire = "Fire 07 2021 ESD02_RAWDATA_UPDATE_Fairbanks.xlsx"
    fireDF = pd.read_excel(fire)

    # replace all instances of "-" with null values
    fireDF = fireDF.replace("-", np.nan)

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
    # reset numbering to associate with the ordered values
    fireDF = fireDF.reset_index(drop=True)

    crfdf = getCRF(fireDF)
    print(crfdf)

    # wait for close command
    input("\nPress enter to exit")
    exit(0)


if __name__ == "__main__":
    # stuff only to run when not called via 'import' here
    main()
