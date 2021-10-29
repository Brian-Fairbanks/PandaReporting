import numpy as np
import pandas as pd
from pandasgui import show


def recalcDict(arr, time):
    # print("=========================\n", arr)
    for unit in arr:
        arr[unit] = list(filter(lambda x: x > time, arr[unit]))
    # print("\n------\n", arr, "\n")
    return arr


def addConcurrentUse(orig, startName, endName):
    """
    Add a column of concurrent values to a dataset

    Parameters
    --------------------------------
    Original : dataframe
        data to which you want to add the columns
    Start Name : str - ex: Unit Time Assigned
        the time the unit shipped out
    End Name : str - ex: Unit Time Call Cleared
        the time the unit will finish its current call

    Returns
    --------------------------------
    Dataframe
        a new dataframe which is a copy of orig, but with an extra column used to identify concurrent use at the time of unit assignment
    """
    # create a dictionary of units, and when they will be done
    unitDict = {}

    # create the column with a obviously default number
    orig["Concurrent Usage"] = np.NaN

    # limit the dictionary as much as possible, since this will go quite slow
    # start with jsut our jurisdiction
    # distArr = orig.index[(orig["Jurisdiction"].isin(["ESD02", "ESD17"]))].tolist()
    distArr = orig.index[(orig["Department"].isin(["ESD02 - Pflugerville"]))].tolist()

    for ind in distArr:
        # get start and end time of incident
        startTime = orig.loc[ind, startName]
        endTime = orig.loc[ind, endName]

        # get unit type from the name
        unitType = "".join(
            [d for d in str(orig.loc[ind, "Radio_Name"]) if not d.isdigit()]
        )

        # store end time for this type into the dictionary
        unitDict[unitType] = [endTime] + (
            unitDict[unitType] if unitType in unitDict else []
        )

        # remove those that are already finished
        recalcDict(unitDict, startTime)

        # store current count as concurrent usage
        orig.loc[ind, "Concurrent Usage"] = (
            len(unitDict[unitType]) - 1  # remove on since this one is being counted
        )

    return orig  # .astype({"Concurrent Usage": "Int64"})


## Main - Used for testing, and will be ignored on import.
def main():
    import loadTestFile

    df = loadTestFile.get()

    # remove data not useful for the testing
    limit = [
        "Master Incident Number",
        "Department",
        "Radio_Name",
        "Unit Time Assigned",
        "Unit Time Call Cleared",
    ]
    # df = df[limit]

    # test the function
    df = addConcurrentUse(df, "Unit Time Assigned", "Unit Time Call Cleared")
    # show the results
    show(df)


if __name__ == "__main__":
    main()
