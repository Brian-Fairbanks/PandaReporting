import pandas as pd
from pandasgui import show


def recalcDict(arr, time):
    tempDict = {}
    for unit in arr:
        if arr[unit] > time:
            tempDict[unit] = arr[unit]
    return tempDict


def addConcurrentUse(orig, unitsToCheck, startName, endName):
    """
    Add a column of concurrent values to a dataset

    Parameters
    --------------------------------
    Original : dataframe
        data to which you want to add the columns
    Units to Check : str - ex: 'ENG2' - only engines in district 2
        a unique substring of units to include as "concurrent"
    Start Name : str - ex: Unit Time Arrived At Scene
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

    orig["Concurrent Usage"] = 0

    # limit the dictionary as much as possible, since

    # for i in res0:
    # fireDF.loc[i, "Status"] = (
    #     "X" if ((fireDF.loc[i - 1, "Status"] in (["X", "C"]))) else "0"
    # )
    return orig


## Main - Used for testing, and will be ignored on import.
def main():
    import loadTestFile

    df = loadTestFile.get()

    # remove data not useful for the testing
    limit = [
        "Master Incident Number",
        "Radio_Name",
        "Unit Time Arrived At Scene",
        "Unit Time Call Cleared",
    ]
    df = df[limit]
    print(df)

    # test the function
    df = addConcurrentUse(
        df, "ENG2", "Unit Time Arrived At Scene", "Unit Time Call Cleared"
    )
    # show the results
    show(df)


if __name__ == "__main__":
    main()
