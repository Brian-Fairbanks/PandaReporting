import pandas as pd


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
    # create a dictionary of
    unitDict = []

    # for i in res0:
    # fireDF.loc[i, "Status"] = (
    #     "X" if ((fireDF.loc[i - 1, "Status"] in (["X", "C"]))) else "0"
    # )
    return orig


## Main - Used for testing, and will be ignored on import.
def main():
    # testDict = {
    #     "Incident Time Call Entered in Queue": [
    #         "Python",
    #         "Java",
    #         "Haskell",
    #         "Go",
    #         "C++",
    #     ],
    #     "Master Incident Number": [120, 85, 95, 80, 90],
    #     "Earliest Time Phone Pickup AFD or EMS": [18, 22, 34, 10, np.nan],
    # }
    # df = pd.DataFrame(testDict)

    import loadTestFile

    df = loadTestFile.get()

    df = addConcurrentUse(
        df, "ENG2", "Unit Time Arrived At Scene", "Unit Time Call Clared"
    )


if __name__ == "__main__":
    main()
