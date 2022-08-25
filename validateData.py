import FireCheck as fc
import cellFix as cf


def checkFile(df):
    print("Starting File Checks")
    curError = "Error unknown"

    # check 0 ----------------------------------------------------------------------
    curError = " -- missing 'Earliest Time Phone Pickup AFD or EMS': "
    print(curError, end="")
    c0 = fc.check0(df)
    if c0 is not None:
        df = cf.getCellFix(df, c0, ["Earliest Time Phone Pickup AFD or EMS"], curError)
    else:
        print("passed")

    # check 1 ----------------------------------------------------------------------
    curError = " -- 'Missing First Arrived Status: "
    print(curError, end="")
    c1 = fc.check1(df)
    if c1 is not None:
        cf.getCellFix(df, c1, ["FirstArrived"], curError)
    else:
        print("passed")

    # check 2 ----------------------------------------------------------------------
    curError = " -- 'Missing Arrival Time: "
    print(curError, end="")
    c2 = fc.check2(df)
    if c2 is not None:
        cf.getCellFix(df, c2, ["Unit Time Arrived At Scene"], curError)
    else:
        print("passed")

    # check 3 ----------------------------------------------------------------------
    curError = " -- 'Earliest Time Phone Pickup AFD or EMS': "
    print(curError, end="")
    c3 = fc.check3(df)
    if c3 is not None:
        cf.getCellFix(df, c3, ["Earliest Time Phone Pickup AFD or EMS"], curError)
    else:
        print("passed")

    # the checks may have alerted the user to input new data: make sure that this current data is passed back at the end
    return df
