import pandas as pd
import numpy as np
import easygui


def getCellFix(orig, df, changeArray, error):
    """
    takes a full dataframe, an abbreviated array of just the problem cells, and the specific cell that is causing problems
    prompts for corrections, and returns a fixed dataframe
    """
    print(df)
    # df = show(df)
    # messagebox.showwarning(
    #     "Errors in data",
    #     "There seem to be some errors in your data.  Please correct the following:",
    # )

    for ind, row in df.iterrows():
        # messagebox.showinfo("error: {0}".format(ind), str(row))
        getInp = easygui.enterbox(
            msg=f"{error}:\n {str(row)}",
            title=row["Master Incident Number"],
            default=row["Incident Time Call Entered in Queue"],
        )
        # directly set result.  We should try and make sure that the types match, right?
        orig.loc[ind, changeArray] = getInp
    return orig


## Main - Used for testing, and will be ignored on import.
def main():
    import FireCheck as fc

    testDict = {
        "Incident Time Call Entered in Queue": [
            "Python",
            "Java",
            "Haskell",
            "Go",
            "C++",
        ],
        "Master Incident Number": [120, 85, 95, 80, 90],
        "Earliest Time Phone Pickup AFD or EMS": [18, 22, 34, 10, np.nan],
        "Incident Time Call Entered in Queue": [17, 21, 33, 9, 22],
    }
    df = pd.DataFrame(testDict)

    print(df)
    print(" -- Starting File Checks --")
    # check 0 ----------------------------------------------------------------------
    print("missing 'Earliest Time Phone Pickup AFD or EMS': ", end="")
    c0 = fc.check0(df)
    if c0 is not None:
        getCellFix(
            df,
            c0,
            ["Earliest Time Phone Pickup AFD or EMS"],
            "Missing 'Earliest Time Phone Pickup AFD or EMS': ",
        )
    else:
        print("passed")

    print(df)


if __name__ == "__main__":
    main()
