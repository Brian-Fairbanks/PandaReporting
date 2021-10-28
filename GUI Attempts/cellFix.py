import pandas as pd
from pandasgui import show

from tkinter import *
from tkinter import messagebox
from tkinter import ttk
from tkinter.filedialog import askopenfile, askopenfilenames
import FireCheck as fc
import numpy as np

import pandas as pd


def getCellFix(orig, df, changeArray):
    """
    takes a full dataframe, an abbreviated array of just the problem cells, and the specific cell that is causing problems
    prompts for corrections, and returns a fixed dataframe
    """
    print(df)
    # df = show(df)
    messagebox.showwarning(
        "Errors in data",
        "There seem to be some errors in your data.  Please correct the following:",
    )
    for ind, row in df.iterrows():
        messagebox.showinfo("error: {0}".format(ind), str(row))
    return df
    print(df)


def main():
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
    }
    df = pd.DataFrame(testDict)

    print(df)
    print(" -- Starting File Checks --")
    # check 0 ----------------------------------------------------------------------
    print("missing 'Earliest Time Phone Pickup AFD or EMS': ", end="")
    c0 = fc.check0(df)
    if c0 is not None:
        getCellFix(df, c0, ["Earliest Time Phone Pickup AFD or EMS"])
    else:
        print("passed")

    print(df)


main()
