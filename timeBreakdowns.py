import pandas as pd
from pandasgui import show
import numpy as np


def addPhPuSteps(df):
    """
    Add a column for "Ph_PU2_UnitArrive Time_Intervals in seconds" values to a dataset

    Parameters
    --------------------------------
    df : dataframe
        data to which you want to add the columns.  must contain "Earliest Phone Pickup Time to Unit Arrival"
    """

    def getStep(time):
        if pd.isnull(time):
            return time
        if time <= 240:
            return "1-240"
        if time <= 390:
            return "241-390"
        if time <= 450:
            return "391-450"
        if time <= 600:
            return "451-600"
        if time <= 750:
            return "601-750"
        if time <= 600:
            return "Over 20 Min"
        return "Over 750"

    df["Ph_PU2_UnitArrive Time_Intervals in seconds"] = df.apply(
        lambda row: getStep(row["Earliest Phone Pickup Time to Unit Arrival"]), axis=1
    )
    return df


def addCallCount(df):
    """
    Add a column showing number of accompanying calls on a specific incident to a dataset

    Parameters
    --------------------------------
    Original : dataframe
        data to which you want to add the columns.  Must contain "Master Incident Number"
    """
    valCount = df["Master Incident Number"].value_counts()
    df["Incident Call Count"] = df.apply(
        lambda row: valCount.at[row["Master Incident Number"]],
        axis=1,
    )
    return df


def addSingleVSMulti(df):
    """
    Add a column for Single vs Multi values to a dataset

    Parameters
    --------------------------------
    Original : dataframe
        data to which you want to add the columns.  Must contain "Status" and "Incident Call Count"
    """

    df["Unit Response Single_vs_Multi Response Count"] = df.apply(
        lambda row: np.where(row["Status"] in ["1", "C"], "1", "0"), axis=1
    )

    def assignSingleMulti(status, count):
        if status in [1, "1", "C"]:
            if count == 1:
                return "Single"
            return "1st Multi"
        return "Multi"

    df["Single_vs_Multi Units ONSC"] = df.apply(
        lambda row: assignSingleMulti(row["Status"], row["Incident Call Count"]),
        axis=1,
    )
    return df
