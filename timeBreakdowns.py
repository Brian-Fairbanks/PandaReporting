import pandas as pd
from pandasgui import show


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


# def addSingleVSMulti(df):
#     """
#     Add a column of concurrent values to a dataset

#     Parameters
#     --------------------------------
#     Original : dataframe
#         data to which you want to add the columns.  Must contain "Status" "Incident Call Count", and "Master Incident Number"
#     """

#     def assignSingleMulti(status, incident):
#         # incCount = df["Master Incident Number" == incident]

#         # if "status" in [1,'C']:
#         #     if (length of DF with same incident number = 0): "Single"
#         #     else "1st multi"
#         # else multi

#         return "incCount"

#     df["Single_vs_Multi Units ONSC"] = df.apply(
#         lambda row: assignSingleMulti(row["Status"], row["Master Incident Number"]),
#         axis=1,
#     )
#     return df
