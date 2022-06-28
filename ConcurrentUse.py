import numpy as np
import pandas as pd
from pandasgui import show
import utils as u
from timer import Timer
from tqdm import tqdm

ourDepartment = ["ESD02 - Pflugerville", "ESD02"]


def recalcDict(arr, time):
    # print("=========================\n", arr)
    for bucket in arr:
        arr[bucket] = list(filter(lambda x: x > time, arr[bucket]))
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
        the time the bucket shipped out
    End Name : str - ex: Unit Time Call Cleared
        the time the bucket will finish its current call

    Returns
    --------------------------------
    Dataframe
        a new dataframe which is a copy of orig, but with an extra column used to identify concurrent use at the time of bucket assignment
    """
    # create a dictionary of buckets, and when they will be done
    bucketDict = {}

    # create the column with a obviously default number
    orig["Concurrent Usage"] = np.NaN

    # ---------------- SECONDARY - TIME IN RANGE CALCULATIONS ---------------
    # =======================================================================
    # set up timeInterval for easier overlap detection

    # dont crash the program when no close is given please...
    def makeInterval(start, end):
        interval = pd.Interval(0, 0, closed="neither")
        try:
            interval = pd.Interval(start, end, closed="right")
        except Exception as e:
            print(e)
        return interval

    orig["Time_Interval"] = orig.apply(
        lambda row: makeInterval(row[startName], row[endName]), axis=1
    )

    # Set up columns to reveal specific time overlap
    for x in range(10):
        orig[f"Time_{x}_Active"] = 0
    #  -----------------------------------------------------------------------

    # limit the dictionary as much as possible, since this will go quite slow
    # start with just our jurisdiction
    # distArr = orig.index[(orig["Jurisdiction"].isin(["ESD02", "ESD17"]))].tolist()
    distArr = orig.index[(orig["Department"].isin(ourDepartment))].tolist()
    with tqdm(total=len(distArr), desc=f"Getting Concurrency") as pbar:
        for ind in distArr:
            # get start and end time of incident
            startTime = orig.loc[ind, startName]
            endTime = orig.loc[ind, endName]

            # get bucket type from the name
            # use the column if it exists, since this work should already be done.
            try:
                bucketType = str(orig.loc[ind, "Bucket Type"])
            except:
                bucketType = u.getUnitBucket(u.getUnitType(orig.loc[ind, "Radio_Name"]))

            # store end time for this type into the dictionary
            bucketDict[bucketType] = [endTime] + (
                bucketDict[bucketType] if bucketType in bucketDict else []
            )

            # remove those that are already finished
            recalcDict(bucketDict, startTime)

            # store current count as concurrent usage
            orig.loc[ind, "Concurrent Usage"] = (
                len(bucketDict[bucketType])
                - 1  # remove on since this one is being counted
            )

            # ---------------- SECONDARY - TIME IN RANGE CALCULATIONS ---------------
            # =======================================================================
            orig = getTimes(orig, ind, orig.loc[ind, "Time_Interval"], bucketType)
            # =======================================================================
            pbar.update(1)

    # remove the new useless interval column
    orig = orig.drop(
        [
            "Time_Interval",
        ],
        axis=1,
    )
    return orig  # .astype({"Concurrent Usage": "Int64"})


# ===================================
#       SECONDARY - TIME IN RANGE CALCULATIONS
# ===================================
def getTimes(df, ind, interval, bucket):
    """
    Get get all time breakdowns for a specific passed row of the passed DF
    """
    # filter on same bucket
    commonTimes = df[
        (df["Bucket Type"] == bucket) & (df["Department"].isin(ourDepartment))
    ]

    # filter that on overlap times
    commonTimes = commonTimes[
        commonTimes.apply(lambda row: interval.overlaps(row["Time_Interval"]), axis=1)
    ]
    # this should never actually happen afterall, you should ALWAYS find yourself.
    # if commonTimes.empty:
    #     return df

    # get list of all overlapping start/end points
    timeList = (
        commonTimes["Unit Time Assigned"].tolist()
        + commonTimes["Unit Time Call Cleared"].tolist()
    )

    timeList.sort()
    # remove everything before this units start and everything after this unit end
    # after opening set to eliminate overlap issues, the original interval now needs to be added in manually
    tempTime = [interval.left]
    for x in timeList:
        if (x in interval) and (x not in tempTime):
            tempTime.append(x)

    # WTF is this list not sorted?

    # print(f"---------------------------------------------------------\n{timeList}")
    # print(f"\nfiltered:\n{tempTime}\n\n")

    # use the list to group out concurrency
    # -----------------------
    inc = df.loc[ind, "Master Incident Number"]
    # print(
    #     f"==================================================================================\n----- {inc}: \n{interval}\n\n{timeList}"
    # )
    timeList = tempTime

    timeDict = {}
    prevTime = timeList[0]
    # create a breakdown of
    # skip the first, as it should always be 0 seconds
    for time in timeList[1:]:
        timeRange = time - prevTime

        setLength = (
            commonTimes[
                commonTimes.apply(lambda row: time in row["Time_Interval"], axis=1)
            ].shape[0]
            - 1
        )
        # print(f"  {str(time):<30}: {str(timeRange):<23}-   {setLength}")
        # max out the time to be 9+ units
        if setLength > 9:
            setLength = 9
        try:
            timeDict[setLength] += timeRange
        except:
            timeDict[setLength] = timeRange
        prevTime = time

    # print(timeDict)
    for x in timeDict:
        # store each time as seconds into the rows new fields
        df.loc[ind, f"Time_{x}_Active"] = int(timeDict[x] / np.timedelta64(1, "s"))

    # show(commonTimes)

    return df


## Main - Used for testing, and will be ignored on import.
def main():
    import loadTestFile

    df = loadTestFile.get()

    # remove data not useful for the testing
    # limit = [
    #     "Master Incident Number",
    #     "Department",
    #     "Radio_Name",
    #     "Unit Time Assigned",
    #     "Unit Time Call Cleared",
    # ]
    # df = df[limit]

    # =================================================================
    #     Add unit type column to simplify analysis
    # =================================================================
    df = u.addUnitType(df)
    df = u.addBucketType(df)

    # test the function
    df = addConcurrentUse(df, "Unit Time Assigned", "Unit Time Call Cleared")
    # show the results
    show(df)


if __name__ == "__main__":
    main()
