from tabulate import tabulate
import traceback
import numpy as np
import pandas as pd
import dateutil.parser as dparser


def pprint(dframe):
    print(tabulate(dframe, headers="keys", tablefmt="psql", showindex=False))


def gracefulCrash(err, trace):
    print("ERROR:", err)
    traceback.print_exception(*trace)
    input("\nPress enter to exit")
    exit(1)


def putColAfter(dataframe, sequence, location):
    """
    Returns a copy of a passed dataframe, but with specific rows moved around

    :param dataframe: Panda Dataframe, dataframe restructure rows
    :param sequence: [str], an array of rows by name in the order they should appear
    :param location: str, the name of the column this sequence should follow
    """
    at = dataframe.columns.get_loc(location) + 1
    return putColAt(dataframe, sequence, at)


def putColAt(dataframe, sequence, location):
    """
    Returns a copy of a passed dataframe, but with specific rows moved around

    :param dataframe: Panda Dataframe, dataframe restructure rows
    :param sequence: [str], an array of rows by name in the order they should appear
    :param location: int, the row number that the sequence should be moved to: 0 to be first.  (anything larger than the dataframe will default to the end)
    """
    # account for loc being too large
    if location >= (len(dataframe.columns) - len(sequence)):
        location = len(dataframe.columns) - len(sequence)
    if location < 0:
        location = 0
    cols = []
    curLoc = 0
    # account of it being 0
    if location == 0:
        cols = sequence[:]
    for x in dataframe.columns:
        if x not in cols + sequence:
            cols.append(x)
            curLoc += 1
            if curLoc == location:
                cols += sequence
    return dataframe[cols]


last = []


def verifyTime(x):
    # track the types coming through so we can more easily conform them all.  Alternatively, look into re-shaping
    global last
    if not (type(x) in last):
        last.append(type(x))
        # print(last)

    # remove nulls
    if x == "" or x == None or ("NaTType") in str(type(x)):
        # print(x, type(x), " - is null")
        return None

    # if already datetime, fine
    if ("Timestamp") in str(type(x)) or ("date") in str(type(x)):
        return x
    # removing this since it creates problems with 'TypeError: Cannot cast array data from dtype('<m8[ns]') to dtype('<M8[ns]') according to the rule 'same_kind''

    # convert strings if you can
    try:
        y = dparser.parse(x)
        # print(x, type(x), " - is valid ")
        return y

    # return Unknown if you cant
    except:
        print(x, " ( ", type(x), " ) - failed")
        return None


def addTimeDiff(df, nt, timeStart, timeEnd):
    """
    Returns a copy of a passed dataframe with a new row added

    :param df: Panda Dataframe, dataframe to add rows too
    :param nt: str, The name of the column to be created
    :param timeStart: str, the name of the row in df which houses the start datetime
    :param timeEnd: str, the name of the row in df which houses the end datetime
    """
    global last
    last = []
    # ensure valid dateTime, or properly noted error
    df[timeEnd] = df[timeEnd].apply(verifyTime)
    df[timeStart] = df[timeStart].apply(verifyTime)

    df[nt] = (df[timeEnd] - df[timeStart]).astype("timedelta64[s]")
    # print(df[nt])

    # print(type(df.loc[1, nt]))
    # print(formatSeconds(float(df.loc[1, nt])))

    # convert from seconds into H:M:S
    # df[nt] = df[nt].apply(formatSeconds)

    return df


def formatSeconds(seconds):
    if pd.isnull(seconds):
        return ""

    seconds = int(float(seconds))

    hours = seconds // (60 * 60)
    seconds %= 60 * 60
    minutes = seconds // 60
    seconds %= 60
    return "%02i:%02i:%02i" % (hours, minutes, seconds)


def dtformat(x):
    # print(type(x))
    # print(formatSeconds(x / np.timedelta64(1, "s")))
    return formatSeconds(x / np.timedelta64(1, "s"))


def getUnitType(name):
    try:
        # get unit type from the name

        # Remove RR from unit name, then round rock units should conform just fine to the others
        name = name.replace("RR", "")
        # then return whats left sans digits
        return "".join([d for d in str(name) if not d.isdigit()])
    except:
        return None


def addUnitType(orig):
    # orig["Unit Type"] = orig.apply(lambda x )
    orig["Unit Type"] = np.vectorize(getUnitType)(orig["Radio_Name"])
    return orig


def getUnitBucket(type):
    buckets = {
        "BT": "ENG",
        "ENG": "ENG",
        "QNT": "ENG",
        "RS": "ENG",
        "TK": "ENG",
        "LAD": "ENG",
        "M": "MED",
        "MED": "MED",
        "MEDC": "MED",
    }
    if type not in buckets:
        return type
    return buckets[type]


def addBucketType(orig):
    orig["Bucket Type"] = orig.apply(lambda x: getUnitBucket(x["Unit Type"]), axis=1)
    return orig


def longComSub(st1: str, st2: str):
    """
    Calculate the longest common substring between 2 given strings.  Returns None if not match, or either value are not strings.
    param st1: String
    param st2: String
    """
    if pd.isnull(st1) or pd.isnull(st2):
        return None
    st1 = st1.lower()
    st2 = st2.lower()
    ans = ""
    for a in range(len(st1)):
        for b in range(len(st2)):
            curStr = []
            k = 0
            while ((a + k) < len(st1) and (b + k) < len(st2)) and st1[a + k] == st2[
                b + k
            ]:
                # print(
                #     f"{a}:{st1[a+k]}  -  {b}:{st2[b+k]}  -  {k}:{st1[a+k]}  =  {curStr}"
                # )
                curStr += st2[b + k]
                k += 1
            if len(ans) <= len(curStr):
                ans = curStr
    return ans


def addWalkUp(df: pd.DataFrame):
    """
    Add column to determine if a unit "walked up":

    data frame must contain the following columns:
        Location_At_Assign_Time ,
        Address of Incident ,
        Unit Dispatch to Respond Time   ,
        Unit Respond to Arrival
    """
    # calculation to determine if an individual unit is a walkup
    def getWalkTimes(t1, t2, substr):
        try:
            if abs(t1) < 2 or abs(t2) < 2 or len(substr) > 5:
                return True
        except:
            pass
        return False

    # add column for common substring for every rows starting/ending locations
    df["location_substring"] = df.apply(
        lambda row: longComSub(
            row["Location_At_Assign_Time"], row["Address of Incident"]
        ),
        axis=1,
    )

    # add column for final decision on is Walkup
    df["is_walkup"] = df.apply(
        lambda row: getWalkTimes(
            row["Unit Dispatch to Respond Time"],  # assign -> enroute
            row["Unit Respond to Arrival"],  # enroute -> arrived
            row["location_substring"],
        ),
        axis=1,
    )

    return df
