from tabulate import tabulate
import traceback
import numpy as np
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
    at = dataframe.columns.get_loc(location)
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


def addTimeDiff(df, nt, t1, t2):
    """
    Returns a copy of a passed dataframe with a new row added

    :param df: Panda Dataframe, dataframe to add rows too
    :param nt: str, The name of the column to be created
    :param t1: str, the name of the row in df which houses the end datetime
    :param t2: str, the name of the row in df which houses the start datetime
    """
    global last
    last = []
    # ensure valid dateTime, or properly noted error
    df[t1] = df[t1].apply(verifyTime)
    df[t2] = df[t2].apply(verifyTime)

    df[nt] = (df[t1] - df[t2]).astype("timedelta64[s]")
    # print(df[nt])

    # print(type(df.loc[1, nt]))
    # print(formatSeconds(float(df.loc[1, nt])))

    # convert from seconds into H:M:S
    # df[nt] = df[nt].apply(formatSeconds)

    return df


def formatSeconds(seconds):
    if seconds == "" or seconds == " ":
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
    # get unit type from the name
    return "".join([d for d in str(name) if not d.isdigit()])


def addUnitType(orig):
    # orig["Unit Type"] = orig.apply(lambda x )
    orig["Unit Type"] = np.vectorize(getUnitType)(orig["Radio_Name"])
    return orig


def getUnitBucket(type):
    buckets = {"BT": "ENG", "ENG": "ENG", "QNT": "ENG"}
    if type not in buckets:
        return type
    return buckets[type]


def addBucketType(orig):
    orig["Bucket Type"] = orig.apply(lambda x: getUnitBucket(x["Unit Type"]), axis=1)
    return orig
