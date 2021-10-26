from tabulate import tabulate
import sys
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


# reorder columns - sample code kept just in case, rewritten to putColAt
def set_column_sequence(dataframe, seq, front=True):
    cols = seq[:]  # copy so we don't mutate seq
    for x in dataframe.columns:
        if x not in cols:
            if front:  # we want "seq" to be in the front
                # so append current column to the end of the list
                cols.append(x)
            else:
                # we want "seq" to be last, so insert this
                # column in the front of the new column list
                # "cols" we are building:
                cols.insert(0, x)
    return dataframe[cols]


def putColAt(dataframe, seq, loc):
    # account for loc being too large
    if loc >= (len(dataframe.columns) - len(seq)):
        loc = len(dataframe.columns) - len(seq)
    if loc < 0:
        loc = 0
    cols = []
    curLoc = 0
    # account of it being 0
    if loc == 0:
        cols = seq[:]
    for x in dataframe.columns:
        if x not in cols + seq:
            cols.append(x)
            curLoc += 1
            # print(x, " : ", curLoc, "!=", loc)
            if curLoc == loc:
                cols += seq
    return dataframe[cols]


def myparser(x):
    # remove nulls
    if x == "" or x == None or ("NaTType") in str(type(x)):
        # print(x, type(x), " - is null")
        return None

    # if already datetime, fine
    if ("Timestamp") in str(type(x)) or ("date") in str(type(x)):
        return x

    # convert strings if you can
    try:
        y = dparser.parse(x)
        # print(x, type(x), " - is valid ")
        return y

    # return Unknown if you cant
    except:
        # print(x, type(x), " - failed")
        return None


def addTimeDiff(df, nt, t1, t2):
    """
    Returns a copy of a passed dataframe with a new row added

    :param df: Panda Dataframe, dataframe to add rows too
    :param nt: str, The name of the column to be created
    :param t1: str, the name of the row in df which houses the end datetime
    :param t2: str, the name of the row in df which houses the start datetime
    """
    # ensure valid dateTime, or properly noted error
    df[t1] = df[t1].apply(myparser)
    df[t2] = df[t2].apply(myparser)

    # set up distinct error codes
    invalidInputs = ["Unknown", "Invalid"]
    conditions = [
        ((df[t1].isnull()) | df[t2].isnull()),
        (
            ~(df[t1].astype(str).isin(invalidInputs))
            & ~(df[t2].astype(str).isin(invalidInputs))
        ),
    ]

    choices = [
        " ",
        # ((df[t1] - df[t2]).astype(str).str.split("0 days ").str[-1]),
        ((df[t1] - df[t2]) / np.timedelta64(1, "s")),
    ]
    df[nt] = np.select(
        conditions,
        choices,
        default="default",
    )

    # print(type(df.loc[1, nt]))
    # print(formatSeconds(float(df.loc[1, nt])))

    df[nt] = df[nt].apply(formatSeconds)

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
