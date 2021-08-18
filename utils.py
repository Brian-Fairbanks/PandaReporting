from tabulate import tabulate
import sys
import traceback


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
