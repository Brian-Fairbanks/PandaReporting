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
