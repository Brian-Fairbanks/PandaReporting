import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys


def gracefulCrash(err):
    print(err)
    input("\nPress enter to exit")
    exit(1)


# set up scope for fire and ems files
fire, ems = "", ""

try:
    for i in range(1, 3):
        print
        if "fire" in sys.argv[i].lower():
            fire = sys.argv[i]
        if "ems" in sys.argv[i].lower():
            ems = sys.argv[i]
except IndexError:
    pass
except Exception as ex:
    gracefulCrash(ex)


if fire == "":
    gracefulCrash("A file was not found for Fire Data")
# if ems == "":
#     gracefulCrash("A file was not found for EMS Data")


df_1s = pd.read_excel(fire)
print(df_1s)

# plt.savefig('saved_figure.png')

# wait for close command
input("\nPress enter to exit")
exit(0)
