import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sys

try:
    droppedFile = sys.argv[1]
except IndexError:
    print("Incorrect Files")


df_1s = pd.read_excel(droppedFile, sheet_name="first")
print(df_1s)

# wait for close command
input("Press enter to exit ;)")
