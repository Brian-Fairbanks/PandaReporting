import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

excel_file_1 = ".\shift-data.xlsx"
excel_file_2 = "third-shift-data.xlsx"

df_1s = pd.read_excel(excel_file_1, sheet_name="first")
df_2s = pd.read_excel(excel_file_1, sheet_name="second")
df_3s = pd.read_excel(excel_file_2)

# print dataframe
print(df_1s)
# print dataframe confined to a specific col
print(df_1s["Product"])

# combinging dataframe
df_all = pd.concat([df_1s, df_2s, df_3s])
print(df_all)

# analysis
pivot = df_all.groupby(["Shift"]).mean()
shift_productivity = pivot.loc[
    :, "Production Run Time (Min)":"Products Produced (Units)"
]

print(shift_productivity)


# graphing
shift_productivity.plot(kind="bar")
plt.show()


# output to single excel
df_all.to_excel("output.xlsx")
