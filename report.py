import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

excel_file_1 = '.\shift-data.xlsx'
# excel_file_2 = 'third-shift-data.xlsx'

df_fs = pd.read_excel(excel_file_1, sheet_name='first')
# df_2s = pd.read_excel(excel_file_1, sheet_name='second')
# df_3s = pd.read_excel(excel_file_2)

print(df_fs)