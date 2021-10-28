import pandas as pd
from pandasgui import show

from tkinter import *
from tkinter import messagebox
from tkinter import ttk
from tkinter.filedialog import askopenfile, askopenfilenames
import FireCheck as fc

import pandas as pd


def getCellFix(orig, df, changeArray):
    print(df)
    show(df)
    # messagebox.showwarning(
    #     "Errors in data",
    #     "There seem to be some errors in your data.  Please correct the following:",
    # )
    # for ind, row in df.iterrows():
    #     messagebox.showinfo("error", str(row))
    return df


def checkFile(df):
    print(" -- Starting File Checks --")
    # check 0 ----------------------------------------------------------------------
    print("missing 'Earliest Time Phone Pickup AFD or EMS': ", end="")
    c0 = fc.check0(df)
    if c0 is not None:
        df = getCellFix(df, c0, ["Earliest Time Phone Pickup AFD or EMS"])
    else:
        print("passed")

    # check 1 ----------------------------------------------------------------------
    print("'Missing First Arrived Status: ", end="")
    c1 = fc.check1(df)
    if c1 is not None:
        getCellFix(df, c1, ["FirstArrived"])
    else:
        print("passed")

    # check 2 ----------------------------------------------------------------------
    print("'Missing Arrival Time: ", end="")
    c2 = fc.check2(df)
    if c2 is not None:
        getCellFix(df, c2, ["Unit Time Arrived At Scene"])
    else:
        print("passed")

    # check 3 ----------------------------------------------------------------------
    print("'Earliest Time Phone Pickup AFD or EMS", end="")
    c3 = fc.check3(df)
    if c3 is not None:
        getCellFix(df, c3, ["Earliest Time Phone Pickup AFD or EMS"])
    else:
        print("passed")


def addFiles():
    files = askopenfilenames(parent=ws, title="Choose Files")
    # ensure unique items in list
    for file in files:
        # temporarily add to file list to show that things are running
        fileList.insert("end", file)

        # then check if file is valid, read it, and hold onto its DF
        if not file in fileArray.keys():
            try:
                excel_filename = r"{}".format(file)
                # read the file
                fileArray[file] = pd.read_excel(excel_filename)
                # sort the array
                fileArray[file] = fc.sort(fileArray[file])

            except ValueError:
                messagebox.showerror("Invalid File", "The loaded file is invalid")
                return None
            except FileNotFoundError:
                messagebox.showerror("Invalid File", "No such file as {excel_filename}")
                return None

    # remove and re-add
    fileList.delete(0, "end")
    for file in fileArray.keys():
        fileList.insert("end", file)

    for df in fileArray:
        checkFile(fileArray[df])


def clearData():
    tv1.delete(*tv1.get_children())


def loadFile():
    fileName = fileList.get(fileList.curselection())
    df = fileArray[fileName]

    clearData()
    tv1["column"] = list(df.columns)
    tv1["show"] = "headings"

    for col in tv1["columns"]:
        tv1.heading(col, text=col)

    df_rows = df.to_numpy().tolist()
    for row in df_rows:
        tv1.insert("", "end", values=row)

    return None


fileArray = {}


ws = Tk()
ws.title("Fire/EMS Management")
ws.geometry("400x400")

ws.columnconfigure(0, weight=1)
ws.rowconfigure(1, weight=1)

#     Frame for file dialog
# =========================================================================================================================
frame1 = LabelFrame(ws, text="File Selection")
frame1.grid(row=0, column=0, columnspan=4, sticky=("ew"))

frame1.columnconfigure(0, weight=1)

addFileLabel = Label(frame1, text="Add Files to List")
addFileLabel.grid(row=0, column=0, padx=10)


addFileBtn = Button(frame1, text="Choose File", command=lambda: addFiles())
addFileBtn.grid(row=0, column=1)

loadExcelDataBtn = Button(frame1, text="Load File", command=lambda: loadFile())
loadExcelDataBtn.grid(row=0, column=2)


fileList = Listbox(frame1, height=5)
fileList.grid(row=1, column=0, columnspan=4, sticky=("ew"))
# TODO - Add ability to drag and drop files directly onto this list

ws.mainloop()
