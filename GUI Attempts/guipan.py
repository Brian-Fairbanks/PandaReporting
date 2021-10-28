import pandas as pd
from pandasgui import show

from tkinter import *
from tkinter import messagebox
from tkinter.filedialog import askopenfile, askopenfilenames
import FireCheck as fc

from validateData import checkFile


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
        df = checkFile(fileArray[df])


fileArray = {}


ws = Tk()
ws.title("Fire/EMS Management")
ws.geometry("400x200")

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

# loadExcelDataBtn = Button(frame1, text="Load File", command=lambda: loadFile())
# loadExcelDataBtn.grid(row=0, column=2)


fileList = Listbox(frame1, height=5)
fileList.grid(row=1, column=0, columnspan=4, sticky=("ew"))

# TODO - Add ability to drag and drop files directly onto this list

ws.mainloop()
