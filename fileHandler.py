from tkinter import *
from tkinter import messagebox
from tkinter import ttk
from tkinter.filedialog import askopenfile, askopenfilenames

import pandas as pd


def addFiles():
    files = askopenfilenames(parent=ws, title="Choose Files")
    for file in files:
        fileList.insert(0, file)


def clearData():
    tv1.delete(*tv1.get_children())


def loadFile():
    fileName = fileList.get(fileList.curselection())
    try:
        excel_filename = r"{}".format(fileName)
        df = pd.read_excel(excel_filename)
    except ValueError:
        messagebox.showerror("The loaded file is invalid")
        return None
    except FileNotFoundError:
        messagebox.showerror("No such file as {excel_filename}")
        return None

    clearData()
    tv1["column"] = list(df.columns)
    tv1["show"] = "headings"

    for col in tv1["columns"]:
        tv1.heading(col, text=col)

    df_rows = df.to_numpy().tolist()
    for row in df_rows:
        tv1.insert("", "end", values=row)

    return None


# def uploadFiles():
#     pb1 = Progressbar(ws, orient=HORIZONTAL, length=300, mode="determinate")
#     pb1.grid(row=4, columnspan=3, pady=20)
#     for i in range(5):
#         ws.update_idletasks()
#         pb1["value"] += 20
#         time.sleep(1)
#     pb1.destroy()
#     Label(ws, text="File Uploaded Successfully!", foreground="green").grid(
#         row=4, columnspan=3, pady=10
#     )


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

#     Frame for data
# =========================================================================================================================
frame2 = LabelFrame(ws, text="Excel Data", height=10, width=20)
frame2.grid(row=1, column=0, columnspan=4, sticky=("nsew"))

# frame2.columnconfigure(0, weight=1)

tv1 = ttk.Treeview(frame2)
tv1.place(relheight=1, relwidth=1)
treescrolly = Scrollbar(frame2, orient="vertical", command=tv1.yview)
treescrollx = Scrollbar(frame2, orient="horizontal", command=tv1.xview)
tv1.configure(xscrollcommand=treescrollx.set, yscrollcommand=treescrolly.set)
treescrolly.pack(side="right", fill="y")
treescrollx.pack(side="bottom", fill="x")


# upld = Button(ws, text="Upload Files", command=uploadFiles)
# upld.grid(row=3, columnspan=3, pady=10)

ws.mainloop()
