import ServerFiles as sf
logger = sf.setup_logging("GUI.log")

import pandas as pd
import traceback
import sys

import copy
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# from pandasgui import show

from tkinter import *
# We can remove messagebox since we no longer use it
#from tkinter import messagebox
from tkinter.filedialog import askopenfile, askopenfilenames

import analyzefire as af
import preprocess as pp
import numpy as np
import Email_Report as er

from Database import SQLDatabase

db = SQLDatabase()
from Email_Report import get_and_run_reports, send_email_with_dataframes  # Assuming send_email is available

fileArray = {}
ws = None

def email_error_report(error_details):
    logger.info("Sending error report by email")

    # 1) Load the 'alerts(EXAMPLE)' configuration from your JSON
    try:
        alerts_email_config = sf.get_email_config("GUI Fatal Error")
    except FileNotFoundError as e:
        logger.error(f"Configuration file missing: {e}")
        return
    except KeyError as e:
        logger.error(f"Invalid configuration section: {e}")
        return

    # 2) Construct the subject & body from the JSON config
    subject_prefix = alerts_email_config.get("subject_prefix", "Alert Notification:")
    subject = f"{subject_prefix} File Processing Error"
    email_body = f"An error occurred:\n\n{error_details}"

    # 3) Update your base email_config from Email_Report.py
    #    so it includes the recipients, cc, subject, etc.
    email_config = copy.deepcopy(er.email_config)
    email_config.update({
        "recipient_emails": alerts_email_config.get("recipients", ""),
        "cc_emails": alerts_email_config.get("cc", ""),
        "subject": subject,
        "Email_Body": email_body
    })

    # 4) Send an email without dataframes by passing an empty dict
    er.send_email_with_dataframes({}, email_config)
    logger.info("Error email sent successfully")


def handle_fatal_error(error, file=None):
    import traceback
    tb = traceback.format_exc()
    error_details = f"Error processing file {file if file else ''}:\n{error}\nTraceback:\n{tb}"
    print(error_details)
    logger.error(error_details)

    # Send the error email using the new function
    email_error_report(error_details)

    sys.exit(1)


def createGui():
    global ws
    ws = Tk()
    ws.title("Fire/EMS Management")
    ws.geometry("600x200")

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

    analyzeButton = Button(frame1, text="Analyze Data", command=lambda: guiAnalyze())
    analyzeButton.grid(row=0, column=2)

    rawInsert = Button(frame1, text="Insert Raw Data", command=lambda: insertRaw())
    rawInsert.grid(row=1, column=2)

    global fileList
    fileList = Listbox(frame1, height=5)
    fileList.grid(row=3, column=0, columnspan=4, sticky=("ew"))

    linkData = Button(ws, text="Update Dependency Tables", command=lambda: update_dependency_tables())
    linkData.grid(row=1, column=0, columnspan=3)

    run_Reports = Button(ws, text="Email Reports", command=lambda: runReports())
    run_Reports.grid(row=2, column=0, columnspan=3)

    return ws


# TODO - Add ability to drag and drop files directly onto this list


def guiAnalyze():
    for file in fileArray:
        fileDF = af.analyzeFire(fileArray[file])
        # ----------------
        # Write to Database
        # ----------------
        from Database import SQLDatabase
        
        data_source = fileDF.loc[0, "Data_Source"]

        db = SQLDatabase()
        # db.insertDF(fileDF)
        db.new_insert_DF(fileDF, data_source)

    return None


def runReports():
    get_and_run_reports()


def update_dependency_tables():
    from datetime import datetime, timedelta
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    one_month_ago = today - timedelta(days=30)
    one_month_ago = one_month_ago.replace(hour=0, minute=0, second=0, microsecond=0)
    print("Updating Fire-EMS Link Table")
    db.RunFireEMSLink(one_month_ago)
    print(" - Done Updating!")
    print("Updating Truck Concurrency Table")
    db.RunConcurrencyUpdate(one_month_ago, today)
    print(" - Done Updating!")


def readRaw(filePath):
    """
    Reads the file and processes it based on its type.
    """
    df = read_file(filePath)
    if "Ph_PU_Time" in df.columns or "Ph PU Time" in df.columns:
        fileType = "ems"
        pp.scrub_raw_ems(df)
    else:
        fileType = "fire"

        df, non_esd_records = pp.split_esd_records(df)
        df = pp.revert_fire_format(df)
        # Dump non_esd records if they exist
        try:
            if len(non_esd_records.index) != 0:
                pp.dump_to_database(non_esd_records, fileType)
        except Exception as e:
            tb = traceback.format_exc()
            print(f"Error Dumping Raw Data: {e}\nTraceback: {tb}")
            sys.exit(1)
    df = df.replace("-", np.nan)
    renames = {
        "ESD02_Record_Daily": "ESD02_Record",
        "ESD02_Record_New_Daily": "ESD02_Record",
        "ESD02_Record_New_Monthly": "ESD02_Record",
        "ESD02_Record_New": "ESD02_Record",
    }
    df = df.rename(columns=renames, errors="ignore")
    return df, fileType

def insertRaw():
    for file in fileArray.keys():
        try:
            df, filetype = readRaw(file)
            if filetype == "fire":
                df = df.drop([
                    "Longitude_At_Assign_Time",
                    "Latitude_At_Assign_Time",
                ], axis=1, errors="ignore")
            dumpRawData(df, filetype)
        except ValueError as e:
            handle_fatal_error(e, file)
        except FileNotFoundError as e:
            handle_fatal_error(e, file)


def remove_completed_files():
    print("Clearing completed Files from FileArray")
    fileArray.clear()


def addFiles(files=None):
    if files is None:
        files = askopenfilenames(parent=ws, title="Choose Files")
    # ensure unique items in list
    for file in files:
        # if ws exists (gui is actually runnng) temporarily add to file list to show that things are running
        if ws:
            fileList.insert("end", file)

        # then check if file is valid, read it, and hold onto its DF
        if not file in fileArray.keys():
            try:
                fileArray[file] = pp.preprocess(read_file(file))
            except ValueError as e:
                handle_fatal_error(e, file)
            except FileNotFoundError as e:
                handle_fatal_error(e, file)
    # Silent run Gatekeeping
    if not ws:
        return

    # reprint list
    fileList.delete(0, "end")
    for file in fileArray.keys():
        fileList.insert("end", file)


def read_file(file_path):
    """
    Reads a file and returns a pandas DataFrame.

    Args:
        file_path (str): The full path to the file.

    Returns:
        DataFrame: The loaded data.
    """
    if file_path.endswith('.csv'):
        return pp.auto_clip_datetime(pd.read_csv(file_path, encoding='latin1'))
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_path}")
    
def dumpRawData(df, type):
    print("Dumping Raw Data to Database")
    db.UpsertRaw(df, type)


def run():
    ws = createGui()
    ws.mainloop()


if __name__ == "__main__":
    ws = createGui()
    ws.mainloop()
