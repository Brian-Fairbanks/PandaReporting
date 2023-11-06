import datetime
import os
import copy
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from dotenv import load_dotenv, find_dotenv
from os import getenv

from analyzefire import export_to_xlsx

import logging

# append log to end of runlog if possible, otherwise start a new file
try:
    logger = logging.getLogger(__name__)
except:
    # set up logging folder
    writePath = "../Logs"

    # logging setup - write to output file as well as printing visably
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger()
    logger.addHandler(
        logging.FileHandler(
            f"{writePath}/BC_Turnout_Report{(datetime.datetime.now()).strftime('%y-%m-%d_%H-%M')}.log",
            "a",
        )
    )

print = logger.info

# ====  Default Email Configuration  ====
load_dotenv(find_dotenv())
email = getenv("SNDRMAIL")
epass = getenv("SNDRPASS")

email_config = {
    "sender_email": email,
    "sender_password": epass,
    "smtp_server": "smtp.gmail.com",
    "smtp_port": 587,
}


# =================================================================
#      Data Aquisition
# =================================================================


def getFormattedTable(query, times):
    from Database import SQLDatabase

    try:
        db = SQLDatabase()
        df = db.retreiveDF(
            query,
            times,
        )
        return df

    except:
        print(
            "  - Process Failed!  - Error in Database Extraction - Please check the logs."
        )
        logging.exception("Exception found in Database Extraction")
        exit(1)


# =================================================================
#      Emailing
# =================================================================


def send_email_with_attachment(file_path, email_config):
    msg = MIMEMultipart()
    msg["From"] = email_config["sender_email"]
    msg["To"] = email_config["recipient_emails"]
    msg["CC"] = email_config["cc_emails"]
    msg["Subject"] = email_config["subject"]

    # Add the email body
    body_text = email_config.get("Email_Body", "")
    body = MIMEText(body_text, "plain")
    msg.attach(body)

    # Attach the report file
    with open(file_path, "rb") as file:
        part = MIMEApplication(file.read(), Name=os.path.basename(file_path))
        part[
            "Content-Disposition"
        ] = f'attachment; filename="{os.path.basename(file_path)}"'
        msg.attach(part)

    # Connect to the SMTP server and send the email
    try:
        smtp_server = smtplib.SMTP(
            email_config["smtp_server"], email_config["smtp_port"]
        )
        smtp_server.starttls()
        smtp_server.login(email_config["sender_email"], email_config["sender_password"])
        smtp_server.sendmail(
            email_config["sender_email"],
            email_config["recipient_emails"].split(","),
            msg.as_string(),
        )
        smtp_server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print("Error sending email:", e)


# =================================================================
#      Reports
# =================================================================


# def send_BC_Report():
#     # ====  Set up email configurations  ====
#     current_date = datetime.datetime.now().strftime("%m/%d/%y")
#     config = copy.deepcopy(email_config)
#     config.update(
#         {
#             "recipient_emails": "bfairbanks@pflugervillefire.org",
#             "cc_emails": "mnyland@pflugervillefire.org,vgonzales@pflugervillefire.org",
#             "subject": f"Weekly Turnout Data {current_date}",
#         }
#     )

#     # ====  Grab report from SQL  ====
#     bc_turnout_query = "SELECT * FROM [UNIT_RUN_DATA].[dbo].[v_BC_Turnout]"
#     times = [
#         "Phone_Pickup_Time",
#     ]

#     turnout_report = getFormattedTable(bc_turnout_query, times)
#     report_file = export_to_xlsx("BC_Turnout", turnout_report)

#     # ====  Trigger email  ====
#     send_email_with_attachment(report_file, config)


def read_rpt_file(filename):
    details = {}
    with open(filename, "r") as file:
        lines = file.readlines()
        key = None
        value = ""
        for line in lines:
            line = line.strip()
            if ": " in line:
                # Save the previous key-value pair if it exists
                if key is not None:
                    details[key] = value
                    value = ""
                key, value = line.split(": ", 1)
            elif line.endswith(":"):  # Key with no value scenario
                if key is not None:  # Save the previous key-value pair
                    details[key] = value
                key = line[:-1]
                value = ""
            elif key:  # Multi-line value scenario
                value += " " + line
            else:
                print(f"Unexpected line format: {line}")
        if key is not None:  # For the last key-value pair in the file
            details[key] = value
    return details


def send_report_from_file(filename):
    # Read the details from the .rpt file
    details = read_rpt_file(filename)

    # Set up email configurations
    current_date = datetime.datetime.now().strftime("%m/%d/%y")
    config = copy.deepcopy(email_config)
    config.update(
        {
            "recipient_emails": details["recipient_emails"],
            "cc_emails": details["cc_emails"],
            "subject": f"{details['subject']} {current_date}",
            "Email_Body": details["Email_Body"],
        }
    )

    # Grab report from SQL
    turnout_report = getFormattedTable(
        details["query"], details["date_time_columns"].split(",")
    )
    report_file = export_to_xlsx("Report", turnout_report)

    # Trigger email
    send_email_with_attachment(report_file, config)


def get_and_run_reports():
    print("\n\n==================  Emailing Reports  ==================")

    # Get the current directory
    current_directory = os.getcwd()

    # Search for 'reports' folder in the current directory and its subdirectories
    for root, dirs, files in os.walk(current_directory):
        if "reports" in dirs:
            reports_directory = os.path.join(root, "reports")
            break
    else:
        print("Couldn't find the 'reports' folder.")
        return

    # Get a list of all .rpt files
    rpt_files = [f for f in os.listdir(reports_directory) if f.endswith(".rpt")]

    # For each .rpt file, send the report
    for rpt_file in rpt_files:
        file_path = os.path.join(reports_directory, rpt_file)
        print(f" - Running {rpt_file} -")
        send_report_from_file(file_path)


def main():
    # send_BC_Report()
    # send_report_from_file("./reports/BC_Turnout.rpt")
    get_and_run_reports()


if __name__ == "__main__":
    main()
