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

import ServerFiles as sf
logger = sf.setup_logging("EmailReports.log")
import Email_Report as er
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
#      Data Acquisition
# =================================================================

def getFormattedTable(query, times):
    from Database import SQLDatabase

    try:
        db = SQLDatabase()
        df = db.retrieve_df(query, times)
        return df
    except Exception as e:
        print("  - Process Failed! Error in Database Extraction.")
        logging.exception("Exception in Database Extraction")
        exit(1)

# =================================================================
#      Emailing
# =================================================================

def send_email_with_attachment(file_path, email_config):
    msg = MIMEMultipart()
    msg["From"] = email_config["sender_email"]
    msg["To"] = email_config["recipient_emails"]
    msg["CC"] = email_config.get("cc_emails", "")
    msg["Subject"] = email_config["subject"]

    # Add the email body
    body_text = email_config.get("Email_Body", "")
    body = MIMEText(body_text, "plain")
    msg.attach(body)

    # Attach the report file
    with open(file_path, "rb") as file:
        part = MIMEApplication(file.read(), Name=os.path.basename(file_path))
        part["Content-Disposition"] = f'attachment; filename="{os.path.basename(file_path)}"'
        msg.attach(part)

    # Connect to the SMTP server and send the email
    try:
        smtp_server = smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"])
        smtp_server.starttls()
        smtp_server.login(email_config["sender_email"], email_config["sender_password"])
        smtp_server.sendmail(
            email_config["sender_email"],
            email_config["recipient_emails"].split(",") +
            email_config.get("cc_emails", "").split(","),
            msg.as_string(),
        )
        smtp_server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print("Error sending email:", e)

def send_email_with_dataframes(dataframes, email_config):
    msg = MIMEMultipart()
    msg["From"] = email_config["sender_email"]
    msg["To"] = email_config["recipient_emails"]
    msg["CC"] = email_config.get("cc_emails", "")
    msg["Subject"] = email_config["subject"]

    # Add the email body
    body_text = email_config.get("Email_Body", "Please find the data attached.")
    body = MIMEText(body_text, "plain")
    msg.attach(body)

    # Attach each dataframe as a CSV
    for name, df in dataframes.items():
        attachment = MIMEText(df.to_csv(index=False), 'csv')
        attachment.add_header('Content-Disposition', 'attachment',
                              filename=f'{name}.csv')
        msg.attach(attachment)

    # Connect to the SMTP server and send the email
    try:
        smtp_server = smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"])
        smtp_server.starttls()
        smtp_server.login(email_config["sender_email"], email_config["sender_password"])
        smtp_server.sendmail(
            email_config["sender_email"],
            email_config["recipient_emails"].split(",") +
            email_config.get("cc_emails", "").split(","),
            msg.as_string(),
        )
        smtp_server.quit()
        print("Email sent successfully.")
    except Exception as e:
        print("Error sending email:", e)

# =================================================================
#      Reports
# =================================================================

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
                    details[key] = value.strip()
                    value = ""
                key, value = line.split(": ", 1)
            elif line.endswith(":"):  # Key with no value scenario
                if key is not None:  # Save the previous key-value pair
                    details[key] = value.strip()
                key = line[:-1]
                value = ""
            elif key:  # Multi-line value scenario
                value += " " + line
            else:
                print(f"Unexpected line format: {line}")
        if key is not None:  # For the last key-value pair in the file
            details[key] = value.strip()
    return details

def should_run_today(days_to_run_str):
    if not days_to_run_str:
        return True  # If no days specified, run every day
    # Parse the days_to_run string into a list
    days_to_run = [day.strip().lower() for day in days_to_run_str.split(",")]
    # Get the current day abbreviation (Mon, Tue, Wed, etc.)
    current_day_abbr = datetime.datetime.now().strftime("%a").lower()
    # Return True if the current day is in the days_to_run list
    return current_day_abbr in days_to_run

def send_report_from_file(filename):
    # Read the details from the .rpt file
    details = read_rpt_file(filename)

    # Set up email configurations
    current_date = datetime.datetime.now().strftime("%m/%d/%y")
    config = copy.deepcopy(email_config)
    config.update(
        {
            "recipient_emails": details.get("recipient_emails", ""),
            "cc_emails": details.get("cc_emails", ""),
            "subject": f"{details.get('subject', '')} {current_date}",
            "Email_Body": details.get("Email_Body", ""),
        }
    )

    # Check if the report should run today
    if not should_run_today(details.get("days_to_run", "")):
        print(f"Skipping report: {filename}. Today is not a scheduled run day.")
        return  # Skip this report

    # Grab report from SQL
    date_time_columns = details.get("date_time_columns", "").split(",")
    date_time_columns = [col.strip() for col in date_time_columns if col.strip()]
    turnout_report = getFormattedTable(details.get("query", ""), date_time_columns)
    report_name = os.path.splitext(os.path.basename(filename))[0]
    report_file = export_to_xlsx(report_name, turnout_report)

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
    get_and_run_reports()

if __name__ == "__main__":
    main()
