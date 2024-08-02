from datetime import datetime, timedelta
import os
from dotenv import load_dotenv, find_dotenv
import imaplib
import email
from email.policy import default
import ServerFiles as sf
from os import path
import shutil  # Import shutil for file copy operations

testing = False
logger = sf.setup_logging("EmailMonitor.log")

def login_to_email():
    load_dotenv(find_dotenv())
    email_account = os.getenv("MONEMAIL")
    password = os.getenv("MONPASS")
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(email_account, password)
        mail.select('"[Gmail]/All Mail"')
        logger.info("Logged into email account")
        return mail
    except imaplib.IMAP4.error as e:
        logger.error(f"Email login failed: {e}")
        exit(1)


# ##############################################################################################################################################
#     Main Code
# ##############################################################################################################################################

# # Leaving as is for future development
# def find_matching_emails(mail, rule):
#     criteria = '(FROM "{}" SUBJECT "{}")'.format(rule["sender"], rule["subject_keyword"])
#     status, messages = mail.search(None, criteria)
#     if status == "OK":
#         logger.info(f"Found {len(messages[0].split())} messages with criteria: {criteria}")
#         return messages[0].split()  # Return a list of message IDs
#     else:
#         logger.warning(f"No messages found for {criteria}")
#         return []

# def process_matched_emails(mail, message_ids, rule):
#     for num in message_ids:
#         _, data = mail.fetch(num, "(RFC822)")
#         email_msg = email.message_from_bytes(data[0][1], policy=default)
#         save_attachments(email_msg, rule)


def open_sftp_client(rule):
    if testing:
        return None
    sftp_client = None
    if "sftp_copy" in rule:
        sftp_client = sf.create_sftp_client(rule["sftp_copy"])
        if sftp_client:
            logger.info("SFTP client created successfully")
        else:
            logger.error("Failed to create SFTP client")
    else:
        logger.info("No SFTP location found in emailMonitoring.json : sftp")
    return sftp_client


def transfer_file_via_sftp(sftp_client, local_path, remote_path):
    if sftp_client is None:
        logger.error("SFTP client is None, cannot transfer file")
        return

    if not path.exists(local_path):
        logger.error(f"Local file does not exist: {local_path}")
        return

    try:
        sftp_client.put(local_path, remote_path)
        logger.info(f"Successfully transferred {local_path} to {remote_path}")
    except Exception as e:
        logger.error(f"Failed to transfer {local_path} to {remote_path}: {e}")

def backup_file(local_path, backup_path):
    if not path.exists(local_path):
        logger.error(f"Local file does not exist: {local_path}")
        return

    try:
        shutil.copy(local_path, backup_path)
        logger.info(f"Successfully backed up {local_path} to {backup_path}")
    except Exception as e:
        logger.error(f"Failed to backup {local_path} to {backup_path}: {e}")

def find_matching_emails(mail, rule, date_range=None, get_most_recent=False):
    criteria = f'FROM "{rule["sender"]}" SUBJECT "{rule["subject_keyword"]}"'
    
    # Handle exclusions if provided
    if "excludes" in rule:
        for ex in rule["excludes"]:
            criteria += f' NOT SUBJECT "{ex}"'

    # Add date range criteria
    if date_range:
        start_date, end_date = date_range
        date_criteria = f'SINCE "{start_date.strftime("%d-%b-%Y")}"'
        criteria = f'({criteria} {date_criteria})'  # Combine all parts with AND implicitly

    logger.debug(f"Final search criteria: {criteria}")

    # Execute search
    status, messages = mail.search(None, criteria)
    if status == "OK" and messages[0]:
        message_ids = messages[0].split()
        if get_most_recent:
            message_ids.sort(reverse=True)  # Sort to get the most recent first
            return message_ids[0]  # Return only the most recent if specified
        return message_ids
    else:
        logger.warning(f"No messages found for {criteria}")
        return []


def process_single_email(mail, message_id, rule, sftp):
    if message_id is None:
        logger.info("No matching email to process.")
        return

    _, data = mail.fetch(message_id, "(RFC822)")
    email_msg = email.message_from_bytes(data[0][1], policy=default)
    file_data = save_attachments(email_msg, rule)
    if file_data:
        local_path = file_data["file_path"]
        remote_path = f".\\{file_data['file_name']}"
        
        # Transfer file via SFTP
        if sftp:
            transfer_file_via_sftp(sftp, local_path, remote_path)
        
        # Backup file if backup_location is specified in the rule
        if "backup_location" in rule:
            backup_file(local_path, path.join(rule["backup_location"], file_data['file_name']))
    else:
        logger.error(f"No file data to transfer: {file_data}")

def format_email_date(date_string):
    date_tuple = email.utils.parsedate_tz(date_string)
    if date_tuple:
        local_date = datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
        return local_date.strftime("%y.%m.%d")
    else:
        return "unknown_date"


def save_attachments(email_msg, rule):
    """
    Collects files if they have not already been collected, and saves them.
    Returns a dictionary of {file_path, file_name} if the file was saved, and None if the file skipped
    """
    base_dir = sf.get_base_dir()
    log_file = path.join(base_dir, "data", "downloaded_files_log.txt")
    sender = email_msg["From"]
    date_str = format_email_date(email_msg["Date"])

    for part in email_msg.walk():
        if (
            part.get_content_maintype() == "multipart"
            or part.get("Content-Disposition") is None
        ):
            continue

        filename = part.get_filename()
        if filename and filename.endswith(rule["attachment_type"]):
            # Insert date_str before the file extension
            name, extension = os.path.splitext(filename)
            new_filename = f"{name} {date_str}{extension}"

            folder_path = rule["folder_path"]
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            filepath = path.join(folder_path, new_filename)

            if not is_file_logged(log_file, new_filename, sender, email_msg["Date"]):
                with open(filepath, "wb") as f:
                    f.write(part.get_payload(decode=True))
                logger.info(f"Saved file to {filepath}")
                log_downloaded_file(log_file, new_filename, sender, email_msg["Date"])
                return {"file_path": filepath, "file_name": new_filename}
            else:
                logger.info(f"Skipped {new_filename}, already processed.")
                return None

def log_downloaded_file(log_file, filename, sender, date):
    with open(log_file, "a") as file:
        file.write(f"{filename};| {sender};| {date}\n")

def is_file_logged(log_file, filename, sender, date):
    try:
        with open(log_file, "r") as file:
            for line in file:
                logged_filename, logged_sender, logged_date = line.strip().split(";| ")
                if (
                    filename == logged_filename
                    and sender == logged_sender
                    and date == logged_date
                ):
                    return True
    except FileNotFoundError:
        # Log file doesn't exist, so the file hasn't been logged before
        return False
    return False


def main():
    email_rules = sf.load_config()
    mail = login_to_email()
    print(f"Beginning rule creation")
    for rule in email_rules:
        try:
            print(f'\n--  {rule["subject_keyword"]}  --\n')
            sftp = open_sftp_client(rule)
            logger.info(f"sftp: {sftp}")
            end_date = datetime.now()
            start_date = end_date - timedelta(days=26)
            message_ids = find_matching_emails(mail, rule, date_range=(start_date, end_date))

            for message_id in message_ids:
                process_single_email(mail, message_id, rule, sftp)
            if sftp:
                sftp.close()
        except Exception as e:
            logger.error(f"Error processing email for rule {rule}: {e}")


if __name__ == "__main__":
    main()
