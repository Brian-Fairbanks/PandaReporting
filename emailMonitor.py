from datetime import datetime
import logging
import os
from dotenv import load_dotenv, find_dotenv
import imaplib
import email
from email.policy import default
import ServerFiles as sf


def login_to_email():
    load_dotenv(find_dotenv())
    email_account = os.getenv("MONEMAIL")
    password = os.getenv("MONPASS")
    try:
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(email_account, password)
        mail.select('"[Gmail]/All Mail"')
        # mail.select('"Inbox"')
        logging.info("Logged into email account")
        return mail
    except imaplib.IMAP4.error as e:
        logging.error(f"Email login failed: {e}")
        exit(1)


# ##############################################################################################################################################
#     Main Code
# ##############################################################################################################################################

# # Leaving as is for future development
# def find_matching_emails(mail, rule):
#     criteria = '(FROM "{}" SUBJECT "{}")'.format(rule["sender"], rule["subject_keyword"])
#     status, messages = mail.search(None, criteria)
#     if status == "OK":
#         logging.info(f"Found {len(messages[0].split())} messages with criteria: {criteria}")
#         return messages[0].split()  # Return a list of message IDs
#     else:
#         logging.warning(f"No messages found for {criteria}")
#         return []

# def process_matched_emails(mail, message_ids, rule):
#     for num in message_ids:
#         _, data = mail.fetch(num, "(RFC822)")
#         email_msg = email.message_from_bytes(data[0][1], policy=default)
#         save_attachments(email_msg, rule)


def find_first_matching_email(mail, rule):
    criteria = '(FROM "{}" SUBJECT "{}")'.format(
        rule["sender"], rule["subject_keyword"]
    )
    status, messages = mail.search(None, criteria)
    if status == "OK" and messages[0]:  # Check if there's at least one match
        message_ids = messages[0].split()
        message_ids.sort(reverse=True)  # Sort so the most recent message is first
        return message_ids[0]  # Return only the first (most recent) message ID
    else:
        logging.warning(f"No messages found for {criteria}")
        return None


def process_first_email(mail, message_id, rule):
    if message_id is None:
        logging.info("No matching email to process.")
        return

    _, data = mail.fetch(message_id, "(RFC822)")
    email_msg = email.message_from_bytes(data[0][1], policy=default)
    save_attachments(email_msg, rule)


def format_email_date(date_string):
    date_tuple = email.utils.parsedate_tz(date_string)
    if date_tuple:
        local_date = datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
        return local_date.strftime("%y.%m.%d")  # Format the date as 'YY.MM.DD.'
    else:
        return "unknown_date"


def save_attachments(email_msg, rule):
    log_file = ".\\data\\downloaded_files_log.txt"  # Define the log file path
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

            filepath = os.path.join(folder_path, new_filename)

            if not is_file_logged(log_file, new_filename, sender, email_msg["Date"]):
                with open(filepath, "wb") as f:
                    f.write(part.get_payload(decode=True))
                logging.info(f"Saved file to {filepath}")
                log_downloaded_file(log_file, new_filename, sender, email_msg["Date"])
            else:
                logging.info(f"Skipped {new_filename}, already processed.")


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
    sf.setup_logging("EmailMonitor")
    email_rules = sf.load_config()
    mail = login_to_email()

    for rule in email_rules:
        try:
            message_id = find_first_matching_email(mail, rule)
            process_first_email(mail, message_id, rule)
        except Exception as e:
            logging.error(f"Error processing email for rule {rule}: {e}")


if __name__ == "__main__":
    main()
