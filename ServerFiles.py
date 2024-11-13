import logging
import json
import os
import paramiko
import shutil
import sys
from os import path
from datetime import datetime

def create_sftp_client(connection_name):
    """
    Create an SFTP client using SSH key authentication.
    """
    try:
        connection = get_sftp_settings(connection_name)
        logging.info(f"Found sftp data: {connection}")
        # Initialize an SSH client
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # Load the private key
        mykey = paramiko.RSAKey.from_private_key_file(connection["key_path"])

        # Connect using the loaded key
        client.connect(
            hostname=connection["hostname"],
            port=connection["port"],
            username=connection["username"],
            pkey=mykey,
        )
        sftp = client.open_sftp()
        return sftp
    except Exception as e:
        logging.error(f"Failed to create SFTP client in create_sftp_client(): {e}", exc_info=True)
        return None


def get_sftp_settings(connection_name):
    if connection_name:
        sftp_host = os.getenv(f"SFTP_{connection_name}_HOST")
        sftp_port = int(
            os.getenv(f"SFTP_{connection_name}_PORT", 22)
        )  # Default to port 22 if not specified
        sftp_username = os.getenv(f"SFTP_{connection_name}_USERNAME")
        sftp_key = os.getenv(f"SFTP_{connection_name}_KEY_PATH")

        if not sftp_host or not sftp_username or not sftp_key:
            logging.error(f"Missing SFTP configuration for connection: {connection_name}")
            logging.error(f"Hostname: {sftp_host}, Username: {sftp_username}, Key Path: {sftp_key}")
        
        return {
            "hostname": sftp_host,
            "port": sftp_port,
            "username": sftp_username,
            "key_path": sftp_key,
        }
    else:
        logging.error("Connection name is missing.")
        return {}


def setup_logging(filename="default.log", base="..\\logs\\", debug=False):
    """Setup logging configuration."""
    if debug:
        loglevel=logging.DEBUG
    else:
        loglevel=logging.INFO
    full_log_path = os.path.abspath(os.path.join(base, filename))
    if not logging.getLogger().hasHandlers():  # Check if handlers already exist
        os.makedirs(base, exist_ok=True)
        logging.basicConfig(
            level=loglevel,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(full_log_path),
                logging.StreamHandler(),  # Optionally add console output
            ],
        )
    logging.info(f"Logging initialized: {full_log_path}")
    return logging.getLogger(filename)


# def setup_logging(filename, base="..\\logs\\"):
#     full_log_path = os.path.join(base, filename)
#     logging.basicConfig(
#         level=logging.INFO,
#         format="%(asctime)s - %(levelname)s - %(message)s",
#         handlers=[
#             logging.FileHandler(full_log_path),
#             logging.StreamHandler(),
#         ],
#     )
#     logging.info(f"Log file enabled at {full_log_path}")


def load_config():
    try:
        base_dir = get_base_dir()
        config_file_location = path.join(base_dir, "data", "Lists", "emailMonitoring.json")
        with open(config_file_location, "r") as file:
            config = json.load(file)
        logging.info("Email monitoring configuration loaded")
        if "email_rules" not in config:
            raise ValueError("Invalid configuration: 'email_rules' key not found")
        return config["email_rules"]
    except Exception as e:
        logging.error(f"Error loading email configuration: {e}")
        sys.exit(1)

def load_config_for_process(
    process, config_path="data\\Lists\\emailMonitoring.json"
):
    """Load configuration from the specified JSON file and filter for autoImportFromFTP processing."""
    base_dir = get_base_dir()
    config_file_location = path.join(base_dir, config_path)
    with open(config_file_location, "r") as config_file:
        config = json.load(config_file)
        return [
            rule
            for rule in config.get("email_rules", [])
            if rule.get("processing") == process
        ]

def find_files_in_directory(directory, file_types):
    """Load all files from the specified directory that match the given file types."""
    base_dir = get_base_dir()
    directory_path = path.join(base_dir, directory)
    for filename in os.listdir(directory_path):
        if filename.endswith(file_types):
            yield path.join(directory_path, filename)

def move_file(file_path, target_directory):
    """Move the specified file to the target directory."""
    base_dir = get_base_dir()
    target_path = path.join(base_dir, target_directory)
    if not os.path.exists(target_path):
        os.makedirs(target_path)
    try:
        shutil.move(file_path, target_path)
    except Exception as e:
        logging.error(f"Error moving file: {e}")
        
        # Handle case where a file with the same name already exists
        if "already exists" in str(e):
            # Extract filename and extension from the file path (corrected)
            filename, extension = os.path.splitext(os.path.basename(file_path))

            # Create a unique filename with timestamp
            new_filename = f"{filename}({datetime.now().strftime('%y-%m-%d_%H.%M.%S')}){extension}"
            new_target_path = os.path.join(target_path, new_filename)

            logging.info(f"Renaming to {new_filename}\n original move: {target_path}\nNew Path: {new_target_path}")

            # Try moving with the unique filename
            try:
                shutil.move(file_path, new_target_path)
                return new_target_path
            except Exception as e2:
                logging.error(f"Failed to create unique filename: {e2}")
                return None  # Indicate failure

def get_base_dir():
    """Return the base directory for the application, whether bundled or not."""
    if getattr(sys, 'frozen', False) and hasattr(sys, '_MEIPASS'):
        # Running in a PyInstaller bundle
        return os.path.abspath(sys._MEIPASS)
    else:
        # Running in a normal Python environment
        return os.path.abspath(os.path.dirname(__file__))
    