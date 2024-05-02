import logging
import json
import os
import paramiko
import shutil


def create_sftp_client(connection_name):
    """
    Create an SFTP client using SSH key authentication.
    """
    try:
        connection = get_sftp_settings(connection_name)
        print(f"Found sftp data: {connection}")
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
        print(f"Failed to create SFTP client: {e}")
        return None


def get_sftp_settings(connection_name):
    if connection_name:
        sftp_host = os.getenv(f"SFTP_{connection_name}_HOST")
        sftp_port = int(
            os.getenv(f"SFTP_{connection_name}_PORT", 22)
        )  # Default to port 22 if not specified
        sftp_username = os.getenv(f"SFTP_{connection_name}_USERNAME")
        sftp_key = os.getenv(f"SFTP_{connection_name}_KEY_PATH")

    return {
        "hostname": sftp_host,
        "port": sftp_port,
        "username": sftp_username,
        "key_path": sftp_key,
    }


def setup_logging(filename="default.log", base="..\\logs\\"):
    """Setup logging configuration."""
    full_log_path = os.path.abspath(os.path.join(base, filename))
    if not logging.getLogger().hasHandlers():  # Check if handlers already exist
        os.makedirs(base, exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
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
        config_file_location = ".\\data\\Lists\\emailMonitoring.json"
        with open(config_file_location, "r") as file:
            config = json.load(file)
        logging.info("Email monitoring configuration loaded")
        return config["email_rules"]
    except Exception as e:
        logging.error(f"Error loading email configuration: {e}")
        exit(1)


def load_config_for_process(
    process, config_path=".\\data\\Lists\\emailMonitoring.json"
):
    """Load configuration from the specified JSON file and filter for autoImportFromFTP processing."""
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
        return [
            rule
            for rule in config.get("email_rules", [])
            if rule.get("processing") == process
        ]


def find_files_in_directory(directory, file_types):
    """Load all files from the specified directory that match the given file types."""
    for filename in os.listdir(directory):
        if filename.endswith(file_types):
            yield os.path.join(directory, filename)


def move_file(file_path, target_directory):
    """Move the specified file to the target directory."""
    if not os.path.exists(target_directory):
        os.makedirs(target_directory)
    shutil.move(file_path, target_directory)
