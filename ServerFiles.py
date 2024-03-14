import logging
import json
import os
import shutil


def setup_logging(filename, base="..\\logs\\"):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(base + filename),
            logging.StreamHandler(),
        ],
    )
    logging.info("Script started")


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
