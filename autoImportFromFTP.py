import os
import json
import shutil
import gui  # assuming gui.py is in the same directory and its functions are refactored to be used here


def load_config(config_path=".\\data\\Lists\\emailMonitoring.json"):
    """Load configuration from the specified JSON file and filter for autoImportFromFTP processing."""
    with open(config_path, "r") as config_file:
        config = json.load(config_file)
        return [
            rule
            for rule in config.get("email_rules", [])
            if rule.get("processing") == "autoImportFromFTP"
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


def process_files(directory, file_types, move_on_success, move_on_failure):
    """Process files and move them based on the outcome."""
    files = find_files_in_directory(directory, file_types)
    for file_path in files:
        try:
            gui.addFiles([file_path])
            gui.insertRaw()
            gui.guiAnalyze()
            # If processing succeeds, move to move_on_success directory
            move_file(file_path, move_on_success)
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            # If processing fails, move to move_on_failure directory
            move_file(file_path, move_on_failure)


def main():
    config = load_config()  # Load the configuration

    for rule in config:
        directory = rule.get("folder_path")
        file_types = rule.get("attachment_type")
        move_on_success = rule.get("move_on_success")
        move_on_failure = rule.get("move_on_failure")

        # Process files and move them based on the outcome
        process_files(directory, file_types, move_on_success, move_on_failure)

    # # Update Dependency Tables
    gui.update_dependency_tables()


if __name__ == "__main__":
    main()
