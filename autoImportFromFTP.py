import ServerFiles as sf
logger = sf.setup_logging('AutoImport.log')
import gui  # assuming gui.py is in the same directory and its functions are refactored to be used here


def process_files(directory, file_types, move_on_success, move_on_failure):
    """Process files and move them based on the outcome."""
    files = list(sf.find_files_in_directory(directory, file_types))
    print(f"Files Found:{list(files)}")

    for file_path in files:
        print(f"Beginning Processing for file: {file_path}")
        print(gui.fileArray)
        try:
            gui.addFiles([file_path])
            gui.insertRaw()
            gui.guiAnalyze()
            # If processing succeeds, move to move_on_success directory
            sf.move_file(file_path, move_on_success)
            print(f"Completed file: {file_path}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            # If processing fails, move to move_on_failure directory
            sf.move_file(file_path, move_on_failure)
        gui.remove_completed_files()


def main():
    config = sf.load_config_for_process("autoImportFromFTP")  # Load the configuration

    for rule in config:
        directory = rule.get("folder_path")
        file_types = rule.get("attachment_type")
        move_on_success = rule.get("move_on_success")
        move_on_failure = rule.get("move_on_failure")

        print(
            f"\n\n===========================================\nBeginning Processing for Directory : {directory}\n===========================================\n\n"
        )

        # Process files and move them based on the outcome
        process_files(directory, file_types, move_on_success, move_on_failure)

    # # Update Dependency Tables
    gui.update_dependency_tables()

    # send email
    import Email_Report as er
    er.get_and_run_reports()


if __name__ == "__main__":
    main()
