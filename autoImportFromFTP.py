import os
import gui  # assuming gui.py is in the same directory and its functions are refactored to be used here


def find_files_in_directory(directory):
    """Load all files from the specified directory"""
    for filename in os.listdir(directory):
        if filename.endswith(".xlsx"):  # assuming Excel files, change this if needed
            yield os.path.join(directory, filename)


def main():
    directory = "C:\\Users\\bfairbanks\\Desktop\\testing"
    files = find_files_in_directory(directory)

    # skip the context menu, and use the files from the ftp folder
    gui.addFiles(files)

    print(gui.fileArray)
    # # insert raw files
    gui.insertRaw()

    # # Process Files
    gui.guiAnalyze()

    # # Update Dependency Tables
    gui.update_dependency_tables()


if __name__ == "__main__":
    main()
