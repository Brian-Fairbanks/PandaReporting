# EMS is going to be particularly problematic, because the entire contents are "ESD02 Dataset", which is included in the "Incidents - ESD02 Dataset - Daily"
# potentially filter on lack of "daily"

import pandas as pd
import analyzefire as af
import logging
import ServerFiles as sf
from Database import SQLDatabase
import gui


def get_time_frame(df, data_source):
    """
    take the passed timeframe, and get the earliest/last incident PhonePickupTimes from them
    Make sure that these times actually extend out all the way to 00:00, assuming this adds a bit before the first, and a bit after the last.
    return the file as a dict {start, end}
    """
    # Choose the correct column based on data source
    if data_source == "ems":
        time_column = "Ph_PU_Date"
    else:  # Default to fire incidents if not EMS
        time_column = "Earliest Time Phone Pickup AFD or EMS"

    # Ensure the column is in datetime format
    df[time_column] = pd.to_datetime(df[time_column])

    # Find the earliest and latest times
    start_time = df[time_column].min()
    end_time = df[time_column].max()

    # Adjust to start and end of the respective days
    start_time = start_time.normalize()  # This sets the time to 00:00:00
    end_time = (end_time + pd.Timedelta(days=1)).normalize() - pd.Timedelta(
        seconds=1
    )  # This sets the time to 23:59:59 of the same day

    return {"start": start_time, "end": end_time}


def analyze_weekly(file):
    return af.analyzeFire(file)


def get_from_database(time_frame, data_source):
    "Grab data from the database from"
    fire_query = [
        f"Select * from RawFire where [Earliest Time Phone Pickup AFD or EMS] > '{time_frame['start']}' and [Earliest Time Phone Pickup AFD or EMS] <= '{time_frame['end']}'",
        ["Earliest Time Phone Pickup AFD or EMS"],
    ]
    ems_query = [
        f"Select * from RawEMS where Ph_PU_Date > '{time_frame['start']}' and Ph_PU_Date <= '{time_frame['end']}'",
        ["Ph_PU_Date"],
    ]

    args = ems_query if data_source == "ems" else fire_query

    try:
        db = SQLDatabase()
        df = db.retreiveDF(*args)  # unpack a set of parameters
    except Exception as e:
        print(e)
    return df


def compare_file(from_file_df, from_db_df):
    """
    Compare our data from the processed weekly file against the data that already exists in the database.
    Create 2 separate lists for data to update, and data that needs to be inserted
    """
    update = []
    insert = []

    # Fill in the blanks

    return {update, insert}


def process_comparison(file_path):
    df, data_source = gui.readRaw(file_path)
    time_frame = get_time_frame(df, data_source)
    database_df = get_from_database(time_frame, data_source)
    dfs = [df, database_df]

    from pandasgui import show

    show(*dfs)
    # compare_file(df, database_df)


def process_directory(directory, file_types, move_on_success, move_on_failure):
    files = list(sf.find_files_in_directory(directory, file_types))
    print(f"Files Found:{list(files)}")

    for file_path in files:
        print(f"Beginning Processing for file: {file_path}")
        process_comparison(file_path)


def main():
    sf.setup_logging("..\\logs\\Comparison.log")
    config = sf.load_config_for_process("WeeklyComparison")

    for rule in config:
        directory = rule.get("folder_path")
        file_types = rule.get("attachment_type")
        move_on_success = rule.get("move_on_success")
        move_on_failure = rule.get("move_on_failure")

        print(
            f"\n\n===========================================\nBeginning Processing for Directory : {directory}\n===========================================\n\n"
        )

        try:
            process_directory(directory, file_types, move_on_success, move_on_failure)
        except Exception as e:
            logging.error(f"Error processing email for rule {rule}: {e}")


if __name__ == "__main__":
    main()
