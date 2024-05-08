# EMS is going to be particularly problematic, because the entire contents are "ESD02 Dataset", which is included in the "Incidents - ESD02 Dataset - Daily"
# potentially filter on lack of "daily"
import ServerFiles as sf
logger = sf.setup_logging("Comparison.log", debug=True)

import pandas as pd
import analyzefire as af
from Database import SQLDatabase
from datetime import timedelta
import gui
from pandasgui import show
import traceback



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
    logger.debug(f"Query: {args[0]}")
    try:
        db = SQLDatabase()
        df = db.retrieve_df(*args)  # unpack a set of parameters
    except Exception as e:
        logger.error(f'Error grabbing data from database: {e}')
    return df

def round_datetime_columns(df):
    # Iterate over each column in the DataFrame
    for column in df.columns:
        # Check if the column is a datetime type
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            # Round the datetime data to the nearest second
            df[column] = df[column].dt.round('S')
    return df

def convert_zip(df, zip_column):
    try:
        # Convert to integer first to avoid any floating point issues like '78787.0'
        df[zip_column] = pd.to_numeric(df[zip_column], errors='coerce').astype('Int64').astype(str)
    except Exception as e:
        logger.error(f"Error converting ZIP codes: {e}")
    return df

def compare_file(from_file_df, from_db_df, data_source):
    """
    Compare our data from the raw weekly file against the data that already exists in the database.
    Create 2 separate lists for data to update, and data that needs to be inserted
    the primary clusters should be:
        data_source "ems":[Incident]+[unit]+[assigned], where unit and assigned can be null
            {incident: 112233, unit: Eng201, assigned: null} != {incident: 112233, unit: Safe201, assigned: null}
            {incident: 112233, unit: Eng201, assigned: 2024/02/01 12:30:20.01} != {incident: 112233, unit: S01, assigned: 2024/02/01 12:39:53.84}
        data_source "fire": [Incident_Number]+[Unit]+[Unit_Assigned]
    """
    # Define comparison keys based on the data source
    if data_source == "ems":
        compare_keys = ["Incident", "Unit", "Assigned"]
    elif data_source == "fire":
        compare_keys = ["Master_Incident_Number", "Radio_Name", "Unit Time Assigned"]
    else:
        raise ValueError(f"Unknown data_source: {data_source}")

    # Normalize null values and ensure data types, especially for dates
    renames = {
        "Alarm Level": "Alarm_Level",
    }
    from_db_df.rename(columns=renames, errors="ignore", inplace=True)

    compare_df = from_file_df.copy()
    round_datetime_columns(from_db_df)
    round_datetime_columns(compare_df)

    compare_df.fillna("null", inplace=True)
    from_db_df.fillna("null", inplace=True)

    compare_df[compare_keys] = compare_df[compare_keys].astype(str)
    from_db_df[compare_keys] = from_db_df[compare_keys].astype(str)

    if data_source == "ems":
        compare_df["Zip"] = compare_df["Zip"].astype(str).str.replace(".0", "", regex=False)
        compare_df["Destination_Zip"] = compare_df["Destination_Zip"].astype(str).str.replace(".0", "", regex=False)
        
    # Prepare columns for update check (non-key columns)
    non_key_columns = [
        col
        for col in compare_df.columns
        if col not in compare_keys
        and col not in ["index", "Master Incident Without First Two Digits"]
    ]

    # Convert DB df to a dict for faster lookups
    db_records = {
        tuple(row[k] for k in compare_keys): row for _, row in from_db_df.iterrows()
    }

    update = []
    insert = []

    for _, new_row in compare_df.iterrows():
        new_record_key = tuple(new_row[k] for k in compare_keys)
        if new_record_key in db_records:
            existing_record = db_records[new_record_key]
            # Identify changed columns
            changed_cols = {
                col: f"{new_row[col]} != {existing_record[col]}"
                for col in non_key_columns
                if new_row[col] != existing_record[col]
            }
            if changed_cols:
                update.append(new_row.name)  # Store index of row to update
                logger.info(f"changed_columns: {changed_cols}:")
        else:
            insert.append(new_row.name)  # Store index of row to insert

    # Filter original DataFrame 
    update_df = from_file_df.loc[update].copy()
    insert_df = from_file_df.loc[insert].copy()

    return {"update": update_df, "insert": insert_df}


def process_comparison(file_path):
    df, data_source = gui.readRaw(file_path)
    time_frame = get_time_frame(df, data_source)
    database_df = get_from_database(time_frame, data_source)

    # Optionally visualize the initial and database dataframes
    # dfs = [df, database_df]
    # show(*dfs)  # Uncomment to use pandasgui for visualization if available

    # Compare the file's DataFrame with the database's DataFrame
    dfs = compare_file(df, database_df, data_source)
    
    # Optionally visualize the comparison results
    # show(**dfs)

    # Apply corrections to the database based on comparison results
    for dftype, df_to_apply in dfs.items():
        apply_compared_corrections_to_database(df_to_apply, dftype)



def process_directory(directory, file_types, move_on_success, move_on_failure):
    files = list(sf.find_files_in_directory(directory, file_types))
    logger.debug (f"Files Found:{list(files)}")

    for file_path in files:
        logger.debug(f"Beginning Processing for file: {file_path}")
        process_comparison(file_path)



def apply_compared_corrections_to_database(df, operation_type):
    """
    Process a DataFrame either for insert or update.
    Args:
        df (pd.DataFrame): The DataFrame to process.
        operation_type (str): Type of operation ('insert' or 'update').
    """
    print(f"Processing {operation_type} DataFrame")
    import preprocess as pp
    try:
        ppdf = pp.preprocess(df)
    except Exception as e:
        tb = traceback.format_exc()  # This captures the entire traceback as a string
        logger.error(f"Error Preprocessing Data: {e}\nTraceback: {tb}")
        exit()
    logger.debug("Finished Pre-Processing Data")
    try:
        analyzed_df = af.analyzeFire(ppdf)
    except Exception as e:
        # Log the error along with the traceback
        tb = traceback.format_exc()  # This captures the entire traceback as a string
        logger.error(f"Error applying data corrections: {e}\nTraceback: {tb}")
        exit()

    # Assuming a different function or additional steps are needed for insert vs. update
    if operation_type == 'insert':
        # db.insertDF(analyzed_df)  # Your method to insert data into the database
        logger.info(f'Insert this to database!')
        show(analyzed_df) 
    elif operation_type == 'update':
        # db.updateDF(analyzed_df)  # Your method to update data in the database
        logger.info(f'Update into to database!')
        show(analyzed_df) 

    logger.info(f"Completed processing {operation_type} DataFrame")

def main():
    config = sf.load_config_for_process("WeeklyComparison")

    for rule in config:
        directory = rule.get("folder_path")
        file_types = rule.get("attachment_type")
        move_on_success = rule.get("move_on_success")
        move_on_failure = rule.get("move_on_failure")

        logger.info(
            f"\n\n================================\nBeginning Processing for Directory : {directory}\n================================\n\n"
        )

        try:
            process_directory(directory, file_types, move_on_success, move_on_failure)
        except Exception as e:
            logger.error(f"Error processing email for rule: {rule}\n: {e}")


if __name__ == "__main__":
    main()
