import ServerFiles as sf
logger = sf.setup_logging("Comparison.log", debug=False)

import pandas as pd
import analyzefire as af
from Database import SQLDatabase
import Email_Report as er
from datetime import timedelta, datetime
import gui
from pandasgui import show
import preprocess as pp
import os
import traceback
import copy

def get_time_frame(df, data_source):
    """
    take the passed timeframe, and get the earliest/last incident PhonePickupTimes from them
    Make sure that these times actually extend out all the way to 00:00, assuming this adds a bit before the first, and a bit after the last.
    return the file as a dict {start, end}
    """
    # Choose the correct column based on data source
    if data_source == "ems":
        time_column = "Ph_PU_Date"
    else:
        time_column = "Earliest Time Phone Pickup AFD or EMS"

    df[time_column] = pd.to_datetime(df[time_column])

    start_time = df[time_column].min().normalize()
    end_time = (df[time_column].max() + pd.Timedelta(days=1)).normalize() - pd.Timedelta(seconds=1)

    return {"start": start_time, "end": end_time}

def analyze_weekly(file):
    return af.analyzeFire(file)

def get_from_database(time_frame, data_source):
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
        df = db.retrieve_df(*args)
    except Exception as e:
        logger.error(f'Error grabbing data from database: {e}')
        df = pd.DataFrame()  # Return an empty DataFrame if there's an error
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
    if data_source == "ems":
        compare_keys = ["Incident", "Unit", "Assigned"]
        closed_time_column = "Closed_Time"
    elif data_source == "fire":
        from_file_df.drop(columns=["Latitude_At_Assign_Time", "Longitude_At_Assign_Time"], errors='ignore', inplace=True)
        compare_keys = ["Master_Incident_Number", "Radio_Name", "Unit Time Assigned"]
        closed_time_column = "Incident Time Call Closed"
    else:
        raise ValueError(f"Unknown data_source: {data_source}")

    renames = {"Alarm_Level": "Alarm Level"}
    from_file_df.rename(columns=renames, errors="ignore", inplace=True)

    if data_source == "ems":
        pp.round_datetime_columns(from_db_df)
        pp.scrub_raw_ems(from_file_df)
        from_file_df["Zip"] = from_file_df["Zip"].astype(str).replace("<NA>", None, regex=False)
        from_file_df["Destination_Zip"] = from_file_df["Destination_Zip"].astype(str).replace("<NA>", None, regex=False)

    compare_df = from_file_df.copy()

    compare_df.fillna("null", inplace=True)
    from_db_df.fillna("null", inplace=True)


    compare_df[compare_keys] = compare_df[compare_keys].astype(str)
    from_db_df[compare_keys] = from_db_df[compare_keys].astype(str)

    non_key_columns = [
        col for col in compare_df.columns
        if col not in compare_keys
        and col not in ["index", "Master Incident Without First Two Digits"]
    ]

    db_records = {
        tuple(row[k] for k in compare_keys): row for _, row in from_db_df.iterrows()
    }

    update = []
    insert = []
    update_changes = []

    for _, new_row in compare_df.iterrows():
        new_record_key = tuple(new_row[k] for k in compare_keys)
        if new_record_key in db_records:
            existing_record = db_records[new_record_key]
            changed_cols = {
                col: f"{new_row[col]} != {existing_record[col]}"
                for col in non_key_columns
                if new_row[col] != existing_record[col]
            }
            if changed_cols:
                update.append(new_row.name)
                change_detail = ", ".join([f"{col}: {new_row[col]} != {existing_record[col]}" for col in changed_cols])
                update_changes.append(change_detail)
        else:
            insert.append(new_row.name)

    update_df = from_file_df.loc[update, compare_keys].copy()
    update_df["Changes"] = update_changes

    insert_columns = compare_keys + [closed_time_column]
    insert_df = from_file_df.loc[insert, insert_columns].copy()

    return {"update": update_df, "insert": insert_df}

def process_comparison(file_path):
    df, data_source = gui.readRaw(file_path)
    time_frame = get_time_frame(df, data_source)
    database_df = get_from_database(time_frame, data_source)
    # dfs = [df, database_df]
    # show(*dfs)  # Uncomment to use pandasgui for visualization if available

    dfs = compare_file(df, database_df, data_source)
    # for dftype, df_to_apply in dfs.items():
    #     apply_compared_corrections_to_database(df_to_apply, dftype, data_source)

    # Insert the entire weekly file
    try:
        apply_compared_corrections_to_database(df, 'insert', data_source)
        email_compare_results(dfs, time_frame, data_source, success=True)
    except Exception as e:
        logger.error(f'Error during processing: {e}')
        email_compare_results(dfs, time_frame, data_source, success=False)

def email_compare_results(dfs, time_frame, data_source, success=True):
    logger.info("Sending DFS By Email")
    start_date = time_frame['start'].strftime("%m/%d/%y")
    end_date = time_frame['end'].strftime("%m/%d/%y")
    subject_prefix = "Failed: " if not success else ""
    subject = f"{subject_prefix}Comparison Report: {data_source.upper()} {start_date} - {end_date}"

    config = copy.deepcopy(er.email_config)
    config.update({
        "recipient_emails": "bfairbanks@pflugervillefire.org",
        "cc_emails": "",
        "subject": subject,
        "Email_Body": "The comparison was successful. Please find the dataframes attached." if success else "The comparison completed, but the insert has failed"
    })

    er.send_email_with_dataframes(dfs, config)

    logger.info("Email Sent")

def process_directory(directory, file_types, move_on_success, move_on_failure):
    if not os.path.isdir(directory):
        logger.error(f"The specified directory {directory} does not exist.")
        return

    files = list(sf.find_files_in_directory(directory, file_types))
    logger.debug(f"Files Found: {files}")

    for file_path in files:
        logger.debug(f"Beginning Processing for file: {file_path}")
        try:
            process_comparison(file_path)
            try:
                sf.move_file(file_path, move_on_success)
                logger.info(f"File successfully processed and moved: {file_path}")
            except Exception as e:
                logger.error(f"Failed to move file {file_path} to success directory: {e}")
        except Exception as e:
            tb = traceback.format_exc()
            logger.error(f"Processing failed for file {file_path}, error: {e}:\n\t {tb}\n")
            try:
                sf.move_file(file_path, move_on_failure)
                logger.info(f"File moved to failure directory: {file_path}")
            except Exception as move_error:
                logger.error(f"Failed to move file {file_path} to failure directory: {move_error}")


def apply_compared_corrections_to_database(df, operation_type, source_type):
    """
    Process a DataFrame either for insert or update.
    Args:
        df (pd.DataFrame): The DataFrame to process.
        operation_type (str): Type of operation ('insert' or 'update').
    """
    print(f"Processing {operation_type} DataFrame")
    
    if df.empty:
        logger.info(f"{operation_type} skipped due to Empty Frame")
        return

    try:
        ppdf = pp.preprocess(df)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Error Preprocessing Data: {e}\nTraceback: {tb}")
        exit()
    logger.debug("Finished Pre-Processing Data")
    try:
        analyzed_df = af.analyzeFire(ppdf)
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"Error applying data corrections: {e}\nTraceback: {tb}")
        exit()

    if operation_type == 'insert':
        logger.info(f'Insert this to database!')
        db.UpsertRaw(df, source_type)
        db.new_insert_DF(analyzed_df, source_type)
    elif operation_type == 'update':
        logger.info(f'Update into to database!')
        db.UpsertRaw(df, source_type)
        db.new_insert_DF(analyzed_df, source_type)

    logger.info(f"Completed processing {operation_type} DataFrame")

def main():
    config = sf.load_config_for_process("WeeklyComparison")
    global db
    try:
        db = SQLDatabase()

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
    except Exception as e:
        logger.error(f"An error has occurred in Compare: {e}", exc_info=True)
    finally:
        if db is not None:
            db.close()
            logger.info("Closed Database Connection")

if __name__ == "__main__":
    main()
