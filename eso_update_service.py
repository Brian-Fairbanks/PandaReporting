import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import logging
from eso import construct_query, get_eso, group_data, store_dfs

# Initialize logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Time interval in minutes for fetching data
API_interval_time_in_seconds = 60
process_interval_in_seconds = 15
catch_up_max_day_increment = 10

# Log file for last update time
last_update_log = "eso_last_update_time.txt"


# ==========  Account periods of downtime  =============================


def get_last_update():
    try:
        with open(last_update_log, "r") as f:
            last_update_time = datetime.fromisoformat(f.read().strip())
    except FileNotFoundError:
        # If no log file exists, assume we're starting fresh from 10 days ago
        last_update_time = datetime.now() - timedelta(days=10)
    return last_update_time


def set_last_update(update_time):
    with open(last_update_log, "w") as f:
        f.write(update_time.isoformat())


def catch_up_data():
    last_update_time = get_last_update()
    current_time = datetime.now()
    if last_update_time + timedelta(minutes=1) < current_time:
        logging.info(f"Last Updated : {last_update_time} - Initializing Catchup Script")
    while last_update_time < current_time:
        start_time = last_update_time
        end_time = min(
            start_time + timedelta(days=catch_up_max_day_increment), current_time
        )
        logging.info(f"  processing: {start_time} - {end_time}")
        query = construct_query(start_time, end_time)
        data = get_eso(query)
        if data:
            group_dfs = group_data(data)
            store_dfs(group_dfs)
            last_update_time = end_time
            set_last_update(last_update_time)
        else:
            break
        time.sleep(1)  # Pause to mitigate API rate limit concerns


# ==========  Main Logic  ==============================================


def fetch_and_process_data():
    # This function now only handles data updates at regular intervals, assuming no large backlogs
    end_time = datetime.now()
    start_time = end_time - timedelta(seconds=API_interval_time_in_seconds)
    query = construct_query(start_time, end_time)
    data = get_eso(query)
    if data:
        group_dfs = group_data(data)
        store_dfs(group_dfs)


def main():
    catch_up_data()  # Perform catch-up first

    # Start regular scheduling
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        fetch_and_process_data, "interval", seconds=process_interval_in_seconds
    )
    scheduler.start()

    # Keep the script running
    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()


if __name__ == "__main__":
    main()
