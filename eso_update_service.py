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


def fetch_and_process_data():
    # Calculate time intervals
    end_time = datetime.now()
    start_time = end_time - timedelta(seconds=API_interval_time_in_seconds)

    query = construct_query(start_time, end_time)
    data = get_eso(query)
    if data:
        group_dfs = group_data(data)
        store_dfs(group_dfs)


def main():
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
