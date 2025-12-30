import os
import logging
from pathlib import Path
from pendulum import DateTime, datetime
from airflow.sdk import Variable
from launchpad_sentiment_task.include.construct_pageview import construct_pageviews_url


# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_pageviews(year, month, day, hour):
    """
    Download the Wikipedia pageviews file for a specific hour
    and save it locally.
    returns the path to the downloaded file
    """
    output_dir = "/opt/airflow/dags/launchpad_sentiment_task/data/raw/"

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Get download URL and expected filename
    url, filename = construct_pageviews_url(year, month, day, hour)
    output_path = os.path.join(output_dir, filename)
    unzipped_output_path = output_path.replace(".gz", ".csv")

    # If file already exists, skip download
    if os.path.exists(unzipped_output_path):
        size = os.path.getsize(unzipped_output_path)
        logger.info("Download File already exists: ", unzipped_output_path, size)
        logger.info("Skipping download.")
        return None

    logger.info("Downloading pageviews from: ", url)

    # Download file from url
    os.system(f'curl {url} -o {filename}')

    logger.info("Download complete. Extracting file...")
    # Download the file using gzip
    os.system(f"gzip -dc {filename} > {unzipped_output_path}")

def download_wrapper(**kwargs):
    """
    Wrapper function to download pageviews for a specific date and hour.
    This function can be used as a PythonOperator callable in Airflow.
    """
    # Define the date and hour for which to download the pageviews
    test_date = Variable.get("test_date", deserialize_json=True)
    year = test_date["year"]
    month = test_date["month"]
    day = test_date["day"]
    hour = test_date["hour"]

    download_pageviews(year, month, day, hour)