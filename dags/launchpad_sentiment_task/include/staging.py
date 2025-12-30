import logging
from pathlib import Path
import pandas as pd
import os
import hashlib
from launchpad_sentiment_task.include.construct_pageview import construct_pageviews_url
from airflow.sdk import Variable

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# filtered symbols to a CSV file
output_dir = "/opt/airflow/dags/launchpad_sentiment_task/data/staging/"

# Ensure output directory exists
Path(output_dir).mkdir(parents=True, exist_ok=True)

def data_source_info(df, year, month, day, hour):
    """
    Add source URL and filename information to the DataFrame.
    """

    # Get download URL and expected filename
    url, filename = construct_pageviews_url(year, month, day, hour)

    df.insert(0, "source_file_name", filename)

def hash_row(row):
    return hashlib.sha256(
        "|".join(row.astype(str)).encode()
    ).hexdigest()


def staging_wrapper(**kwargs):    
    """
    Wrapper function to add source info and row_id to the filtered symbols DataFrame.
    This function can be used as a PythonOperator callable in Airflow.
    """
    # Define the date and hour for which to download the pageviews
    test_date = Variable.get("test_date", deserialize_json=True)
    year = test_date["year"]
    month = test_date["month"]
    day = test_date["day"]
    hour = test_date["hour"]

    # Define to save staged symbols
    output_path = os.path.join(output_dir, f"staging_symbols_{year}_{month}_{day}_{hour}.csv")

    # Read the filtered symbols CSV file
    input_dir = "/opt/airflow/dags/launchpad_sentiment_task/data/processed/"
    input_path = os.path.join(input_dir, f"filtered_symbols_{year}_{month}_{day}_{hour}.csv") 

    df = pd.read_csv(input_path)

    # add source info
    data_source_info(df, year, month, day, hour)

    # generate hashes
    row_ids = df[["project", "symbol", "views", "size"]].apply(hash_row, axis=1)

    # insert at index 0
    df.insert(0, "unique_id", row_ids)

    # Save the updated DataFrame back to CSV
    df.to_csv(output_path, index=False)
    logger.info(f"Staged data saved to {output_path}")

    # Load staged data into Postgres
    #load_staged_data_to_postgres(output_path)
