import logging
from pathlib import Path
import pandas as pd
import os
import re
from launchpad_sentiment_task.include.construct_pageview import construct_pageviews_url
from airflow.sdk import Variable

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def filter_symbols(year, month, day, hour):
    """
    Filter and return only the required symbols from the provided list.
    The required symbols are defined in an Airflow Variable named 'required_symbols'.
    """
    output_dir = "/opt/airflow/dags/launchpad_sentiment_task/data/processed/"

    # Ensure output directory exists
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    source_dir = "/opt/airflow/dags/launchpad_sentiment_task/data/raw/"
    
    # Get download URL and expected filename
    url, filename = construct_pageviews_url(year, month, day, hour)
    source_path = os.path.join(source_dir, filename)
    unzipped_source_path = source_path.replace(".gz", ".csv")

    symbols_list = pd.read_csv(unzipped_source_path, sep = " ", header=None)
    symbols_list.columns = ["project", "symbol", "views", "size"]

    required_symbols = Variable.get("required_symbols", deserialize_json=True)
    values_list = list(required_symbols.values())

    # Build regex pattern from keywords
    pattern = "|".join(map(re.escape, values_list))

    filtered_symbols = symbols_list[
        symbols_list["symbol"].str.contains(pattern, case=False, na=False)
    ]
    #filtered_symbols = symbols_list[symbols_list["symbol"].isin(values_list)]
    
    logger.info(f"Filtered symbols: {filtered_symbols}")

    # Save filtered symbols to a CSV file
    output_path = os.path.join(output_dir, f"filtered_symbols_{year}_{month}_{day}_{hour}.csv")
    filtered_symbols.to_csv(output_path, index=False)
    logger.info(f"Filtered symbols saved to {output_path}")


def filter_required_wrappper(**kwargs):
    """
    Wrapper function to filter required symbols.
    This function can be used as a PythonOperator callable in Airflow.
    """

    # Define the date and hour for which to download the pageviews
    test_date = Variable.get("test_date", deserialize_json=True)
    year = test_date["year"]
    month = test_date["month"]
    day = test_date["day"]
    hour = test_date["hour"]

    return filter_symbols(year, month, day, hour)