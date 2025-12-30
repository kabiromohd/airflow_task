import logging
from airflow.sdk import Variable

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def construct_pageviews_url(year, month, day, hour):
    """
    Build the Wikimedia pageviews URL and filename
    for a given date and hour.
    returns (url, filename)
    """
    # Base url for Wikimedia pageviews
    base_url = Variable.get("base_url")

    # Builds the filename
    filename = f"pageviews-{year}{month:}{day}-{hour}0000.gz"

    # Builds url for hourly dump file
    url = f"{base_url}/{year}/{year}-{month}/{filename}"

    logger.info("Constructed URL: ", url)
    return url, filename
