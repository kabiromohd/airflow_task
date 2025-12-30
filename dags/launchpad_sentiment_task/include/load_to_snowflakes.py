import logging
import os
from airflow.sdk import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE = "SENTIMENT_DB"
SCHEMA = "PUBLIC"
TABLE = "SENTIMENT_DATA"
STAGE = "AIRFLOW_STAGE"


def load_staging_to_snowflakes(csv_path):
    """
    Direct, idempotent load into Snowflake with deduplication.
    No staging tables.
    """

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"{csv_path} not found")

    hook = SnowflakeHook(snowflake_conn_id="snowflakes_sentiment")
    conn = hook.get_conn()
    cur = conn.cursor()

    try:

        # -----------------------------
        # Stage (idempotent)
        # -----------------------------
        cur.execute(f"""
            CREATE STAGE IF NOT EXISTS {DATABASE}.{SCHEMA}.{STAGE}
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
            );
        """)

        # -----------------------------
        # Final table (logical PK)
        # -----------------------------
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.{TABLE} (
                unique_id STRING,
                source_file_name STRING,
                project STRING,
                symbol STRING,
                views INTEGER,
                size INTEGER,
                PRIMARY KEY (unique_id)
            );
        """)

        # -----------------------------
        # Upload file (idempotent)
        # -----------------------------
        cur.execute(
            f"""
            PUT file://{csv_path}
            @{DATABASE}.{SCHEMA}.{STAGE}
            OVERWRITE = FALSE
            """
        )

        # -----------------------------
        # MERGE directly from stage
        # -----------------------------
        cur.execute(f"""
            MERGE INTO {DATABASE}.{SCHEMA}.{TABLE} tgt
            USING (
                SELECT
                    t.$1 AS unique_id,
                    t.$2 AS source_file_name,
                    t.$3 AS project,
                    t.$4 AS symbol,
                    t.$5::INTEGER AS views,
                    t.$6::INTEGER AS size
                FROM @{DATABASE}.{SCHEMA}.{STAGE} t
            ) src
            ON tgt.unique_id = src.unique_id
            WHEN NOT MATCHED THEN
              INSERT (
                unique_id,
                source_file_name,
                project,
                symbol,
                views,
                size
              )
              VALUES (
                src.unique_id,
                src.source_file_name,
                src.project,
                src.symbol,
                src.views,
                src.size
              );
        """)

        logger.info("Direct Snowflake load completed successfully")

    finally:
        cur.close()
        conn.close()


def load_snowflakes_wrapper(**kwargs):
    """
    Airflow wrapper function.
    """
    test_date = Variable.get("test_date", deserialize_json=True)

    year = test_date["year"]
    month = test_date["month"]
    day = test_date["day"]
    hour = test_date["hour"]

    input_dir = "/opt/airflow/dags/launchpad_sentiment_task/data/staging/"
    input_path = os.path.join(
        input_dir, f"staging_symbols_{year}_{month}_{day}_{hour}.csv"
    )

    load_staging_to_snowflakes(input_path)