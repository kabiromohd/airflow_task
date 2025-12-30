from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import datetime
from datetime import timedelta
from launchpad_sentiment_task.include.download import download_wrapper
from launchpad_sentiment_task.include.filter_required_symbols import filter_required_wrappper
from launchpad_sentiment_task.include.staging import staging_wrapper
from launchpad_sentiment_task.include.load_to_snowflakes import load_snowflakes_wrapper


default_args = {
    "owner": "Kabir Olawale Mohammed",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["kabirolawalemohammed@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="launchPadsentiment_pipeline",
    default_args=default_args,
    description="Download and analyze Wikipedia pageviews data for sentiment analysis",
    schedule=None,
    start_date=datetime(2025, 12, 1, 0, tz="UTC"),
    template_searchpath="/opt/airflow/dags/launchpad_sentiment_task/include",
    catchup=False
) as dag:

    # Task 1: Download pageviews data
    download_task = PythonOperator(
        task_id="download_pageviews",
        python_callable=download_wrapper,
    )

    # Task 2: Filter symbols
    filter_task = PythonOperator(
        task_id="filter_symbols",
        python_callable=filter_required_wrappper,
    )

    # Task 3: Stage data
    stage_task = PythonOperator(
        task_id="prepare_staging_data",
        python_callable=staging_wrapper,
    )

    # Task 4: Load data to Snowflakes
    load_snowflakes_task = PythonOperator(
        task_id="load_data_to_snowflakes",
        python_callable=load_snowflakes_wrapper,
    )

    download_task >> filter_task >> stage_task >> load_snowflakes_task