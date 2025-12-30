# airflow_task

###  Project Scenario Background:
Hired as a data engineer by a data consulting organization, who is looking at building a stock market prediction tool that applies sentiment analysis, called LaunchSentiment. To perform this sentiment analysis, they plan to leverage the data about the number of Wikipedia page views a company has.

Wikipedia is one of the largest public information resources on the internet. Besides the wiki pages, other items such as website pageview counts are also publicly available. To make things simple, they assume that an increase in a company’s website page views shows a positive sentiment, and the company’s stock is likely to increase. On the other hand, a decrease in pageviews tells us a loss in interest, and the stock price is likely to decrease.

### Data Source:
Luckily the needed data to perform this sentiment analysis is readily available. The Wikimedia Foundation (the organization behind Wikipedia) has provided all pageviews since 2015 in machine-readable format. The pageviews can be downloaded in gzip format and are aggregated per hour per page.

Each hourly dump is approximately 50 MB in gzipped text files and is somewhere between 200 and 250 MB in size unzipped.

All pageviews data can be found here: https://dumps.wikimedia.org/other/pageviews

The pageviews data for October, 2025 can be found here: https://dumps.wikimedia.org/other/pageviews/2025/2025-10/

### Sample Data Explanation

<img width="512" height="402" alt="airflow_task_sample_data" src="https://github.com/user-attachments/assets/d47daaab-4ef6-4f05-bad6-66d386772f07" />

### Project Tasks:
For a start my manager has asked me to create the first version of a DAG pulling the Wikipedia pageview counts by downloading, extracting, and reading the pageview data for any one hour duration on any date in December 2025 (e.g 4pm data for 10th of December, 2024). To further streamline your analysis, you have been asked to select just five companies (Amazon, Apple, Facebook, Google, and Microsoft) from the data extracted in order to initially track and validate the hypothesis.

When done with the DAG development and would have successfully loaded the data into a database by running the data pipeline, then perform a simple analysis to show which company’s page out of the 5 selected has the highest views (Will write a simple SQL query to achieve this).

### Pipeline Overview

1. Objective:

Build a data pipeline that extracts data from a source (CSV, API, or compressed .gz files), transforms it (cleaning, enrichment, hashing), and loads it into a PostgreSQL or Snowflake database.

Pipeline must handle gracefully, avoid duplicates, and provide monitoring & alerts.

### Key Design Decisions:

- Orchestration: Apache Airflow – for scheduling, retries, logging, and monitoring.

- Extraction: Support for .csv and .gz files from local or remote sources.

- Transformation: Row hashing for idempotency, filtering specific symbols.

- Load: Snowflake with duplicate avoidance.

### Best Practices:

- Idempotence: Row hash ensures we don’t insert duplicates.

- Retries: Tasks retry on failure with exponential backoff.

- Alerts: Email or Slack alerts on failures.

- Logging: Detailed logging for debugging.

### Best Practices Implemented

- Idempotence: Each row has a unique_id hash; duplicates are ignored using ON CONFLICT DO NOTHING.

- Retries & Alerts: Tasks retry up to 3 times on failure.

- Email alerts on failure: 

- Logging: Every major step logs info for observability.

- Graceful Error Handling: Missing files or API failures can be handled with try-except.

Modular Tasks: Extraction, transformation, and load are separate tasks – easier debugging & scalability.

### Documentation Notes

- Why PythonOperator: Chosen for flexibility with API calls, file handling, and Pandas transformations.

- Why Hashing Rows: Ensures inserting the same data twice doesn’t create duplicates.

- Why Catchup=False: Avoids reprocessing historical data automatically

### Clone project
```
git clone https://github.com/kabiromohd/airflow_task.git

cd airflow_task

pip install -r requirements.txt

docker-compose up -d   ====> this starts the airflow instance and create all the necessary folders and file for airflow
```

Open the Airflow UI via ``` localhost:8080``` and trigger the dag for the sentiment task

### Airflow run-time UI

![Airflow UI](https://github.com/user-attachments/assets/9ed8948b-95ab-4ac2-bbdf-7ce48c8fa8c1)

### Snowflakes run-time UI

This is a snapshot of the snowflakes UI

![Snowflakes UI](https://github.com/user-attachments/assets/d181df08-596e-418f-b4b1-884cd72a57e3)

- Reason for choice of Snowflake
  Snowflake was chosen as the analytical data warehouse for this pipeline due to its scalability, operational simplicity, and strong integration with modern data engineering tools like Apache Airflow. 



