import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.get_twitch_data import get_twitch_data

S3_HOOK = S3Hook(aws_conn_id="aws_conn")  # getting connection to s3
S3_BUCKET = "twitch-data-samanalyst"  # s3 bucket we'll save the data to
RUN_SAMPLE = False

default_args = {
    "email_on_failure": True,
    "email": [os.getenv("FAILURE_EMAIL")],
}

with DAG(
    "get_raw_twitch_data",
    description="Gets raw twitch data from their API and saves two files as parquet to s3",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
) as dag:
    get_raw_twitch_data = PythonOperator(
        task_id="get_raw_twitch_data",
        python_callable=get_twitch_data,
        op_kwargs={
            "s3_hook": S3_HOOK,
            "s3_bucket": S3_BUCKET,
            "run_sample": RUN_SAMPLE,
        },
    )

    get_raw_twitch_data
