import datetime
import os

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.games import process_games_dim, upsert_games_dim
from src.streams import (
    insert_into_streams_fct,
    process_streams_data,
    upsert_streamers_dim,
)
from src.utils.date_utils import get_process_dates

S3_HOOK = S3Hook(aws_conn_id="aws_conn")
S3_BUCKET = "twitch-data-samanalyst"

default_args = {
    "email_on_failure": True,
    "email": [os.getenv("FAILURE_EMAIL")],
}

with DAG(
    "transform_raw_twitch_data",
    description="Transforms raw twitch data, saves it to s3 as a parquet, then uploads it into iceberg",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime.datetime(2025, 1, 1),
    catchup=False,
    params={
        "start_date": Param(
            default=None,
            title="Start date",
            description="Must be YYYYMMDD and provided as an int (e.g. 20200131)",
        ),
        "end_date": Param(
            default=None,
            title="End date",
            description="Must be YYYYMMDD and provided as an int (e.g. 20200131)",
        ),
    },
) as dag:
    get_process_dates = PythonOperator(
        task_id="get_process_dates",
        python_callable=get_process_dates,
        provide_context=True,
    )

    process_games_dim = PythonOperator(
        task_id="process_games_data",
        python_callable=process_games_dim,
        op_kwargs={
            "s3_hook": S3_HOOK,
            "s3_bucket": S3_BUCKET,
        },
        provide_context=True,
    )

    process_streams_data = PythonOperator(
        task_id="process_streams_data",
        python_callable=process_streams_data,
        op_kwargs={
            "s3_hook": S3_HOOK,
            "s3_bucket": S3_BUCKET,
        },
        provide_context=True,
    )

    upsert_games_dim = PythonOperator(
        task_id="upsert_games_dim",
        python_callable=upsert_games_dim,
        op_kwargs={
            "s3_hook": S3_HOOK,
            "s3_bucket": S3_BUCKET,
        },
        provide_context=True,
    )

    upsert_streamers_dim = PythonOperator(
        task_id="upsert_streamers_dim",
        python_callable=upsert_streamers_dim,
        op_kwargs={
            "s3_hook": S3_HOOK,
            "s3_bucket": S3_BUCKET,
        },
        provide_context=True,
    )

    insert_into_streams_fct = PythonOperator(
        task_id="insert_into_streams_fct",
        python_callable=insert_into_streams_fct,
        op_kwargs={
            "s3_hook": S3_HOOK,
            "s3_bucket": S3_BUCKET,
        },
        provide_context=True,
    )

    get_process_dates >> [process_games_dim, process_streams_data]
    process_games_dim >> upsert_games_dim
    process_streams_data >> [upsert_streamers_dim, insert_into_streams_fct]
