import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
import datetime
from twitch_game_analytics import get_twitch_games_and_streams


def pull_twitch_data() -> None:
    """
    Function that runs the get_twitch_games_and_streams function and then
    saves the dfs as parquets into s3.

    For each dataset, we obtain the current datetime in UTC and append
    that to the file name so that we can track when the data was loaded.
    """

    # obtain our twitch data
    dfs = get_twitch_games_and_streams(run_sample=True)

    # capture current time stamp to use in our file name
    load_datetime = datetime.datetime.now(datetime.UTC).strftime('%Y%m%d_T%H%M%SZ')

    s3_bucket = "twitch-data-samanalyst"

    # writing both files as parquet to s3
    write_parquet_to_s3(dfs["games_df"], s3_bucket, f'raw/games_dim/games_{load_datetime}.parquet')

    write_parquet_to_s3(dfs["streams_df"], s3_bucket, f'raw/streams_fct/streams_{load_datetime}.parquet')


def write_parquet_to_s3(
    df : pd.DataFrame,
    s3_bucket_name : str,
    save_path : str
):
    """
    Function that takes a pandas df as an input and saves it as a parquet
    to a specified location within s3
    """

    # Convert DataFrame to Parquet
    table = pa.Table.from_pandas(df)

    # Convert Parquet to in-memory BytesIO buffer
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)  # Reset pointer to the start of the buffer
    
    # Initialize S3 hook and upload the CSV to S3
    s3_hook = S3Hook(aws_conn_id='aws_conn')
    s3_hook.load_bytes(
        bytes_data=parquet_buffer.getvalue(),  # Get the bytes of the buffer
        key=save_path,
        bucket_name=s3_bucket_name
    )

    print(f"File uploaded to s3://{s3_bucket_name}/{save_path}")


# Airflow DAG definition
with DAG(
    'extract_twitch_data',
    description='Extracts data from twitch and saves it to S3 as a parquet',
    schedule_interval=None,
    start_date=datetime.datetime(2025, 2, 16),
    catchup=False,
) as dag:
    
    upload_task = PythonOperator(
        task_id='pull_twitch_data_and_save_to_s3',
        python_callable=pull_twitch_data,
    )

    upload_task