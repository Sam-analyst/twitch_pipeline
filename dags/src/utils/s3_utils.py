import logging
from io import BytesIO
from typing import List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger("airflow.task")


def read_parquet_from_s3_for_date(
    s3_hook: S3Hook,
    s3_bucket: str,
    file_path: str,
    date_pattern: str,
) -> pd.DataFrame:
    """
    This function retrieves all file paths from the specified
    S3 bucket and location (`file_path`) that match the given
    `date_pattern`. It then reads the parquet files from those
    paths into Pandas DataFrames, concatenates them, and
    returns the combined DataFrame.

    Parameters
    ----------
    s3_hook : Airflow S3Hook
        The S3Hook used to interact with Amazon S3.

    s3_bucket : str
        The name of the S3 bucket from which the files will be read.

    file_path : str
        The S3 file path (prefix) where the raw parquet files are located.
        This is typically the directory or folder within the S3 bucket
        where the files are stored.

    date_pattern : str
        The pattern (e.g., a date in the form of "YYYYMMDD") used to
        filter files. Only files that match this pattern will be processed.

    Returns
    -------
    pd.DataFrame
        A concatenated Pandas DataFrame containing the data from all
        parquet files that matched the date pattern.
    """
    # first get all files from file_path that match the date pattern
    filepaths_list = get_s3_filepaths(
        s3_hook=s3_hook,
        s3_bucket=s3_bucket,
        file_path=file_path,
        date_pattern=date_pattern,
    )

    logger.info(f"Found files for {date_pattern}: {filepaths_list}")

    dfs = [
        read_parquet_from_s3(s3_hook, s3_bucket, file_path)
        for file_path in filepaths_list
    ]

    return pd.concat(dfs)


def get_s3_filepaths(
    s3_hook: S3Hook, s3_bucket: str, file_path: str, date_pattern: str
) -> List[str]:
    """
    Helper function that finds all file paths in a s3 location
    that contains the date_pattern and returns them as a list.
    """

    # get all files from the s3 - file_path location
    files_list = s3_hook.list_keys(bucket_name=s3_bucket, prefix=file_path)

    # filter them based on date_pattern
    filtered_files_list = [file for file in files_list if date_pattern in file]

    # make sure it's not empty
    if len(filtered_files_list) == 0:
        raise Exception(f"No files found for {date_pattern}")

    return filtered_files_list


def read_parquet_from_s3(
    s3_hook: S3Hook, s3_bucket: str, file_path: str
) -> pd.DataFrame:
    """
    This function fetches a Parquet file from the specified
    S3 bucket and file path using the provided S3Hook.

    The file is read into memory and then loaded into a
    Pandas DataFrame for further processing.
    """
    # get parquet file from s3
    parquet_file = s3_hook.get_key(key=file_path, bucket_name=s3_bucket)

    # read contents of file (in bytes) into memory
    parquet_bytes = parquet_file.get()["Body"].read()

    # convert byte data into ByteIO buffer to simulate a file object for pandas
    parquet_buffer = BytesIO(parquet_bytes)

    # Read the Parquet file from the buffer and return it as a Pandas DataFrame
    return pd.read_parquet(parquet_buffer)


def write_parquet_to_s3(
    df: pd.DataFrame, s3_hook: S3Hook, s3_bucket: str, file_path: str
):
    """
    Helper function that saves a pandas df as parquet into a given S3 location.

    Parameters
    ----------
    df: pd.DataFrame
        A pandas dataframe to write to s3
    s3_hook: S3Hook
        S3Hook instance, used for authentication to s3 bucket
    s3_bucket: str
        The name of the s3 bucket to save data into
    file_path: str
        File path for which to save the dataframe. This should
        include the file name as well.
    """

    # convert pandas df to parquet
    table = pa.Table.from_pandas(df)

    # convert parquet to in-memory BytesIO buffer
    parquet_buffer = BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)  # Reset pointer to the start of the buffer

    # write file to s3
    s3_hook.load_bytes(
        bytes_data=parquet_buffer.getvalue(),
        bucket_name=s3_bucket,
        key=file_path,
        replace=True,  # overwrite df if exists
    )

    logger.info(f"File uploaded to s3://{s3_bucket}/{file_path}")
