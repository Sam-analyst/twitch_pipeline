import logging

import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.utils.df_transform_utils import (
    add_snapshot_date_column,
    get_snapshot_array_from_df,
    validate_snapshot_array,
)
from src.utils.iceberg_utils import (
    append_pandas_into_iceberg,
    upsert_pandas_into_iceberg,
)
from src.utils.s3_utils import (
    read_parquet_from_s3_for_date,
    write_parquet_to_s3,
)

logger = logging.getLogger("airflow.task")


def process_streams_data(
    s3_hook: S3Hook,
    s3_bucket: str,
    **kwargs,
) -> None:
    """
    This function gets the dates for which to process the
    raw streams df, loops thru each date & transforms the data,
    and then finally saves it in a new location.

    The list of dates to process the data is obtained
    from the "get_process_dates" task.

    For each date, we read in the raw files and dedupe
    them by getting the latest snapshot_datetime for
    each "id", which represents the unique stream id for
    twitch. This is done to since the rwa data will have
    duplicate data in it.

    Once the data is at the stream id - timestamp level,
    we create 2 processed dataframes. The first being
    the streamers dim table, the second being the
    streams_fct table.

    Parameters
    ----------
    s3_hook : Airflow S3Hook
        The S3Hook used to interact with Amazon S3.
    s3_bucket : str
        The name of the S3 bucket from which the files will be read.
    **kwargs:
        This function needs to be called within a task with
        provide_context = True.
    """
    # obtain dates list from task 1 via xcom
    ti = kwargs["ti"]
    dates_list = ti.xcom_pull(task_ids="get_process_dates")
    logger.info(f"Found dates list: {dates_list}")

    for date in dates_list:
        logger.info(f"processing for {date}")

        # read parquet files into pandas df for given date
        streams_df = read_parquet_from_s3_for_date(
            s3_hook=s3_hook,
            s3_bucket=s3_bucket,
            file_path="raw/streams/",
            date_pattern=date,
        )

        # dedupe our streams_df
        streams_df = streams_df.drop_duplicates(
            subset=["id", "snapshot_datetime"], keep="first"
        ).sort_values(
            "snapshot_datetime",
            ascending=False,  # sorting values for aggs later on
        )

        # add snapshot date
        streams_df = add_snapshot_date_column(streams_df)

        # get snapshot array
        snapshot_date_array = get_snapshot_array_from_df(streams_df)

        # validate it
        validate_snapshot_array(snapshot_date_array, date)

        # create streamers dim table
        streamers_dim = streams_df.drop_duplicates(
            subset="user_id", keep="first"
        ).reset_index(drop=True)[
            ["user_id", "user_login", "user_name", "snapshot_date"]
        ]

        # create streams_fct table
        groupby_cols = ["id", "user_id", "game_id", "snapshot_date"]

        # lambda function, which we'll use next
        def distinct_array(x):
            return list(sorted(set(x)))

        streams_fct = (
            streams_df.groupby(groupby_cols)
            .agg(
                avg_viewers=("viewer_count", "mean"),
                title=("title", "first"),
                started_at=("started_at", "min"),
                language=("language", "first"),
                tags_used=(
                    "tags",
                    lambda x: distinct_array(
                        [item for sublist in x for item in sublist]
                    ),
                ),
                is_mature=("is_mature", "first"),
            )
            .reset_index()
        )

        streams_fct.rename(columns={"id": "stream_id"}, inplace=True)

        # write streamers dim
        write_parquet_to_s3(
            df=streamers_dim,
            s3_hook=s3_hook,
            s3_bucket=s3_bucket,
            file_path=f"transformed/streamers_dim/streamers_{snapshot_date_array[0]}.parquet",
        )

        # write streams_fct
        write_parquet_to_s3(
            df=streams_fct,
            s3_hook=s3_hook,
            s3_bucket=s3_bucket,
            file_path=f"transformed/streams_fct/streams_fct_{snapshot_date_array[0]}.parquet",
        )


def upsert_streamers_dim(
    s3_hook: S3Hook,
    s3_bucket: str,
    **kwargs,
) -> None:
    """
    This function reads the transformed streamers dim file from
    s3 and then upserts it into the streamers_dim iceberg table.

    The list of dates to process the data is obtained
    from the "get_process_dates" task.

    For each date, we read in the transformed files and
    then upsert it into the streamers_dim iceberg table.

    Upsert is done in case streamers didn't stream on
    a given date.

    Parameters
    ----------
    s3_hook : Airflow S3Hook
        The S3Hook used to interact with Amazon S3.
    s3_bucket : str
        The name of the S3 bucket from which the files will be read.
    **kwargs:
        This function needs to be called within a task with
        provide_context = True.
    """
    ti = kwargs["ti"]
    dates_list = ti.xcom_pull(task_ids="get_process_dates")
    logger.info(f"Found dates list: {dates_list}")

    for date in dates_list:
        streamers_dim = read_parquet_from_s3_for_date(
            s3_hook=s3_hook,
            s3_bucket=s3_bucket,
            file_path="transformed/streamers_dim/",
            date_pattern=date,
        )

        # rename snapshot date to last_updated_date since that's the date in the iceberg table
        streamers_dim = streamers_dim.rename(
            columns={"snapshot_date": "last_updated_date"}
        )

        upsert_pandas_into_iceberg(
            df=streamers_dim, table="twitch_db.streamers_dim", id="user_id"
        )


def insert_into_streams_fct(s3_hook: S3Hook, s3_bucket: str, **kwargs) -> None:
    """
    This function reads the transformed streams fct file from
    s3 and then appends it to the streams_fct iceberg table.

    The list of dates to process the data is obtained
    from the "get_process_dates" task.

    Parameters
    ----------
    s3_hook : Airflow S3Hook
        The S3Hook used to interact with Amazon S3.
    s3_bucket : str
        The name of the S3 bucket from which the files will be read.
    **kwargs:
        This function needs to be called within a task with
        provide_context = True.
    """
    ti = kwargs["ti"]
    dates_list = ti.xcom_pull(task_ids="get_process_dates")
    logger.info(f"Found dates list: {dates_list}")

    for date in dates_list:
        streams_fct = read_parquet_from_s3_for_date(
            s3_hook=s3_hook,
            s3_bucket=s3_bucket,
            file_path="transformed/streams_fct/",
            date_pattern=date,
        )

        # adjust data types to avoid errors with iceberg
        streams_fct["snapshot_date"] = streams_fct["snapshot_date"].astype("int")
        streams_fct["avg_viewers"] = streams_fct["avg_viewers"].astype("float")
        streams_fct["started_at"] = pd.to_datetime(
            streams_fct["started_at"]
        ).dt.tz_localize(None)

        append_pandas_into_iceberg(df=streams_fct, table="twitch_db.streams_fct")
