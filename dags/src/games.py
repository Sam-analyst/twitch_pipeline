import logging

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.utils.df_transform_utils import (
    add_snapshot_date_column,
    get_snapshot_array_from_df,
    validate_snapshot_array,
)
from src.utils.iceberg_utils import upsert_pandas_into_iceberg
from src.utils.s3_utils import (
    read_parquet_from_s3_for_date,
    write_parquet_to_s3,
)

logger = logging.getLogger("airflow.task")


def process_games_dim(
    s3_hook: S3Hook,
    s3_bucket: str,
    **kwargs,
) -> None:
    """
    This function gets the dates for which to process the
    raw games df, loops thru each date & transforms the data,
    and then finally saves it in a new location.

    The list of dates to process the data is obtained
    from the "get_process_dates" task.

    For each date, we read in the raw files and dedupe
    them by getting the latest snapshot_datetime for
    each "id", which represents the unique game id for
    twitch. This is done to provide a single representation
    for the games data for a given date. The raw data
    is ran multiple times each day.

    Once the data is at the game id - date level,
    we save it out to s3 as a tranformed file.

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
    ti = kwargs.get("ti")
    dates_list = ti.xcom_pull(task_ids="get_process_dates")
    logger.info(f"Found dates list: {dates_list}")

    for date in dates_list:
        logger.info(f"processing for {date}")

        # read parquet files into pandas df for given date
        games_df = read_parquet_from_s3_for_date(
            s3_hook=s3_hook,
            s3_bucket=s3_bucket,
            file_path="raw/games/",
            date_pattern=date,
        )

        # dedupe our games_df
        games_df = games_df.sort_values(
            "snapshot_datetime", ascending=False
        ).drop_duplicates(subset=["id"], keep="first")

        # add snapshot date
        games_df = add_snapshot_date_column(games_df)

        # get snapshot array
        snapshot_date_array = get_snapshot_array_from_df(games_df)

        # validate it
        validate_snapshot_array(snapshot_date_array, date)

        # select the columns we need and rename id to game_id
        games_df = (
            games_df[["id", "name", "box_art_url", "snapshot_date"]]
            .rename(columns={"id": "game_id"})
            .reset_index(drop=True)
        )

        write_parquet_to_s3(
            df=games_df,
            s3_hook=s3_hook,
            s3_bucket=s3_bucket,
            file_path=f"transformed/games_dim/games_{snapshot_date_array[0]}.parquet",
        )


def upsert_games_dim(
    s3_hook: S3Hook,
    s3_bucket: str,
    **kwargs,
) -> None:
    """
    This function reads the transformed games file from
    s3 and then upserts it into the games_dim iceberg table.

    The list of dates to process the data is obtained
    from the "get_process_dates" task.

    Upsert is done in case certain games weren't streamed
    on a certain day.

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
        games_dim = read_parquet_from_s3_for_date(
            s3_hook=s3_hook,
            s3_bucket=s3_bucket,
            file_path="transformed/games_dim/",  # location of transformed games file
            date_pattern=date,
        )

        # rename snapshot date to last_updated_date since that's the date in the iceberg table
        games_dim = games_dim.rename(columns={"snapshot_date": "last_updated_date"})

        upsert_pandas_into_iceberg(
            df=games_dim, table="twitch_db.games_dim", id="game_id"
        )
