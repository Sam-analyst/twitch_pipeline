import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from src.utils.s3_utils import write_parquet_to_s3
from twitch_game_analytics import get_twitch_games_and_streams


def get_twitch_data(s3_hook: S3Hook, s3_bucket: str, run_sample: bool = False) -> None:
    """

    At a high-level, this function runs the get_twitch_games_and_streams
    function and then saves the dfs into s3 as parquets.

    The get_twitch_games_and_streams function returns a dictionary
    of pandas dataframes. The first, called games_df, is a dimension
    table containing information on the games/categories being
    streamed. The second, called streams_df, is a table containing
    information on current streams and number of viewers.

    It's important to note that these tables will have duplicate
    data. We save the data as-is and dedupe the data when
    transforming them in later dags. This was done to preserve the raw
    state of the data for easier debugging.

    Parameters
    ----------
    s3_hook: S3Hook
        S3Hook instance, used for authentication to s3 bucket
    s3_bucket: str
        The name of the s3 bucket to save data into
    run_sample: bool (optional)
        Whether to run a sample of the data. This should only be used
        for development purposes only. Default is False
    """

    # extract our twitch data from the api
    dfs_dict = get_twitch_games_and_streams(run_sample=run_sample)

    # capture current time stamp to use in our file name
    snapshot_datetime = datetime.datetime.now(datetime.UTC).strftime("%Y%m%d_T%H%M%SZ")

    # add these timestamps to the dataframes
    dfs_dict["games_df"]["snapshot_datetime"] = snapshot_datetime
    dfs_dict["streams_df"]["snapshot_datetime"] = snapshot_datetime

    # write to s3 as parquet
    write_parquet_to_s3(
        df=dfs_dict["games_df"],
        s3_hook=s3_hook,
        s3_bucket=s3_bucket,
        file_path=f"raw/games/games_{snapshot_datetime}.parquet",
    )

    write_parquet_to_s3(
        df=dfs_dict["streams_df"],
        s3_hook=s3_hook,
        s3_bucket=s3_bucket,
        file_path=f"raw/streams/streams_{snapshot_datetime}.parquet",
    )
