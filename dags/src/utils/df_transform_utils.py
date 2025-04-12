import numpy as np
import pandas as pd


def add_snapshot_date_column(
    df: pd.DataFrame, snapshot_datetime_column_name: str = "snapshot_datetime"
) -> pd.DataFrame:
    """
    Helper function that assumes a column with the format: "%Y%m%d_T%H%M%SZ",
    splits it on the "_", gets the 0th index (first element), then adds it
    as a column to the df
    """
    df["snapshot_date"] = df[snapshot_datetime_column_name].str.split("_").str[0]
    return df


def get_snapshot_array_from_df(
    df: pd.DataFrame, snapshot_date_column_name: str = "snapshot_date"
) -> np.ndarray:
    """
    Gets unique snapshot dates from column
    """
    return df[snapshot_date_column_name].unique()


def validate_snapshot_array(array: np.ndarray, date: str) -> None:
    """
    Helper function to ensure snapshot array is correct
    """

    if len(array) > 1 or array[0] != date:
        raise Exception(
            f"Snapshot dates in the raw data dont align with given process date {array}"
        )
