import os

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog


def get_iceberg_table(table):
    """
    Gets iceberg table from aws. Table must be database.table
    """
    catalog = load_catalog("glue_catalog", **{"type": "glue"})
    return catalog.load_table(table)


def convert_iceberg_into_pandas(iceberg_table):
    """
    Converts an iceberg table into pandas
    """
    return iceberg_table.scan().to_pandas()


def upsert_pandas_into_iceberg(df, table, id):
    # get iceberg table
    iceberg_table = get_iceberg_table(table=table)

    # convert to pandas
    full_df = convert_iceberg_into_pandas(iceberg_table=iceberg_table)

    # upsert using pandas
    merged_df = pd.merge(full_df, df, on=id, how="outer", suffixes=("_old", "_new"))

    columns_excluding_id = [col for col in full_df.columns if col != id]

    for col in columns_excluding_id:
        merged_df[col] = merged_df[f"{col}_new"].combine_first(merged_df[f"{col}_old"])

    merged_df = merged_df[full_df.columns]

    final_table = pa.Table.from_pandas(merged_df)

    with iceberg_table.transaction() as txn:
        txn.overwrite(final_table)


def append_pandas_into_iceberg(df, table):
    # get iceberg table
    iceberg_table = get_iceberg_table(table=table)

    final_table = pa.Table.from_pandas(df)

    # this is needed so we can upload timestamp into iceberg ughh
    os.environ["PYICEBERG_DOWNCAST_NS_TIMESTAMP_TO_US_ON_WRITE"] = "true"

    iceberg_table.append(final_table)
