from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 1),
    "email_on_failure": True,
    "email": ["samtavalalidev@gmail.com"]
}

with DAG(
    "twitch_pipeline_test",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
    ) as dag:

    # create games dim temp table
    create_temp_games_dim = SQLExecuteQueryOperator(
        task_id="create_temp_games_dim",
        conn_id="snowflake_conn_id",
        sql="""
        CREATE TABLE IF NOT EXISTS temp_games_dim (
        game_id STRING,
        game_name STRING,
        box_art_url STRING,
        igdb_id STRING
        )
        """
    )

    create_temp_games_dim