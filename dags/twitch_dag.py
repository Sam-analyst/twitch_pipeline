from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from twitch_game_analytics import get_top_n_games_and_stats
from datetime import datetime, timedelta
from include.utils import read_sql

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 10, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "twitch_pipeline",
    default_args=default_args,
    schedule_interval="0 */4 * * *",
    catchup=False
    ) as dag:

    delete_temp_games_dim = SQLExecuteQueryOperator(
        task_id="delete_temp_games_dim",
        conn_id="snowflake_conn_id",
        sql=read_sql("sql/games_dim/delete_temp_games_dim.sql")
    )

    create_temp_games_dim = SQLExecuteQueryOperator(
        task_id="create_temp_games_dim",
        conn_id="snowflake_conn_id",
        sql=read_sql("sql/games_dim/create_temp_games_dim.sql")
    )

    @task
    def get_and_insert_twitch_data(N_GAMES=20):
        """Task to get top n games and stats from twitch and upload them to snowflake"""

        dfs = get_top_n_games_and_stats(N_GAMES)

        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn_id")

        with hook.get_conn() as conn:

            # insert games dim into temp table
            success, nchunks, nrows, _ = write_pandas(conn, dfs["games_dim"], "TEMP_GAMES_DIM")

            if success:
                print(f"Sucessfully inserted {nrows} rows into TEMP_GAMES_DIM")

            success, nchunks, nrows, _ = write_pandas(conn, dfs["game_stats"], "GAME_METRICS_FCT", use_logical_type=True)

            if success:
                print(f"Sucessfully inserted {nrows} rows into GAME_METRICS_FCT")
        
        print(conn.is_closed())
    
    upsert_games_dim = SQLExecuteQueryOperator(
        task_id="upsert_games_dim",
        conn_id="snowflake_conn_id",
        sql=read_sql("sql/games_dim/upsert_games_dim.sql")
    )

    delete_temp_games_dim >> create_temp_games_dim >> get_and_insert_twitch_data(20) >> upsert_games_dim