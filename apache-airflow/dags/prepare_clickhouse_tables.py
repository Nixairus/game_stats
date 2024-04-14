
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import clickhouse_driver as ch

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 4)
}

######## PYTHON CODE
def execute_clickhouse_code():


    connection = ch.Client(
        host='192.168.1.11',
        user='default',
        password=''
    )

    create_query_game_reviews = '''CREATE TABLE IF NOT EXISTS game_reviews
    (appid UInt32, 
    recommendationid UInt32,
    author_steamid String,
    author_num_games_owned UInt32,
    language String,
    review_text String,
    timestamp_created Int32,
    timestamp_updated Int32,
    voted_up Boolean,
    votes_up UInt32,
    votes_funny UInt32,
    steam_purchase Boolean,
    received_for_free Boolean,
    written_during_early_access Boolean,
    weighted_vote_score Float32
    ) ENGINE ReplacingMergeTree(recommendationid)
    ORDER BY (appid, weighted_vote_score)
    '''
    connection.execute(create_query_game_reviews)

    create_query_game_info = '''
        CREATE TABLE IF NOT EXISTS game_info (
            steam_appid UInt32,
            name String,
            required_age UInt32,
            is_free UInt8,
            short_description String,
            developers String,
            publishers String,
            categories_id Array(UInt32),
            genres_ids Array(UInt32),
            coming_soon UInt8,
            date String,
            price_rub UInt32
        )
        ENGINE = ReplacingMergeTree(steam_appid)
        ORDER BY steam_appid
    '''

    connection.execute(create_query_game_info)

with DAG('prepare_clickhouse_tables', default_args=default_args, schedule_interval=None) as dag: 
    prepare_clickhouse_tables_task = PythonOperator(
        task_id='prepare_clickhouse_tables',
        python_callable=execute_clickhouse_code
    )

prepare_clickhouse_tables_task