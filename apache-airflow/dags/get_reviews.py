from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import clickhouse_driver as ch
import requests
import time
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 4, 4)
}

def get_app_ids(connection):
    query = 'SELECT app_id FROM top100games'
    result_query = connection.execute(query)
    app_ids = [row[0] for row in result_query]
    return app_ids

def get_app_ids_task():
    connection = ch.Client(
        host='192.168.1.11',
        user='default',
        password=''
    )
    app_ids = get_app_ids(connection)
    return app_ids

def get_reviews_task(app_id, dataset_size=1000):
    connection = ch.Client(
        host='192.168.1.11',
        user='default',
        password=''
    )
    reviews_df = request_reviews_for_app(app_id, dataset_size)
    write_to_clickhouse(connection, reviews_df)

def get_app_ids(connection):
    query = 'SELECT app_id FROM top100games'
    result_query = connection.execute(query)
    app_ids = [row[0] for row in result_query]
    return app_ids

def request_reviews_for_app(app_id, dataset_size):
    api_url = f'https://store.steampowered.com/appreviews/{app_id}?json=1&num_per_page=100&purchase_type=all&day_range=30'
    result_api = requests.get(url=api_url)
    result_json = result_api.json()
    reviews = result_json.get('reviews', [])
    reviews_count = len(reviews)
    while reviews_count < dataset_size:
        time.sleep(5)
        cursor = result_json.get('cursor')
        if not cursor:
            break
        api_url = f'https://store.steampowered.com/appreviews/{app_id}?json=1&num_per_page=100&purchase_type=all&cursor={cursor}'
        result_api = requests.get(url=api_url)
        result_json = result_api.json()
        reviews += result_json.get('reviews', [])
        reviews_count = len(reviews)
    reviews = reviews[:dataset_size]
    df = pd.DataFrame(reviews)
    df['appid'] = app_id
    df['author_steamid'] = df['author'].apply(lambda x: x['steamid'])
    df['author_num_games_owned'] = df['author'].apply(lambda x: x['num_games_owned'])
    df['recommendationid'] = df['recommendationid'].astype(int)
    df['weighted_vote_score'] = df['weighted_vote_score'].astype(float)
    df = df[['appid', 'recommendationid', 'author_steamid', 'author_num_games_owned', 'language', 'review', 'timestamp_created', 'timestamp_updated', 'voted_up', 'votes_up', 'votes_funny', 'steam_purchase', 'received_for_free', 'written_during_early_access', 'weighted_vote_score']]
    return df

def write_to_clickhouse(connection, df):
    connection.execute('CREATE TABLE IF NOT EXISTS game_reviews (appid UInt32, recommendationid UInt32, author_steamid String, author_num_games_owned UInt32, language String, review_text String, timestamp_created Int32, timestamp_updated Int32, voted_up Boolean, votes_up UInt32, votes_funny UInt32, steam_purchase Boolean, received_for_free Boolean, written_during_early_access Boolean, weighted_vote_score Float32) ENGINE MergeTree ORDER BY appid')
    connection.execute('INSERT INTO game_reviews VALUES', df.values.tolist())

def create_get_reviews_operator(app_id):
    return PythonOperator(
        task_id=f'get_reviews_for_app_{app_id}',
        python_callable=get_reviews_task,
        op_kwargs={'app_id': app_id},
    )

with DAG('get_games_reviews', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    get_app_ids_operator = PythonOperator(
        task_id='get_app_ids',
        python_callable=get_app_ids_task,
    )

    get_app_ids_task = get_app_ids_task()
    get_reviews_operators = [create_get_reviews_operator(app_id) for app_id in get_app_ids_task]

    get_app_ids_operator >> get_reviews_operators