from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import clickhouse_driver as ch
import logging
import requests
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'get_top_games_info',
    default_args=default_args,
    description='Fetch data from API Steam and write to ClickHouse',
)


def fetch_api_data(app_id):
    logger = logging.getLogger(__name__)
    api_url = 'https://store.steampowered.com/api/appdetails?appids=' + str(app_id)
    try:
        result_api = requests.get(url=api_url)
        result_api.raise_for_status()
        data = result_api.json()[str(app_id)]['data']

        developers = ', '.join(data['developers'])
        publishers = ', '.join(data['publishers'])
        categories_id = [category['id'] for category in data['categories']]
        genres_ids = [genre['id'] for genre in data['genres']]

        game_info = {
            'steam_appid': int(data['steam_appid']),
            'name': data['name'].replace("'","\\'"),
            'required_age': int(data['required_age']),
            'is_free': int(data['is_free']),
            'short_description': data['short_description'].replace("'","\\'"),
            'developers': developers,
            'publishers': publishers,
            'categories_id': categories_id,
            'genres_ids': genres_ids,
            'coming_soon': int(data['release_date']['coming_soon']),
            'date': data['release_date']['date'],
        }

        if 'price_overview' in data and 'final' in data['price_overview']:
            game_info['price_rub'] = int(data['price_overview']['final'])
        else:
            game_info['price_rub'] = 0
        time.sleep(1)
        return game_info
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for app_id {app_id}: {str(e)}")
        logger.error(f"Error fetching data for app_id {app_id}: {str(e)}")
        return None
    except KeyError as e:
        print(f"KeyError for app_id {app_id}: {str(e)}")
        logger.error(f"KeyError for app_id {app_id}: {str(e)}")
        return None


def write_to_clickhouse():
    connection = ch.Client(
        host='192.168.1.11',
        user='default',
        password=''
    )

    query = 'SELECT app_id FROM top100games'
    result_query = connection.execute(query)
    app_ids = [row[0] for row in result_query]

    game_info_list = []

    for app_id in app_ids:
        game_info = fetch_api_data(app_id)
        if game_info is None:
            continue
        game_info_list.append(game_info)
        

    insert_query = 'INSERT INTO game_info (steam_appid, name, required_age, is_free, short_description, developers, publishers, categories_id, genres_ids, coming_soon, date, price_rub) VALUES'
    values = []
    for game_info in game_info_list:
        values.append("({steam_appid}, '{name}', {required_age}, {is_free}, '{short_description}', '{developers}', '{publishers}', {categories_id}, {genres_ids}, {coming_soon}, '{date}', {price_rub})".format(**game_info))
    insert_query += ', '.join(values)

    connection.execute(insert_query)


with DAG('get_game_info', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    fetch_and_write_task = PythonOperator(
        task_id='fetch_and_write_task',
        python_callable=write_to_clickhouse,
    )
fetch_and_write_task