from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import clickhouse_driver as ch

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 14)
}

######## PYTHON CODE
def api_get_top_games():
    import requests
    api_url='https://steamspy.com/api.php?request=top100in2weeks'
    result_api = requests.get(url=api_url)
    top = result_api.json()
    top_games_list = list(top.keys())

    return top_games_list

def insert_top_games(top_games_list):
    insert_query = 'INSERT INTO top100games (app_id) VALUES'
    drop_query = 'DROP TABLE IF EXISTS top100games'
    create_query = '''
    CREATE TABLE IF NOT EXISTS top100games (
        app_id UInt32
    )
    ENGINE = ReplacingMergeTree(app_id)
    ORDER BY app_id
'''

    # Create a ClickHouse connection
    connection = ch.Client(
        host='192.168.1.11',
        user='default',
        password=''
    )

    # Drop the table, if it exists
    connection.execute(drop_query)

    # Create a new table game_info with the correct data types for arrays
    connection.execute(create_query)

    # Create a list of values for bulk insertion
    values = []
    for app_id in top_games_list:
        values.append(f"({int(app_id)})")

    # Combine the values into a string
    values_str = ', '.join(values)

    # Full SQL query for bulk insertion
    full_insert_query = f"{insert_query} {values_str}"

    # Execute the bulk insertion of data
    connection.execute(full_insert_query)

########### DAG SETUP
with DAG('find_top_games', default_args=default_args, schedule_interval=None, catchup=False) as dag:
    
    fetch_top_games_task = PythonOperator(
        task_id='api_get_top_games',
        python_callable=api_get_top_games
    )
    
    insert_top_games_task = PythonOperator(
        task_id='insert_top_games',
        python_callable=insert_top_games,
        op_args=[fetch_top_games_task.output]
    )
    
fetch_top_games_task >> insert_top_games_task