# game_stats

Airflow managed Steam stats database 

## Установка

Чтобы установить и запустить проект, выполните следующие шаги:

1. Клонируйте репозиторий:
```
git clone https://github.com/Nixairus/game_stats.git
git checkout dev
cd game_stats
```

2. Запуск ClickHouse:
```
cd clickhouse
docker-compose up -d
```

3. Запуск Metabase:
```
cd ../metabase
mkdir -p db
docker-compose up -d
```

4. Запуск Airflow:
```
cd ../apache-airflow
./bootstrap.sh
```

5. Выполните пересборку apache-airflow для поддержки clickhouse-driver изменив строки в docker-compose-файле:
```
sed -i 's/# build: ./build: ./g' docker-compose.yaml
sed -i "s/AIRFLOW__CORE__LOAD_EXAMPLES: 'true'/AIRFLOW__CORE__LOAD_EXAMPLES: 'false'/g" docker-compose.yaml
docker-compose stop
docker-compose build
docker-compose up -d
```

6. В скриптах apache-airflow/dags/*.py поменяйте IP адрес clickhouse-сервера на локальный (замените 192.268.1.11 на свой).
