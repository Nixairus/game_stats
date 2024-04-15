# game_stats
Airflow managed Steam stats database 
Выполяем 
git clone https://github.com/Nixairus/game_stats.git
git checkout dev
cd game_stats

Запуск ClickHouse:
cd clickhouse
docker-compose up -d

Запуск Metabase
cd metabase
mkdir -p db
docker-compose up -d

Запуск Airflow
cd apache-airflow
./bootstrap.sh
Далее необходимо будет перебилдить для поддержки clickhouse-driver (см комментарии bootstrap.sh)
sed -i 's/# build: ./build: ./g' docker-compose.yaml
sed -i "s/AIRFLOW__CORE__LOAD_EXAMPLES: 'true'/AIRFLOW__CORE__LOAD_EXAMPLES: 'false'/g" docker-compose.yaml
docker-compose stop
docker-compose build
docker-compose up -d

В скриптах apache-airflow/dags/*.py поменять IP адрес clickhouse-сервера на локальный(заменить 192.268.1.11 на свой)
