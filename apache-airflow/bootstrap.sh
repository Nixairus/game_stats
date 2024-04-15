mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.9.0/docker-compose.yaml'
docker-compose up airflow-init
docker-compose up -d
# Лучше выполнять руками
# sed -i 's/# build: ./build: ./g' docker-compose.yaml
# sed -i "s/AIRFLOW__CORE__LOAD_EXAMPLES: 'true'/AIRFLOW__CORE__LOAD_EXAMPLES: 'false'/g" docker-compose.yaml
# docker-compose stop
# docker-compose build
# docker-compose up -d