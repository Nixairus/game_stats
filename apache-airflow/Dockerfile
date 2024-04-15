FROM apache/airflow:2.9.0
COPY requirements.txt /
RUN pip install --no-cache-dir setuptools "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt