FROM apache/airflow:2.10.0-python3.8


COPY airflow/dags/ /opt/airflow/dags/
COPY requirements.txt /requirements.txt
COPY script_tables.sql /opt/airflow/scripts/script_tables.sql
COPY etl /opt/airflow/etl/
COPY utils /opt/airflow/utils/
COPY bronze /opt/airflow/bronze/
COPY silver /opt/airflow/silver/
COPY gold /opt/airflow/gold/

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow
