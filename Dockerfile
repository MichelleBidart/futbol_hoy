FROM apache/airflow:2.10.0-python3.8

COPY airflow/dags/ /opt/airflow/dags/
COPY requirements.txt /requirements.txt

COPY script_tables.sql /opt/airflow/scripts/script_tables.sql
COPY etl/ /opt/airflow/etl/
COPY .env /opt/airflow/.env


# Agregar el directorio /opt/airflow/etl al PYTHONPATH
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/etl"

RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow

EXPOSE 8080

CMD ["airflow", "webserver"]
