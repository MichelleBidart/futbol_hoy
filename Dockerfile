FROM apache/airflow:2.10.0-python3.8

COPY airflow/dags/ /opt/airflow/dags/
COPY requirements.txt /requirements.txt

COPY script_tables.sql /opt/airflow/scripts/script_tables.sql
COPY etl/etl_countries.py /opt/airflow/etl/etl_countries.py
COPY etl/etl_teams_venues.py /opt/airflow/etl/etl_teams_venues.py
COPY etl/api_url_configurations.py /opt/airflow/etl/api_url_configurations.py  
COPY etl/redshift_utils.py /opt/airflow/etl/redshift_utils.py
COPY .env /opt/airflow/.env
COPY etl/etl_leagues.py /opt/airflow/etl/etl_leagues.py
COPY etl/etl_match.py /opt/airflow/etl/etl_match.py


ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/etl"

RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow

EXPOSE 8080

CMD ["airflow", "webserver"]
