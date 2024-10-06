FROM apache/airflow:2.10.0-python3.8


COPY airflow/dags/ /opt/airflow/dags/
COPY requirements.txt /requirements.txt

# Copia los scripts necesarios
COPY script_tables.sql /opt/airflow/scripts/script_tables.sql
COPY etl/etl_countries.py /opt/airflow/scripts/etl_countries.py
COPY etl/etl_teams_venues.py /opt/airflow/scripts/etl_teams_venues.py
COPY config/config.properties /opt/airflow/config/config.properties


# Instalar dependencias
RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow

# Exponer el puerto para el servidor web
EXPOSE 8080

# Comando para iniciar el servidor web de Airflow
CMD ["airflow", "webserver"]
