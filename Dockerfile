FROM apache/airflow:2.10.0-python3.8

COPY airflow/dags/ /opt/airflow/dags/

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

WORKDIR /opt/airflow

RUN airflow db init

EXPOSE 8080

CMD ["airflow", "webserver"]
