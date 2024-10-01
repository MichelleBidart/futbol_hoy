# Usa la imagen oficial de Airflow como base
FROM apache/airflow:2.10.0-python3.8

# Copia los DAGs y otros archivos necesarios al contenedor
COPY airflow/dags/ /opt/airflow/dags/

COPY requirements.txt /requirements.txt

# Instala dependencias adicionales si las necesitas
RUN pip install --no-cache-dir -r /requirements.txt

# Establece la carpeta de trabajo
WORKDIR /opt/airflow

# Inicializa Airflow
RUN airflow db init

# Exponer el puerto de Airflow (webserver)
EXPOSE 8080

# Comando para iniciar el webserver cuando el contenedor se ejecute
CMD ["airflow", "webserver"]
