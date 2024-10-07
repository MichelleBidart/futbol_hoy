import redshift_connector
import awswrangler as wr
from dotenv import load_dotenv
import os

load_dotenv('/opt/airflow/.env')

def get_redshift_connection():
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')
    redshift_host = os.getenv('REDSHIFT_HOST')
    redshift_port = os.getenv('REDSHIFT_PORT')
    redshift_dbname = os.getenv('REDSHIFT_DBNAME')

    # Parámetros de conexión a Redshift
    conn_params = {
        'host': redshift_host,
        'database': redshift_dbname,
        'user': redshift_user,
        'password': redshift_password,
        'port': int(redshift_port),
    }

    # Establecer conexión con redshift_connector
    conn = redshift_connector.connect(**conn_params)
    print("Conexión exitosa con Redshift mediante redshift_connector")
    return conn

def get_schema():
    return os.getenv('REDSHIFT_SCHEMA')
