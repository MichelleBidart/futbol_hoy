from sqlalchemy import create_engine
import configparser
import psycopg2

config = configparser.ConfigParser()
config.read('config/config.properties')

redshift_user = config.get('REDSHIFT', 'user')
redshift_password = config.get('REDSHIFT', 'password')
redshift_host = config.get('REDSHIFT', 'host')
redshift_port = config.get('REDSHIFT', 'port')
redshift_dbname = config.get('REDSHIFT', 'dbname')
redshift_schema = config.get('REDSHIFT', 'schema')


connection = psycopg2.connect(
dbname=redshift_dbname,
user=redshift_user,
password=redshift_password,
host=redshift_host,
port=redshift_port
)
print("Conexión exitosa con psycopg2")
# Crear el motor con SQLAlchemy
engine = create_engine('postgresql+psycopg2://', creator=lambda: connection)

try:
    # Probar la conexión con SQLAlchemy
    connection = engine.connect()
    print("Conexión exitosa con SQLAlchemy")
    connection.close()  # Cerrar la conexión después de probarla
except Exception as e:
    print(f"Error al conectar con SQLAlchemy: {e}")
