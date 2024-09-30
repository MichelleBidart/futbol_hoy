from sqlalchemy import create_engine
import configparser

config = configparser.ConfigParser()
config.read('config/config.properties')

# Obtener las variables de conexión de Redshift desde el archivo de configuración
redshift_user = config.get('REDSHIFT', 'user')
redshift_password = config.get('REDSHIFT', 'password')
redshift_host = config.get('REDSHIFT', 'host')
redshift_port = config.get('REDSHIFT', 'port')
redshift_dbname = config.get('REDSHIFT', 'dbname')

# Crear el motor con SQLAlchemy
engine = create_engine(
    f"postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_dbname}"
)

try:
    # Probar la conexión con SQLAlchemy
    connection = engine.connect()
    print("Conexión exitosa con SQLAlchemy")
    connection.close()  # Cerrar la conexión después de probarla
except Exception as e:
    print(f"Error al conectar con SQLAlchemy: {e}")
