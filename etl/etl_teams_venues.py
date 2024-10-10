import utils.api_url_configurations as api_url_configurations
import requests
import utils.redshift_utils as redshift_utils
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv
import os
from typing import List, Dict
from utils import constants
from utils import parquet_operations
from utils import database_operations


def extract_teams_venues() -> None:
    """
    Extrae datos de equipos y estadios desde la API y los guarda en archivos Parquet.
    
    """
    #la version gratuita pide al menos un parametro
    params = {
        'country': 'Argentina'
    }
    url, headers = api_url_configurations.get_api_url_headers()
    response = requests.get(f'{url}/teams', headers=headers, params=params)
    response.raise_for_status()

    teams = response.json()['response']

    teams_df = pd.DataFrame([{
        'id': team['team']['id'],
        'name': team['team']['name'],
        'country': team['team']['country'],
        'logo': team['team']['logo'],
        'stadium_id': team['venue']['id']
    } for team in teams])
    

    venues_df = pd.DataFrame([{
        'id': team['venue']['id'],
        'name': team['venue']['name'],
        'city': team['venue']['city'],
        'capacity': team['venue']['capacity'],
        'address': team['venue']['address']
    } for team in teams if team['venue']['id'] is not None])
    

    parquet_operations.save_parquet(os.path.join(constants.Config.BASE_TEMP_PATH, constants.Config.TEAM_FOLDER) 
                                    ,constants.Config.TEAM_ARGENTINA_FILE, teams_df)
    
    parquet_operations.save_parquet(os.path.join(constants.Config.BASE_TEMP_PATH,constants.Config.VENUES_FOLDER),
                                    constants.Config.VENUES_ARGENTINA_FILE, venues_df)


def transform_teams() -> None:
    """
    Transforma los datos de equipos para filtrar los equipos de Argentina.
    
    """
    argentina_teams_df = parquet_operations.read_parquet_file(os.path.join(constants.Config.BASE_TEMP_PATH,constants.Config.TEAM_FOLDER,constants.Config.TEAM_ARGENTINA_FILE))

    argentina_teams_df.drop_duplicates(subset=['id'])

    parquet_operations.save_parquet(os.path.join(constants.Config.BASE_TEMP_PATH, constants.Config.TEAM_FOLDER) 
                                    ,constants.Config.TEAM_ARGENTINA_FILE, argentina_teams_df)

def transform_venues() -> None:
    """
    Transforma los datos de estadios eliminando duplicados.
    
    """
    venues_argentina_df = parquet_operations.read_parquet_file(os.path.join(constants.Config.BASE_TEMP_PATH,constants.Config.VENUES_FOLDER,constants.Config.VENUES_ARGENTINA_FILE))
    

    venues_argentina_df = venues_argentina_df.drop_duplicates(subset=['id'])

    
    parquet_operations.save_parquet(os.path.join(constants.Config.BASE_TEMP_PATH,constants.Config.VENUES_FOLDER),
                                    constants.Config.VENUES_ARGENTINA_FILE, venues_argentina_df)

def load_to_redshift(table_name : str, folder : str, file : str) -> None:
    """
    Carga los datos de equipos y estadios a Redshift desde archivos Parquet.
    
    Args:
        table_name (str): Nombre de la tabla en Redshift.
    """
    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()

    database_operations.delete_table_from_redshift(conn, table_name, schema)

    df = parquet_operations.read_parquet_file(os.path.join(constants.Config.BASE_TEMP_PATH, folder, file))

    
    wr.redshift.to_sql(
        df=df,
        con=conn,
        table=table_name,
        schema=schema,
        mode='append', 
        use_column_names=True,
        lock=True,
        index=False
    )
    print(f"Datos cargados en la tabla {schema}.{table_name}")


def etl_teams_and_venues():
    """
    Proceso ETL para extraer, transformar y cargar datos de equipos y estadios en Redshift.
    """
    extract_teams_venues()
    
    transform_teams()
    transform_venues()

    print("Datos transformados guardados en Parquet.")

    load_to_redshift(constants.Config.TABLE_NAME_TEAM, constants.Config.TEAM_FOLDER, constants.Config.TEAM_ARGENTINA_FILE)
    load_to_redshift(constants.Config.TABLE_NAME_VENUE, constants.Config.VENUES_FOLDER,constants.Config.VENUES_ARGENTINA_FILE)


if __name__ == '__main__':
    etl_teams_and_venues()
