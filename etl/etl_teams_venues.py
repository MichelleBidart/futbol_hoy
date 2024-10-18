import utils.api_url_configurations as api_url_configurations
import requests
import utils.redshift_utils as redshift_utils
import pandas as pd
import awswrangler as wr
import os
from typing import List, Dict
from utils import constants
from utils import parquet_operations
from utils import database_operations
from datetime import datetime

def extract_teams_venues() -> List[Dict[str, any]]:
    """
    Extrae datos de equipos y estadios desde la API y los retorna como una lista de diccionarios.

    Returns:
        List[Dict[str, any]]: Lista de diccionarios con los datos de los equipos y estadios.
    """
    params = {'country': 'Argentina'}
    url, headers = api_url_configurations.get_api_url_headers()
    response = requests.get(f'{url}/teams', headers=headers, params=params)
    response.raise_for_status()

    teams_data = response.json().get('response', [])

    if not teams_data:
        raise Exception('La respuesta de la API de equipos está vacía.')

    print(f'Datos de equipos extraídos: {len(teams_data)} equipos')
    return teams_data

def transform_teams(teams_data: List[Dict[str, any]]) -> pd.DataFrame:
    """
    Transforma los datos de equipos y devuelve un DataFrame.

    Args:
        teams_data (List[Dict[str, any]]): Lista de diccionarios con los datos de los equipos.

    Returns:
        pd.DataFrame: DataFrame con los datos transformados de los equipos.
    """
    print('Transformando datos de equipos')
    teams_df = pd.DataFrame([{
        'id': team['team']['id'],
        'name': team['team']['name'],
        'country': team['team']['country'],
        'logo': team['team']['logo'],
        'stadium_id': team['venue']['id']
    } for team in teams_data])

    teams_df = teams_df.drop_duplicates(subset=['id'])
    return teams_df

def transform_venues(teams_data: List[Dict[str, any]]) -> pd.DataFrame:
    """
    Transforma los datos de estadios y devuelve un DataFrame.

    Args:
        teams_data (List[Dict[str, any]]): Lista de diccionarios con los datos de los equipos.

    Returns:
        pd.DataFrame: DataFrame con los datos transformados de los estadios.
    """
    print('Transformando datos de estadios...')
    venues_df = pd.DataFrame([{
        'id': team['venue']['id'],
        'name': team['venue']['name'],
        'city': team['venue']['city'],
        'capacity': team['venue']['capacity'],
        'address': team['venue']['address']
    } for team in teams_data if team['venue']['id'] is not None])

    venues_df = venues_df.drop_duplicates(subset=['id'])
    return venues_df

def load_to_redshift(table_name: str, df :pd.DataFrame):
    """
    Carga un archivo Parquet en Redshift.

    Args:
        table_name (str): Nombre de la tabla en Redshift.
        folder (str): Carpeta donde se encuentra el archivo Parquet.
        file_name (str): Nombre del archivo Parquet.
    """
    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()

    database_operations.delete_table_from_redshift(conn, table_name, schema)

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
    Ejecuta el proceso ETL para los datos de equipos y estadios.
    """
    start_time = datetime.now()
    print(f"Iniciando el proceso ETL para equipos y estadios: {start_time}")


    teams_data = extract_teams_venues()

    teams_df = transform_teams(teams_data)
    venues_df = transform_venues(teams_data)

    load_to_redshift(constants.Config.TABLE_NAME_TEAM, teams_df)
    load_to_redshift(constants.Config.TABLE_NAME_VENUE, venues_df)

    print("Proceso ETL finalizado correctamente.")


if __name__ == '__main__':
    etl_teams_and_venues()
