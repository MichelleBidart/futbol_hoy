import utils.api_url_configurations as api_url_configurations
import requests
import utils.redshift_utils as redshift_utils
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv
import os
from typing import List, Dict

load_dotenv('/opt/airflow/.env')

def extract_teams_venues() -> tuple:
    """
    Extrae datos de equipos y estadios desde la API y los guarda en archivos Parquet.
    
    Returns:
        tuple: Rutas de los archivos Parquet de equipos y estadios.
    """
    params = {
        'country': 'Argentina'
    }
    url, headers = api_url_configurations.get_api_url_headers()
    response = requests.get(f'{url}/teams', headers=headers, params=params)
    response.raise_for_status()
    teams = response.json()['response']
    
    teams_parquet_directory = './temp/extract/teams'
    venues_parquet_directory = './temp/extract/venues'

    teams_parquet_path = os.path.join(teams_parquet_directory, 'teams_argentina.parquet')
    venues_parquet_path = os.path.join(venues_parquet_directory, 'venues_argentina.parquet')

    os.makedirs(teams_parquet_directory, exist_ok=True)
    os.makedirs(venues_parquet_directory, exist_ok=True)

    teams_df = pd.DataFrame([{
        'id': team['team']['id'],
        'name': team['team']['name'],
        'country': team['team']['country'],
        'logo': team['team']['logo'],
        'stadium_id': team['venue']['id']
    } for team in teams])
    
    teams_df.to_parquet(teams_parquet_path, index=False)
    print(f"Datos de equipos de Argentina guardados en {teams_parquet_path}")


    venues_df = pd.DataFrame([{
        'id': team['venue']['id'],
        'name': team['venue']['name'],
        'city': team['venue']['city'],
        'capacity': team['venue']['capacity'],
        'address': team['venue']['address']
    } for team in teams if team['venue']['id'] is not None])
    
    venues_df.to_parquet(venues_parquet_path, index=False)
    print(f"Datos de estadios de Argentina guardados en {venues_parquet_path}")

    return teams_parquet_path, venues_parquet_path


def transform_teams(parquet_path: str) -> pd.DataFrame:
    """
    Transforma los datos de equipos para filtrar los equipos de Argentina.
    
    Args:
        parquet_path (str): Ruta del archivo Parquet con los datos de equipos.
    
    Returns:
        pd.DataFrame: DataFrame filtrado con los equipos de Argentina.
    """
    teams_df = pd.read_parquet(parquet_path)
    argentina_teams_df = teams_df[teams_df['country'] == 'Argentina']
    return argentina_teams_df


def transform_venues(parquet_path: str) -> pd.DataFrame:
    """
    Transforma los datos de estadios eliminando duplicados.
    
    Args:
        parquet_path (str): Ruta del archivo Parquet con los datos de estadios.
    
    Returns:
        pd.DataFrame: DataFrame de estadios sin duplicados.
    """
    venues_df = pd.read_parquet(parquet_path)
    venues_df = venues_df.drop_duplicates(subset=['id'])
    return venues_df


def load_to_redshift(parquet_path: str, table_name: str):
    """
    Carga los datos de equipos y estadios a Redshift desde archivos Parquet.
    
    Args:
        parquet_path (str): Ruta del archivo Parquet con los datos.
        table_name (str): Nombre de la tabla en Redshift.
    """
    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()

    with conn.cursor() as cursor:
        cursor.execute(f'DELETE FROM "{schema}"."{table_name}"')
        conn.commit()
        print(f"Datos eliminados de la tabla {schema}.{table_name}.")

    df = pd.read_parquet(parquet_path)
    
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
    teams_parquet_path, venues_parquet_path = extract_teams_venues()
    
    argentina_teams_df = transform_teams(teams_parquet_path)
    venues_df = transform_venues(venues_parquet_path)

    argentina_teams_parquet_path = './temp/extract/teams/transformed_teams_arg.parquet'
    venues_parquet_transformed_path = './temp/extract/venues/transformed_venues_arg.parquet'

    argentina_teams_df.to_parquet(argentina_teams_parquet_path, index=False)
    venues_df.to_parquet(venues_parquet_transformed_path, index=False)

    print("Datos transformados guardados en Parquet.")

    load_to_redshift(argentina_teams_parquet_path, 'team')
    load_to_redshift(venues_parquet_transformed_path, 'venue')


if __name__ == '__main__':
    etl_teams_and_venues()
