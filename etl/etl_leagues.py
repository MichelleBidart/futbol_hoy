import requests
import pandas as pd
import awswrangler as wr
from datetime import datetime
from typing import List, Dict
import utils.redshift_utils as redshift_utils
import utils.api_url_configurations as api_url_configurations
import utils.database_operations as database_operations
from utils import constants
from utils import parquet_operations
import os
from utils import constants

def extract_leagues() -> List[Dict[str, any]]:
    """
    Extrae los datos de ligas desde la API y los retorna como una lista de diccionarios.

    Returns:
        List[Dict[str, any]]: Lista de diccionarios con los datos de las ligas.
    """
    url, headers = api_url_configurations.get_api_url_headers()
    params = {'country': 'Argentina'}
    response = requests.get(f'{url}/leagues', headers=headers, params=params)
    response.raise_for_status()

    leagues = response.json().get('response', [])

    if not leagues:
        raise Exception('No se encontraron datos en la respuesta de ligas.')

    print(f'Datos de ligas extraídos: {len(leagues)} ligas')
    return leagues

def transform_leagues(leagues: List[Dict[str, any]]) -> pd.DataFrame:
    """
    Transforma los datos de ligas y devuelve un DataFrame.

    Args:
        leagues (List[Dict[str, any]]): Lista de diccionarios con los datos de las ligas.

    Returns:
        pd.DataFrame: DataFrame con los datos transformados de las ligas.
    """
    if not leagues:
        raise ValueError('No se han proporcionado datos de ligas para la transformación.')

    print('Transformando datos de ligas...')
    leagues_data = []
    for league_info in leagues:
        league = league_info['league']
        country = league_info['country']
        for season in league_info['seasons']:
            leagues_data.append({
                'league_id': league['id'],
                'league_name': league['name'],
                'country': country['name'],
                'season_year': season['year'],
                'start_date': season['start'],
                'end_date': season['end'],
                'current': season['current']
            })

    df_leagues = pd.DataFrame(leagues_data).drop_duplicates()
    return df_leagues

def save_to_parquet(df: pd.DataFrame, folder: str, file_name: str):
    """
    Guarda un DataFrame en un archivo Parquet.

    Args:
        df (pd.DataFrame): DataFrame que se va a guardar.
        folder (str): Carpeta donde se guardará el archivo.
        file_name (str): Nombre del archivo Parquet.
    """
    parquet_path = os.path.join(constants.Config.BASE_TEMP_PATH, folder)
    parquet_operations.save_parquet(parquet_path, file_name, df)
    print(f'DataFrame guardado en {parquet_path}/{file_name}')

def load_leagues_to_redshift(df_leagues: pd.DataFrame):
    """
    Carga un DataFrame de ligas en Redshift.

    Args:
        df_leagues (pd.DataFrame): DataFrame con los datos transformados de las ligas.
    """
    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()
    table_name = constants.Config.TABLE_LEAGUE


    database_operations.delete_table_from_redshift(conn, table_name, schema)

    wr.redshift.to_sql(
        df=df_leagues,
        con=conn,
        table=table_name,
        schema=schema,
        mode='append',
        use_column_names=True,
        lock=True,
        index=False
    )

    print(f"Datos de ligas cargados en la tabla {schema}.{table_name}")

def etl_leagues():
    """
    Ejecuta el proceso ETL para los datos de ligas.
    """
    start_time = datetime.now()
    print(f"Iniciando el proceso ETL para las ligas: {start_time}")

    leagues = extract_leagues()

    df_leagues = transform_leagues(leagues)

    save_to_parquet(df_leagues, constants.Config.LEAGUE_FOLDER, constants.Config.LEAGUE_FILE)

    load_leagues_to_redshift(df_leagues)

    print("Proceso ETL para las ligas finalizado correctamente.")

if __name__ == '__main__':
    etl_leagues()
