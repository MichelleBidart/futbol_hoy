import utils.api_url_configurations as api_url_configurations 
import requests
import utils.redshift_utils as redshift_utils
import pandas as pd
import awswrangler as wr
from typing import List, Dict
from utils import parquet_operations
from utils import constants
from utils import database_operations
from datetime import datetime
import os


def extract_countries() -> List[Dict[str, str]]:
    """
    Obtiene los datos de los países de la API de fútbol, los guarda en formato Parquet y retorna la lista de países.

    Returns:
        list: Lista de diccionarios con los datos de los países (formato: code, name, flag).
    """
    #llama al endpoint de countries
    url, headers = api_url_configurations.get_api_url_headers()
    response = requests.get(f'{url}/countries', headers=headers)
    response.raise_for_status()

    countries = response.json()['response']

    if not countries:
        raise Exception(f'la respuesta de {url}/countries viene vacía')

    print(f'los countries son: {countries}')

    return countries   

def transform_countries(countries : List[Dict[str, str]]) -> pd.DataFrame:
    """
    Valida que las columnas sean las esperadas, que 'code' no sea null, que sea varchar de hasta 10 caracteres,
    elimina duplicados en la columna 'code', transforma los datos en un DataFrame y crea un archivo Parquet.
    
    Args:
        countries (List[Dict[str, str, str]]): Lista de diccionarios con datos de países.
    """

    print(f'transform countries {countries}')

    countries_df = pd.DataFrame(countries)

    if countries_df['name'].isnull().any():
        raise ValueError("La columna 'name' contiene valores nulos, lo cual no está permitido.")

    if not countries_df['code'].apply(lambda x: pd.isna(x) or (isinstance(x, str) and len(x) <= 10)).all():
        raise ValueError("La columna 'code' debe ser null o un string con un máximo de 10 caracteres.")
    
    countries_df.drop_duplicates(subset='code', inplace=True)

    return countries_df


def load_to_redshift(df_countries_transform : pd.DataFrame):
    """
    Carga los datos de países desde un archivo Parquet a Redshift.
    Elimina previamente los datos existentes en la tabla 'country'.
    """

    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()

    #Se borrar los datos de la tabla country. Se hace esto para que no haya duplicados
    database_operations.delete_table_from_redshift(conn, constants.Config.TABLE_NAME_COUNTRY, schema)

    wr.redshift.to_sql(
        df=df_countries_transform,
        con=conn,
        table=constants.Config.TABLE_NAME_COUNTRY,
        schema=schema,
        mode='append', 
        use_column_names=True,
        lock=True,
        index=False
    )

    print(f"Datos cargados en la tabla {schema}.{constants.Config.TABLE_NAME_COUNTRY}.")


def etl_countries():

    print("empieza a ejecutarse el archivo etl_contries, {time}", datetime.now())
 
    countries = extract_countries()

    df_countries_transform = transform_countries(countries)

    load_to_redshift(df_countries_transform)

    print("finaliza la ejecución de etl_contries, {time}", datetime.now())


if __name__ == '__main__':
    etl_countries()
