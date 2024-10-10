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

def extract_countries() -> None:
    """
    Obtiene los datos de los países de la API de fútbol, los guarda en formato Parquet y retorna la lista de países.

    Returns:
        list: Lista de diccionarios con los datos de los países (formato: code, name, flag).
    """
    #llama al endpoint de countris
    url, headers = api_url_configurations.get_api_url_headers()
    response = requests.get(f'{url}/countries', headers=headers)
    response.raise_for_status()

    countries = response.json()['response']

    df_countries = pd.DataFrame(countries)

    parquet_operations.save_parquet(
    os.path.join(constants.Config.BASE_TEMP_PATH, constants.Config.COUNTRY_FOLDER), 
    constants.Config.CONTRIES_FILE, 
    df_countries
    )    

def transform_countries() -> None:
    """
    Valida que las columnas sean las esperadas, que 'code' no sea null, que sea varchar de hasta 10 caracteres,
    elimina duplicados en la columna 'code', transforma los datos en un DataFrame y crea un archivo Parquet.
    
    Args:
        countries (List[Dict[str, str]]): Lista de diccionarios con datos de países.
    """

    countries_df = parquet_operations.read_parquet_file(constants.Config.CONTRIES_ARGENINA_FILE_READ)

    if countries_df['name'].isnull().any():
        raise ValueError("La columna 'name' contiene valores nulos, lo cual no está permitido.")

    if not countries_df['code'].apply(lambda x: pd.isna(x) or (isinstance(x, str) and len(x) <= 10)).all():
        raise ValueError("La columna 'code' debe ser null o un string con un máximo de 10 caracteres.")
    
    countries_df.drop_duplicates(subset='code', inplace=True)

    parquet_operations.save_parquet(constants.Config.BASE_TEMP_PATH.join(constants.Config.COUNTRY_FOLDER),constants.Config.CONTRIES_FILE ,countries_df)


def load_to_redshift():
    """
    Carga los datos de países desde un archivo Parquet a Redshift.
    Elimina previamente los datos existentes en la tabla 'country'.
    """


    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()

    #Se borrar los datos de la tabla country. Se hace esto para que no haya duplicados
    database_operations.delete_table_from_redshift(conn, constants.Config.TABLE_NAME_COUNTRY, schema)

    df = parquet_operations.read_parquet_file(constants.Config.CONTRIES_ARGENINA_FILE_READ)

    wr.redshift.to_sql(
        df=df,
        con=conn,
        table=constants.Config.TABLE_NAME_COUNTRY,
        schema=schema,
        mode='append', 
        use_column_names=True,
        lock=True,
        index=False
    )

    print(f"Datos cargados en la tabla {schema}.{constants.Config.TABLE_NAME_COUNTRY} desde el archivo Parquet.")



def etl_countries():

    print("empieza a ejecutarse el archivo etl_contries, {time}", datetime.now())
 
    extract_countries()

    transform_countries()
    
    load_to_redshift()

    print("finaliza la ejecucion de etl_contries, {time}", datetime.now())


if __name__ == '__main__':
    etl_countries()
