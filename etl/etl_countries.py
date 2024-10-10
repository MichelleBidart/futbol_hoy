import utils.api_url_configurations as api_url_configurations 
import requests
import utils.redshift_utils as redshift_utils
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv
import os
from typing import List, Dict

load_dotenv('/opt/airflow/.env')

def extract_countries() -> list:
    """
    Obtiene los datos de los países de la API de fútbol, los guarda en formato Parquet y retorna la lista de países.

    Returns:
        list: Lista de diccionarios con los datos de los países (formato: code, name, flag).
    """
    url, headers = api_url_configurations.get_api_url_headers()

    response = requests.get(f'{url}/countries', headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Error al obtener los datos de {url}countries")

    countries = response.json()['response']
    df_countries = pd.DataFrame(countries)

  
    parquet_directory = './temp/extract/countries'
    parquet_path = os.path.join(parquet_directory, 'countries.parquet')


    os.makedirs(parquet_directory, exist_ok=True)


    df_countries.to_parquet(parquet_path, index=False)

    print(f"Datos de países guardados en formato Parquet en: {parquet_path}")

    return countries


def transform_countries(countries: List[Dict[str, str]]) -> None:
    """
    Valida que las columnas sean las esperadas, que 'code' no sea null, que sea varchar de hasta 10 caracteres,
    elimina duplicados en la columna 'code', transforma los datos en un DataFrame y crea un archivo Parquet.
    
    Args:
        countries (List[Dict[str, str]]): Lista de diccionarios con datos de países.
    """

    expected_columns = ['code', 'name', 'flag']

    countries_df = pd.DataFrame(countries)

    if countries_df['name'].isnull().any():
        raise ValueError("La columna 'name' contiene valores nulos, lo cual no está permitido.")

    if not countries_df['code'].apply(lambda x: pd.isna(x) or (isinstance(x, str) and len(x) <= 10)).all():
        raise ValueError("La columna 'code' debe ser null o un string con un máximo de 10 caracteres.")
    
    countries_df.drop_duplicates(subset='code', inplace=True)

    parquet_directory = './temp/extract/countries'
    parquet_path = os.path.join(parquet_directory, 'countries.parquet')

    os.makedirs(parquet_directory, exist_ok=True)

    countries_df.to_parquet(parquet_path, index=False)

    print(f"Datos guardados en formato Parquet en: {parquet_path}")




def load_to_redshift():
    """
    Carga los datos de países desde un archivo Parquet a Redshift.
    Elimina previamente los datos existentes en la tabla 'country'.
    """

    table_name = 'country'
    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()

    with conn.cursor() as cursor:
        cursor.execute(f'DELETE FROM "{schema}"."{table_name}"')
        conn.commit()
        print(f"Datos eliminados de la tabla {schema}.{table_name}.")

    parquet_path = './temp/extract/countries/countries.parquet'
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

    print(f"Datos cargados en la tabla {schema}.{table_name} desde el archivo Parquet.")



def etl_countries():
 
    countries = extract_countries()

    transform_countries(countries)
    
    load_to_redshift()


if __name__ == '__main__':
    etl_countries()
