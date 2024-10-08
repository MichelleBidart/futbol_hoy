import etl.api_url_configurations as api_url_configurations 
import requests
import etl.redshift_utils as redshift_utils
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv
import os

load_dotenv('/opt/airflow/.env')
#load_dotenv('.env')

def extract_countries():
    """
    Extrae datos de países

    Returns:
        list: Una lista de diccionarios con los datos de los países extraídos de la API.
    """
    url, headers = api_url_configurations.get_api_url_headers()
    response = requests.get(f'{url}/countries', headers=headers) 
    if response.status_code !=200:
        raise Exception(f"response error while traying to bring back {url}/countries ")
    return response.json()['response']


def transform_countries(countries):

    countries_df = pd.DataFrame(countries)
    
    os.makedirs('./temp/extract/countries', exist_ok=True)

    countries_df.to_csv(os.getenv('TEMP_CSV_COUNTRIES'), index=False)



def load_to_redshift():
    table_name = 'country'
    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()

    with conn.cursor() as cursor:
        cursor.execute(f'DELETE FROM "{schema}"."{table_name}"')
        conn.commit()
        print(f"Datos eliminados de la tabla {schema}.{table_name}.")

    df = pd.read_csv(os.getenv('TEMP_CSV_COUNTRIES'))
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



def etl_countries():
 
    countries = extract_countries()

    transform_countries(countries)
    
    load_to_redshift()


if __name__ == '__main__':
    etl_countries()
