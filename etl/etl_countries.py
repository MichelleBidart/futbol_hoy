import configparser
import requests
import csv
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def return_api_url(endpoint):
    config = configparser.ConfigParser()
    config.read('config/config.properties')

    api_base_url = config.get('API_FOOTBALL', 'url')
    api_url = f"{api_base_url}/{endpoint}"
    print(f"La URL de la API es: {api_url}")

    return api_url

def return_headers():
    config = configparser.ConfigParser()
    config.read('config/config.properties')

    api_key = config.get('API_FOOTBALL', 'api_key')

    headers = {
        'x-apisports-key': api_key
    }
    return headers


def extract_countries():
    response = requests.get(return_api_url('countries'), headers=return_headers())
    countries = response.json()['response']
    

    csv_path = './temp/extract/countries/countries.csv'
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['name', 'code'])
        for country in countries:
            writer.writerow([country['name'], country['code']])
    
    print(f"Datos de países guardados en {csv_path}")
    return csv_path


def transform_countries(csv_path):

    csv_path_argentina = './temp/extract/countries/countries_arg.csv'

    countries_df = pd.read_csv(csv_path)

    argentina_df = countries_df[countries_df['code'] == 'AR']

    print(argentina_df)

    csv_path = argentina_df.to_csv('./temp/extract/countries/countries_arg.csv', index=False)

    return csv_path_argentina


def load_to_redshift(csv_path, table_name):
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

    engine = create_engine('postgresql+psycopg2://', creator=lambda: connection)
    
    df = pd.read_csv(csv_path)
    
    df.to_sql(table_name, engine, index=False, if_exists='append', schema=redshift_schema)
    print(f"Datos cargados en la tabla {redshift_schema}.{table_name}")



def etl_countries():
 
    csv_path = extract_countries()

    print("csv_path retornado: ", csv_path)

    argentina_csv_path = transform_countries(csv_path)

    print("argentina_csv_path retornado: ", argentina_csv_path)
    
    load_to_redshift(argentina_csv_path, 'country')


if __name__ == '__main__':
    etl_countries()
