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


def extract_teams_venues():

    params = {
        'country': 'Argentina'
    }
    response = requests.get(return_api_url('teams'), headers=return_headers(), params=params)
    teams = response.json()['response']

   
    teams_csv_path = './temp/extract/teams/teams_argentina.csv'
    venues_csv_path = './temp/extract/venues/venues_argentina.csv'

    with open(teams_csv_path, mode='w', newline='', encoding='utf-8') as teams_file:
        teams_writer = csv.writer(teams_file)
        teams_writer.writerow(['id', 'name', 'country', 'logo', 'stadium_id'])
        for team in teams:
            teams_writer.writerow([team['team']['id'], team['team']['name'], team['team']['country'], team['team']['logo'], team['venue']['id']])
    
    print(f"Datos de equipos de Argentina guardados en {teams_csv_path}")


    with open(venues_csv_path, mode='w', newline='', encoding='utf-8') as venues_file:
        venues_writer = csv.writer(venues_file)
        venues_writer.writerow(['id', 'name', 'city', 'capacity', 'address'])
        for team in teams:
            venue = team['venue']
            if venue['id'] is not None:  
                venues_writer.writerow([venue['id'], venue['name'], venue['city'], venue['capacity'], venue['address']])
    
    print(f"Datos de estadios de Argentina guardados en {venues_csv_path}")

    return teams_csv_path, venues_csv_path


def transform_teams(csv_path):
    teams_df = pd.read_csv(csv_path)
    argentina_teams_df = teams_df[teams_df['country'] == 'Argentina']
    return argentina_teams_df


def transform_venues(csv_path):
    venues_df = pd.read_csv(csv_path)
    venues_df = venues_df.drop_duplicates(subset=['id'])
    return venues_df


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


def etl_teams_and_venues():

    teams_csv_path, venues_csv_path = extract_teams_venues()
    print("teams_csv_path retornado: ", teams_csv_path)
    print("venues_csv_path retornado: ", venues_csv_path)


    argentina_teams_df = transform_teams(teams_csv_path)
    venues_df = transform_venues(venues_csv_path)

    argentina_teams_csv_path = './temp/extract/teams/transformed_teams_arg.csv'
    venues_csv_transformed_path = './temp/extract/venues/transformed_venues_arg.csv'

    argentina_teams_df.to_csv(argentina_teams_csv_path, index=False)
    venues_df.to_csv(venues_csv_transformed_path, index=False)

    print("Datos transformados guardados.")


    load_to_redshift(argentina_teams_csv_path, 'team')
    

    load_to_redshift(venues_csv_transformed_path, 'venue')


if __name__ == '__main__':
    etl_teams_and_venues()