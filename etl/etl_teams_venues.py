import requests
import csv
import pandas as pd
import os
from dotenv import load_dotenv
import api_url_configurations
import redshift_utils
import awswrangler as wr

load_dotenv('/opt/airflow/.env')


def extract_teams_venues():

    params = {
        'country': 'Argentina'
    }
    url, headers = api_url_configurations.get_api_url_headers()
    response = requests.get(f'{url}/teams', headers=headers, params=params)
    teams = response.json()['response']

    os.makedirs('./temp/extract/teams', exist_ok=True)
    os.makedirs('./temp/extract/venues', exist_ok=True)
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
    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()


    with conn.cursor() as cursor:
        cursor.execute(f'DELETE FROM "{schema}"."{table_name}"')
        conn.commit()
        print(f"Datos eliminados de la tabla {schema}.{table_name}.")
    
    df = pd.read_csv(csv_path)
    
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