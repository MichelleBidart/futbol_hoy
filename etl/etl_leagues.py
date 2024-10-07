import requests
import pandas as pd
from airflow.models import Variable
import redshift_utils as redshift_utils
import awswrangler as wr

def extract_leagues_etl():
    url = "https://v3.football.api-sports.io/leagues"
    params = {
        'country': 'Argentina'
    }
    api_key = Variable.get("x-apisports-key")
    headers = {
        'x-apisports-key': api_key
    }
    
    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    leagues = data['response']

    return leagues

def transform_leagues(leagues):
    if not leagues:
        raise ValueError('No leagues data was pulled.')

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

    print("Leagues transformed and pushed to XCom") 
    return leagues_data

def save_leagues_redshift(leagues_data):
    if not leagues_data:
        raise ValueError('No leagues data found in XCom.')

    df_leagues = pd.DataFrame(leagues_data).drop_duplicates()

    conn = redshift_utils.get_redshift_connection()
    schema = Variable.get("redshift_schema")

    wr.redshift.to_sql(
        df=df_leagues,
        con=conn,
        table='leagues',
        schema=schema,
        mode='append', 
        use_column_names=True,
        lock=True,
        index=False
    )

    print("Leagues data saved to Redshift")