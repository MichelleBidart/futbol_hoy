import requests
import pandas as pd
from datetime import timedelta
from airflow.models import Variable
import redshift_utils as redshift_utils
import awswrangler as wr

def extract_fixtures(fixture_date):
    
    url = "https://v3.football.api-sports.io/fixtures"
    params = {
        'date': fixture_date  
    }
    api_key = Variable.get("x-apisports-key")
    headers = {
        'x-apisports-key': api_key
    }
    
    response = requests.get(url, headers=headers, params=params)
    data = response.json()

    fixtures = [fixture for fixture in data['response'] if fixture['league']['country'] == 'Argentina']

    return fixtures


def transform_fixtures(fixtures):

    if not fixtures:
        print('No fixture data was pulled.')
        return None, None
    
    conn = redshift_utils.get_redshift_connection()
    schema = Variable.get("redshift_schema")

    match_data = []
    status_data = []

    for fixture_info in fixtures:

        fixture = fixture_info['fixture']
        score = fixture_info['score']
        league_info = fixture_info['league']


        home_score = (score['halftime']['home'] or 0) + (score['fulltime']['home'] or 0) + (score['extratime']['home'] or 0)
        away_score = (score['halftime']['away'] or 0) + (score['fulltime']['away'] or 0) + (score['extratime']['away'] or 0)

        match_id = fixture['id']

        existing_match = pd.read_sql(
            f'SELECT 1 FROM "{schema}".match WHERE id = {match_id}', con=conn
        )

        if not existing_match.empty:
            continue


        match_data.append({
            'id': fixture['id'],
            'date': fixture['date'],
            'timezone': fixture['timezone'],
            'referee': fixture.get('referee', None),
            'venue_id': fixture['venue']['id'],
            'team_home_id': fixture_info['teams']['home']['id'],
            'team_away_id': fixture_info['teams']['away']['id'],
            'home_score': home_score,
            'away_score': away_score,
            'penalty_home': score['penalty']['home'],
            'penalty_away': score['penalty']['away'],
            'league_id': league_info['id'],  
            'season_year': league_info['season'],  
            'period_first': fixture['periods']['first'],
            'period_second': fixture['periods']['second']
        })


        status = fixture['status']
        status_data.append({
            'id': fixture['id'],
            'description': status['long']
        })

    return match_data, status_data


def load_fixtures_to_redshift(match_data, status_data):

    if not match_data:
        print("no data")
        return

    conn = redshift_utils.get_redshift_connection()

    df_match = pd.DataFrame(match_data)
    df_status = pd.DataFrame(status_data)

    schema = Variable.get("redshift_schema")

    wr.redshift.to_sql(
        df=df_match,
        con=conn,
        table='match',
        schema=schema,
        mode='append', 
        use_column_names=True,
        lock=True,
        index=False
    )

    wr.redshift.to_sql(
        df=df_status,
        con=conn,
        table='status',
        schema=schema,
        mode='append', 
        use_column_names=True,
        lock=True,
        index=False
    )

    print("Data loaded to Redshift successfully")