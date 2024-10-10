import requests
import pandas as pd
from airflow.models import Variable
import utils.redshift_utils as redshift_utils
import utils.api_url_configurations as api_url_configurations
import utils.database_operations as database_operations
import awswrangler as wr

def extract_leagues_etl():
    url, headers = api_url_configurations.get_api_url_headers()
    params = {
        'country': 'Argentina'
    }
    response = requests.get(url + "leagues", headers=headers, params=params)
    response.raise_for_status()
    
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
    table_name = "league"
    database_operations.delete_table_from_redshift(conn, table_name, schema )

    wr.redshift.to_sql(
        df=df_leagues,
        con=conn,
        table=table_name,
        schema=schema,
        mode=table_name, 
        use_column_names=True,
        lock=True,
        index=False
    )

    print("Leagues data saved to Redshift")