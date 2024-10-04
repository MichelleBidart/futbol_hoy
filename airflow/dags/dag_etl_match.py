from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from airflow.models import Variable
from sqlalchemy import create_engine
import psycopg2
from datetime import timedelta

def extract_fixtures(execution_date, **kwargs):

    fixture_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    
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


def transform_fixtures(**kwargs):
    fixtures = kwargs['ti'].xcom_pull(task_ids='extract_fixtures')
    if not fixtures:
        raise ValueError('No fixture data was pulled.')

    match_data = []
    status_data = []

    for fixture_info in fixtures:

        fixture = fixture_info['fixture']
        score = fixture_info['score']
        league_info = fixture_info['league']


        home_score = (score['halftime']['home'] or 0) + (score['fulltime']['home'] or 0) + (score['extratime']['home'] or 0)
        away_score = (score['halftime']['away'] or 0) + (score['fulltime']['away'] or 0) + (score['extratime']['away'] or 0)

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

    kwargs['ti'].xcom_push(key='match_data', value=match_data)
    kwargs['ti'].xcom_push(key='status_data', value=status_data)

    print("Fixtures for Argentina transformed and pushed to XCom")


def load_fixtures_to_redshift(**kwargs):

    match_data = kwargs['ti'].xcom_pull(task_ids='transform_fixtures', key='match_data')
    status_data = kwargs['ti'].xcom_pull(task_ids='transform_fixtures', key='status_data')


    df_match = pd.DataFrame(match_data).drop_duplicates()
    df_status = pd.DataFrame(status_data).drop_duplicates()

    redshift_user = Variable.get("redshift_user")
    redshift_password = Variable.get("redshift_password")
    redshift_host = Variable.get("redshift_host")
    redshift_port = Variable.get("redshift_port")
    redshift_dbname = Variable.get("redshift_dbname")
    redshift_schema = Variable.get("redshift_schema")

    connection = psycopg2.connect(
        dbname=redshift_dbname,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port
    )
    print("Conexión exitosa con Redshift (psycopg2)")

    engine = create_engine('postgresql+psycopg2://', creator=lambda: connection)


    df_match.to_sql('match', engine, schema=redshift_schema, if_exists='append', index=False)
    df_status.to_sql('status', engine, schema=redshift_schema, if_exists='append', index=False)

    print("Data loaded to Redshift successfully")


with DAG(
    dag_id='daily_fixtures_etl_dag_argentina',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args={
        'depends_on_past': False,
        'retries': 1
    }
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_fixtures',
        python_callable=extract_fixtures,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_fixtures',
        python_callable=transform_fixtures,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_fixtures_to_redshift',
        python_callable=load_fixtures_to_redshift,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
