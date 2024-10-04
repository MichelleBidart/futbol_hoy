from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from airflow.models import Variable
from sqlalchemy import create_engine
import psycopg2

def extract_leagues(**kwargs):
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

def transform_leagues(**kwargs):
    leagues = kwargs['ti'].xcom_pull(task_ids='extract_leagues')
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

    kwargs['ti'].xcom_push(key='leagues_data', value=leagues_data)
    print("Leagues transformed and pushed to XCom") 

def save_leagues_redshift(**kwargs):
    leagues_data = kwargs['ti'].xcom_pull(task_ids='transform_leagues', key='leagues_data')
    
    if not leagues_data:
        raise ValueError('No leagues data found in XCom.')

    df_leagues = pd.DataFrame(leagues_data).drop_duplicates()


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
    print("Conexión exitosa con psycopg2")

    engine = create_engine('postgresql+psycopg2://', creator=lambda: connection)

    df_leagues.to_sql('league', engine, schema=redshift_schema, if_exists='append', index=False)
    print("Leagues data saved to Redshift")

with DAG(
    dag_id='all_leagues_argentina_dag',
    schedule_interval='@monthly',
    start_date=days_ago(1),
    catchup=False,
    default_args={
        'depends_on_past': False,
        'retries': 1
    }
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_leagues',
        python_callable=extract_leagues,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_leagues',
        python_callable=transform_leagues,
        provide_context=True
    )

    save_task = PythonOperator(
        task_id='save_leagues_redshift',
        python_callable=save_leagues_redshift,
        provide_context=True
    )

    extract_task >> transform_task >> save_task
