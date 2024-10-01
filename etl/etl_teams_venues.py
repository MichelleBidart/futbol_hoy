from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd

# Función para extraer las ligas de Argentina
def extract_leagues():
    url = "https://v3.football.api-sports.io/leagues?country=Argentina"
    headers = {
        'x-apisports-key': 'YOUR_API_KEY'
    }
    response = requests.get(url, headers=headers)
    data = response.json()

    # Aquí devuelves todas las ligas, no solo las activas
    leagues = data['response']
    
    return leagues

# Función para procesar la data y transformarla en DataFrame usando XCom
def transform_leagues(ti):
    leagues = ti.xcom_pull(task_ids='extract_leagues')
    if not leagues:
        raise ValueError('No leagues data was pulled.')

    # Convertir a DataFrame
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

    df_leagues = pd.DataFrame(leagues_data)
    
    # Almacenar el DataFrame en XCom
    ti.xcom_push(key='leagues_data', value=df_leagues.to_dict())
    print("Leagues transformed and pushed to XCom")

# Función opcional para guardar el DataFrame en un CSV
def save_leagues_csv(ti):
    leagues_data = ti.xcom_pull(task_ids='transform_leagues', key='leagues_data')
    if not leagues_data:
        raise ValueError('No leagues data found in XCom.')

    df_leagues = pd.DataFrame.from_dict(leagues_data)
    
    # Guardar como CSV
    df_leagues.to_csv('/path/to/save/leagues.csv', index=False)
    print("Leagues saved to CSV")

# Definir el DAG
with DAG(
    dag_id='all_leagues_argentina_dag',
    schedule_interval='@monthly',  # Frecuencia mensual
    start_date=days_ago(1),         # Definir la fecha de inicio como un día antes
    catchup=False,                  # No ejecutar los DAGs previos
    default_args={
        'depends_on_past': False,   # Este valor depende de si quieres que dependa de ejecuciones pasadas
        'retries': 1
    }
) as dag:

    # Task para extraer ligas de Argentina
    extract_task = PythonOperator(
        task_id='extract_leagues',
        python_callable=extract_leagues
    )

    # Task para transformar y almacenar los datos en XCom
    transform_task = PythonOperator(
        task_id='transform_leagues',
        python_callable=transform_leagues
    )

    # Task opcional para guardar los datos en un CSV
    save_task = PythonOperator(
        task_id='save_leagues_csv',
        python_callable=save_leagues_csv
    )

    # Definir la secuencia de tareas
    extract_task >> transform_task >> save_task
