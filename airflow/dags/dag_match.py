from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from bronze import bronze_fixture
from silver import silver_match
from airflow.utils.dates import days_ago
from gold import gold_match

def run_bronze(execution_date, **kwargs):
    """
    Se hace la ingesta de los fixtures del día anterior
    """
    fixture_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    return bronze_fixture.ingest_data_fixture(fixture_date)

def run_silver(**kwargs):
    """
    filtra los fixture y los guarda en la bse de datos
    """

    fixtures = kwargs['ti'].xcom_pull(task_ids='run_bronze')
    match_data = silver_match.clean_fixture(fixtures)

    print("Fixtures for Argentina transformed and pushed to XCom")

    return match_data

def run_gold(**kwargs):
    
    """
    se crean diferentes tablas con estadisticas
    """
    match_data = kwargs['ti'].xcom_pull(task_ids='run_silver')
    print(f'los resultados de silver son {match_data}')
    gold_match.get_statistics()
    print("End of loading data")

with DAG(
    dag_id='daily_fixtures_etl_dag_argentina',
    schedule_interval='@daily',
    start_date=days_ago(1), 
    catchup=False,
    max_active_runs=1, 
    default_args={
        'depends_on_past': False,
        'retries': 1
    }
) as dag:
    
    bronze_task = PythonOperator(
        task_id='run_bronze',  
        python_callable=run_bronze
    )

    silver_task = PythonOperator(
        task_id='run_silver',
        python_callable=run_silver
    )

    gold_task = PythonOperator(
        task_id='run_gold',
        python_callable=run_gold
    )

    bronze_task >> silver_task >> gold_task  
