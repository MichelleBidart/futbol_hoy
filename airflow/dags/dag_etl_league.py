from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import etl.etl_leagues as etl_leagues

def extract_leagues(**kwargs):
   return etl_leagues.extract_leagues_etl()

def transform_leagues(**kwargs):
    leagues = kwargs['ti'].xcom_pull(task_ids='extract_leagues')

    leagues_data = etl_leagues.transform_leagues(leagues)

    kwargs['ti'].xcom_push(key='leagues_data', value=leagues_data)
    print("Leagues transformed and pushed to XCom") 

def save_leagues_redshift(**kwargs):
    leagues_data = kwargs['ti'].xcom_pull(task_ids='transform_leagues', key='leagues_data')
    
    etl_leagues.save_leagues_redshift(leagues_data)



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
