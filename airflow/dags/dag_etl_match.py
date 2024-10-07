from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import etl.etl_match as etl_match

def extract_fixtures(execution_date, **kwargs):

    fixture_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    return etl_match.extract_fixtures(fixture_date)

def transform_fixtures(**kwargs):
    fixtures = kwargs['ti'].xcom_pull(task_ids='extract_fixtures')
    match_data, status_data = etl_match.transform_fixtures(fixtures)

    kwargs['ti'].xcom_push(key='match_data', value=match_data)
    kwargs['ti'].xcom_push(key='status_data', value=status_data)

    print("Fixtures for Argentina transformed and pushed to XCom")


def load_fixtures_to_redshift(**kwargs):

    match_data = kwargs['ti'].xcom_pull(task_ids='transform_fixtures', key='match_data')
    status_data = kwargs['ti'].xcom_pull(task_ids='transform_fixtures', key='status_data')

    etl_match.load_fixtures_to_redshift(match_data, status_data)

    print("end of loading datas")


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
