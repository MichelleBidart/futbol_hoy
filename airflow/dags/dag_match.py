from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


from bronze.run_bronze import run_bronze  
from silver.run_silver import run_silver  
from gold.run_gold import run_gold 


default_args = {
    "owner": "michelle_bidart",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "max_active_runs":1,
    "retry_delay": timedelta(minutes=5), 
}

with DAG(
    dag_id="daily_fixtures_etl_dag_argentina",
    default_args=default_args,
    description="DAG para procesar datos de fixtures y cargar en Redshift",
    schedule_interval="@daily",  
    start_date=days_ago(1),  
    catchup=False,  
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_run",
        python_callable=run_bronze,
        provide_context=True,
    )

    silver_task = PythonOperator(
        task_id="silver_run",
        python_callable=run_silver,
        provide_context=True,
    )

    gold_task = PythonOperator(
        task_id="gold_run",
        python_callable=run_gold,
        provide_context=True,
    )

    bronze_task >> silver_task >> gold_task
