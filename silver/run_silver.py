from silver.transform_data import clean_fixture
from silver.save_to_redshift import save_fixture_to_database
from utils import redshift_utils


def run_silver(**kwargs):
    fixtures = kwargs['ti'].xcom_pull(task_ids='bronze_run')
    conn = redshift_utils.get_redshift_connection()  
    df_match, df_status = clean_fixture(fixtures, conn)
    if df_match is not None and df_status is not None:
        save_fixture_to_database(df_match, df_status, conn)


    print("Fixtures para Argentina transformados y cargados en Redshift.")
