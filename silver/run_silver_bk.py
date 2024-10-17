from silver.transform_data import clean_fixture
from silver.save_to_redshift import save_fixture_to_database


def run_silver(**kwargs):
    """
    Limpia y transforma los datos de fixtures y los guarda en la base de datos.
    """
    fixtures = kwargs['ti'].xcom_pull(task_ids='bronze_run')
    match, status = clean_fixture(fixtures)
    
    #si se ejecuta dos veces en el mismo dia el script va a devoler NONE. Para validar que hay un error
    #  de cannot unpack non-iterable NoneType object hago la siguente validacion 

    if match is not None and status is not None:
        save_fixture_to_database(match, status)


    print("Fixtures para Argentina transformados y cargados en Redshift.")
