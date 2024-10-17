from gold.gold_match import get_statistics


def run_gold(**kwargs):
    """
    Crea estadísticas y tablas con agregaciones a partir de los datos de partidos.
    """
    match_data = kwargs['ti'].xcom_pull(task_ids='silver_run')

    print(f'Resultados de la capa Silver: {match_data}')

    get_statistics()

    print("Finalización de la carga de datos en la capa Gold")