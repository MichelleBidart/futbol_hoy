from gold.gold_match import get_statistics


def run_gold(**kwargs):
    """
    Crea estadísticas y tablas con agregaciones a partir de los datos de partidos.
    """

    get_statistics()

    print("Finalización de la carga de datos en la capa Gold")