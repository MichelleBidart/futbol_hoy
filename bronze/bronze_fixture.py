import os
import pandas as pd
import requests
from airflow.models import Variable
from utils import api_url_configurations, parquet_operations, constants


def ingest_data_fixture(fixture_date: str) -> list:
    """
    Obtiene los datos de los partidos (fixtures) de una fecha específica desde la API de fútbol, 
    los guarda en un archivo Parquet y retorna la respuesta en formato JSON.

    Args:
        fixture_date (str): La fecha de los partidos en formato 'YYYY-MM-DD' para los cuales se desean obtener datos.

    Returns:
        list: Lista de diccionarios con la respuesta de la API de fútbol para los partidos en la fecha indicada.
    """
    url, headers = api_url_configurations.get_api_url_headers()

    # Parámetros de la solicitud a la API (fecha del fixture)
    params = {
        'date': fixture_date
    }

    response = requests.get(url + "fixtures", headers=headers, params=params)

    response.raise_for_status()

    data = response.json()
    df_day_fixture = pd.DataFrame(data['response'])

    parquet_filename = f"match_{fixture_date}.parquet"
    parquet_operations.save_parquet(os.path.join(constants.Config.BASE_TEMP_PATH,constants.Config.MATCH_FOLDER), parquet_filename, df_day_fixture)


    return data['response']
