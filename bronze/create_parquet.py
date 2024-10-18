import os
import pandas as pd
from utils import parquet_operations, constants

def create_parquet(fixture_data: list, fixture_date: str):
    """
    Guarda los datos de los partidos (fixtures) en un archivo Parquet.

    Args:
        fixture_data (list): Lista de diccionarios con la respuesta de la API para los partidos.
        fixture_date (str): Fecha de los partidos en formato 'YYYY-MM-DD'.
    """
    print(f'data a guardar en parquet {list}')
    
    df_day_fixture = pd.DataFrame(fixture_data)

    parquet_filename = f"match_{fixture_date}.parquet"

    parquet_operations.save_parquet(
        os.path.join(constants.Config.BASE_TEMP_PATH, constants.Config.MATCH_FOLDER), 
        parquet_filename, 
        df_day_fixture
    )

