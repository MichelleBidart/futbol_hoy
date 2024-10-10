import pandas as pd
import os
from dotenv import load_dotenv

def save_parquet(directory: str, file_name: str, df: pd.DataFrame):
    """
    Guarda un DataFrame en un archivo Parquet en una ruta especificada. Si el directorio no existe, lo crea.
    
    :param directory: Directorio donde se guardará el archivo.
    :param file_name: Nombre del archivo parquet.
    :param df: DataFrame que se guardará en el archivo.
    """

    file_path = os.path.join(directory, file_name)
    
    print(f'se va a guardar el archivo parquet enla siguiente ruta, {file_path}')
    
    os.makedirs(directory, exist_ok=True)

    df.to_parquet(file_path)

    print(f'Archivo Parquet guardado en: {file_path}.')

def read_parquet_file(file_path: str) -> pd.DataFrame:
    """
    Lee un archivo Parquet desde la ruta especificada y devuelve un DataFrame.

    :param file_path: Ruta completa del archivo Parquet.
    :return: DataFrame con los datos del archivo Parquet.
    """
    absolute_path = os.path.abspath(file_path)
    
    print(f'El absolute path es, {absolute_path}')

    if os.path.exists(absolute_path):
        df = pd.read_parquet(absolute_path)
        print(f"Archivo Parquet cargado desde: {absolute_path}")
        return df
    else:
        raise FileNotFoundError(f"El archivo {absolute_path} no existe.")

