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
    absolute_directory = os.path.abspath(directory)
    print(f"Directorio completo: {absolute_directory}")
    print(f'el directorio es {directory}')

    file_path = os.path.join(directory, file_name)
    
    print(f'se va a guardar el archivo parquet enla siguiente ruta, {file_path}')
    
    os.makedirs(directory, exist_ok=True)

    df.to_parquet(file_path, index=False)

    print(f'Archivo Parquet guardado en: {file_path}.')


