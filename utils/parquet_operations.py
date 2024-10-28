import pandas as pd
import os

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

    df.to_parquet(file_path, index=False)

    print(f'Archivo Parquet guardado en: {file_path}.')

    return file_path



def read_parquet(file_path: str) -> list:
    """
    Lee un archivo Parquet y devuelve su contenido como una lista de listas.
    
    Parámetros:
    - file_path (str): Ruta del archivo Parquet a leer.

    Retorna:
    - list: Lista de listas donde cada sublista representa una fila en el DataFrame.
    """
    
    df = pd.read_parquet(file_path)
    print(df.to_dict(orient='records'))    
    return df.to_dict(orient='records')
