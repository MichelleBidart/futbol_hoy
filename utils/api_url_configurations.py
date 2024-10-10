import os

def get_api_url_headers():
    """
    Obtiene la URL base de la API y las cabeceras necesarias para hacer solicitudes.

    La URL base de la API y la clave de la API se obtienen desde variables de entorno.
    La clave de la API es luego utilizada en las cabeceras para autenticar la solicitud.

    Returns:
        tuple: La URL base de la API y un diccionario con las cabeceras.
    """
    
    api_base_url = os.getenv('API_FOOTBALL_URL')
    
    api_key = os.getenv('API_FOOTBALL_KEY')

    print(f"La URL de la API es: {api_base_url}")

    headers = {
        'x-apisports-key': api_key
    }

    return api_base_url, headers
