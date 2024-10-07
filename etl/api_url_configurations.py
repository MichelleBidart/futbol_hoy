import os

def get_api_url_headers():

    api_base_url = os.getenv('API_FOOTBALL_URL')
    api_key = os.getenv('API_FOOTBALL_KEY')

    print(f"La URL de la API es: {api_base_url}")

    headers = {
        'x-apisports-key': api_key
    }
    return api_base_url, headers
