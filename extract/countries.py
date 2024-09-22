import configparser
import requests
import csv

def return_countries_api_url():
    config = configparser.ConfigParser()
    config.read('config/config.properties')

    api_base_url = config.get('API_FOOTBALL', 'url')
    api_url = api_base_url + '/countries'
    print(f"La URL de la API es: {api_url}")

    return api_url

def return_headers():
    config = configparser.ConfigParser()
    config.read('config/config.properties')

    api_key = config.get('API_FOOTBALL', 'api_key')

    headers = {
        'x-apisports-key': api_key
    }

    return headers


response = requests.get(return_countries_api_url(), headers=return_headers())
countries = response.json()['response']
url_csv = './temp/extract/countries/countries.csv'

with open(url_csv, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    

    writer.writerow(['country_name', 'country_code', 'country_flag'])
    

    for country in countries:
        writer.writerow([
            country['name'],
            country['code'],
            country['flag']
        ])

print("Datos de países guardados en {url_csv}.", url_csv)
