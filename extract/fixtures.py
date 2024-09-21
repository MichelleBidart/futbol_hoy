import requests
import configparser
import csv
from datetime import datetime
import os

""" 
Este fichero se encarga de llamar a la api fixtures de api football y pasar los archivos  a un csv plano y los guarda en la carpeta temp
"""

config = configparser.ConfigParser()
config.read('config/config.properties')

api_base_url = config.get('API_FOOTBALL', 'url')
api_url = api_base_url + '/fixtures'
print(f"La url de la api es: ", api_url)

api_key = config.get('API_FOOTBALL', 'api_key')

params = {'date': '2024-09-14'}
headers = {
    'x-apisports-key': api_key
}

try:
    print(f'Start fixture API', datetime.now())
    response = requests.get(api_url, headers=headers, params=params)
    if response.status_code != 200:
        raise ValueError(f"Error en la solicitud: {response.status_code}")
    print(f'End fixture API', datetime.now())
    data = response.json()
    fixtures = data.get('response')
    if not fixtures:
        raise ValueError("La respuesta no contiene datos en 'response' o está vacía.")
    
    print(f"se devolvieron los datos correctamente con los resultados: ", )
    
    today_in_seconds = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file_name = f'{today_in_seconds}_temp.csv'
    csv_file_path = os.path.join('temp', csv_file_name)


    encabezados = [
        'fixture_id', 'fixture_referee', 'fixture_timezone', 'fixture_date', 'fixture_timestamp',
        'period_first', 'period_second',
        'venue_id', 'venue_name', 'venue_city',
        'status_long', 'status_short', 'status_elapsed',
        'league_id', 'league_name', 'league_country', 'league_season', 'league_round',
        'home_team_id', 'home_team_name', 'home_team_logo',
        'away_team_id', 'away_team_name', 'away_team_logo',
        'home_goals', 'away_goals',
        'halftime_home', 'halftime_away',
        'fulltime_home', 'fulltime_away',
        'extratime_home', 'extratime_away',
        'penalty_home', 'penalty_away'
    ]

    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as file:
        print(f'Start writting csv files', datetime.now())
        writer = csv.writer(file)
        writer.writerow(encabezados)  


        for fixture in fixtures:
            fixture_id = fixture['fixture'].get('id')
            fixture_referee = fixture['fixture'].get('referee')
            fixture_timezone = fixture['fixture'].get('timezone')
            fixture_date = fixture['fixture'].get('date')
            fixture_timestamp = fixture['fixture'].get('timestamp')

    
            period_first = fixture['fixture']['periods'].get('first')
            period_second = fixture['fixture']['periods'].get('second')

    
            venue_id = fixture['fixture']['venue'].get('id')
            venue_name = fixture['fixture']['venue'].get('name')
            venue_city = fixture['fixture']['venue'].get('city')

            status_long = fixture['fixture']['status'].get('long')
            status_short = fixture['fixture']['status'].get('short')
            status_elapsed = fixture['fixture']['status'].get('elapsed')

            league_id = fixture['league'].get('id')
            league_name = fixture['league'].get('name')
            league_country = fixture['league'].get('country')
            league_season = fixture['league'].get('season')
            league_round = fixture['league'].get('round')

            home_team_id = fixture['teams']['home'].get('id')
            home_team_name = fixture['teams']['home'].get('name')
            home_team_logo = fixture['teams']['home'].get('logo')

            away_team_id = fixture['teams']['away'].get('id')
            away_team_name = fixture['teams']['away'].get('name')
            away_team_logo = fixture['teams']['away'].get('logo')

            home_goals = fixture['goals'].get('home')
            away_goals = fixture['goals'].get('away')

            halftime_home = fixture['score']['halftime'].get('home')
            halftime_away = fixture['score']['halftime'].get('away')
            fulltime_home = fixture['score']['fulltime'].get('home')
            fulltime_away = fixture['score']['fulltime'].get('away')
            extratime_home = fixture['score']['extratime'].get('home')
            extratime_away = fixture['score']['extratime'].get('away')
            penalty_home = fixture['score']['penalty'].get('home')
            penalty_away = fixture['score']['penalty'].get('away')

            writer.writerow([
                fixture_id, fixture_referee, fixture_timezone, fixture_date, fixture_timestamp,
                period_first, period_second,
                venue_id, venue_name, venue_city,
                status_long, status_short, status_elapsed,
                league_id, league_name, league_country, league_season, league_round,
                home_team_id, home_team_name, home_team_logo,
                away_team_id, away_team_name, away_team_logo,
                home_goals, away_goals,
                halftime_home, halftime_away,
                fulltime_home, fulltime_away,
                extratime_home, extratime_away,
                penalty_home, penalty_away
            ])
    
    print(f"Datos convertidos exitosamente a {csv_file_path} a las ", datetime.now())

except ValueError as ve:
    print(f"Error de valor: {ve}")
except Exception as ex:
    print(f"Ocurrió un error inesperado: {ex}")