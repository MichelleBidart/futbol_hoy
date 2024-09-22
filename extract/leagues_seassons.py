import configparser
from datetime import datetime
import requests
import csv


def return_api_url():
    config = configparser.ConfigParser()
    config.read('config/config.properties')

    api_base_url = config.get('API_FOOTBALL', 'url')
    api_url = api_base_url + '/leagues'
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


print(f'Start league API', datetime.now())
response = requests.get(return_api_url(), headers=return_headers())
leagues = response.json()

leagues_csv_file = './temp/extract/leagues/'  + datetime.now().strftime('%Y%m%d_%H%M%S') + '_leagues_temp.csv'
seasons_csv_file = './temp/extract/seasons/' + datetime.now().strftime('%Y%m%d_%H%M%S') + '_seasons_temp.csv'

with open(leagues_csv_file, mode='w', newline='', encoding='utf-8') as file_leagues:
    writer = csv.writer(file_leagues)
    

    writer.writerow([
        'league_id', 'league_name', 'league_type', 'league_logo',
        'country_name', 'country_code', 'country_flag'
    ])
    

    for item in leagues['response']:
        league_info = item['league']
        country_info = item['country']
        
        writer.writerow([
            league_info['id'],
            league_info['name'],
            country_info['code'], 
            league_info['type'],
            league_info['logo'],
        ])


with open(seasons_csv_file, mode='w', newline='', encoding='utf-8') as file_season:
    writer = csv.writer(file_season)
    

    writer.writerow([
        'league_id', 'season_year', 'season_start', 'season_end', 'season_current',
        'coverage_fixtures_events', 'coverage_fixtures_lineups', 'coverage_fixtures_statistics_fixtures',
        'coverage_fixtures_statistics_players', 'coverage_standings', 'coverage_players',
        'coverage_top_scorers', 'coverage_top_assists', 'coverage_top_cards',
        'coverage_injuries', 'coverage_predictions', 'coverage_odds'
    ])

    for item in leagues['response']:
        league_id = item['league']['id']
        
        for season in item['seasons']:
            writer.writerow([
                league_id,
                season['year'],
                season['start'],
                season['end'],
                season['current'],
                season['coverage']['fixtures']['events'],
                season['coverage']['fixtures']['lineups'],
                season['coverage']['fixtures']['statistics_fixtures'],
                season['coverage']['fixtures']['statistics_players'],
                season['coverage']['standings'],
                season['coverage']['players'],
                season['coverage']['top_scorers'],
                season['coverage']['top_assists'],
                season['coverage']['top_cards'],
                season['coverage']['injuries'],
                season['coverage']['predictions'],
                season['coverage']['odds']
            ])

print(f"Datos guardados en season {seasons_csv_file} y leagues {leagues_csv_file} a las  ", seasons_csv_file, leagues_csv_file, datetime.now())
